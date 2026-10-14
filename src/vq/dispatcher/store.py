"""Состояние dispatcher в Postgres (§2.4, §2.5): claim с фенсингом по epoch и чекпоинт одной инструкцией."""

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, fields
from pathlib import Path
from typing import Any

import psycopg
from psycopg.rows import class_row

from vq.core.model import SiteState, Station, Vehicle

MIGRATIONS_DIR = Path(__file__).resolve().parents[3] / "migrations"
PG_OPTIONS = "-c statement_timeout=5000 -c lock_timeout=2000 -c idle_in_transaction_session_timeout=10000"
MIGRATE_LOCK_KEY = 0x76710001  # advisory lock: два экземпляра не применяют схему одновременно

REFUSAL_HINT = "make reset"

# Колонки, с которыми работает код: stations и vehicles — поля модели один к одному (§2.5), processor — служебная.
SCHEMA: dict[str, frozenset[str]] = {
    "processor": frozenset(
        "site_id epoch next_offset now_ts logic_version config_hash msgs_accepted msgs_not_newer msgs_invalid "
        "updated_at".split()
    ),
    "stations": frozenset({*(f.name for f in fields(Station)), "updated_at"}),
    "vehicles": frozenset({*(f.name for f in fields(Vehicle)), "updated_at"}),
}


@dataclass
class Counters:
    """Точные счётчики входных сообщений; пишутся в чекпоинте вместе с offset."""

    accepted: int = 0
    not_newer: int = 0
    invalid: int = 0

    def plus(self, other: "Counters") -> "Counters":
        return Counters(self.accepted + other.accepted, self.not_newer + other.not_newer, self.invalid + other.invalid)


@dataclass
class Claim:
    epoch: int
    next_offset: int | None  # None — ещё ничего не обработано
    now_ts: int | None
    counters: Counters
    state: SiteState | None  # None, когда next_offset IS NULL


class StartRefused(Exception):
    """Отказ старта, лечится make reset.

    reason: logic_version_changed | config_changed | schema_changed | state_older_than_log | log_recreated |
    partitions.
    """

    hint = REFUSAL_HINT

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


_COLUMNS = """
SELECT table_name::text, column_name::text FROM information_schema.columns
 WHERE table_schema = current_schema() AND table_name = ANY(%s)
"""

_INSERT_PROCESSOR = """
INSERT INTO processor (site_id, epoch, logic_version, config_hash)
VALUES (%s, 0, %s, %s)
ON CONFLICT (site_id) DO NOTHING
"""

_BUMP_EPOCH = """
UPDATE processor SET epoch = epoch + 1, updated_at = now()
 WHERE site_id = %s
RETURNING epoch, next_offset, now_ts, logic_version, config_hash, msgs_accepted, msgs_not_newer, msgs_invalid
"""

_SET_VERSIONS = "UPDATE processor SET logic_version = %s, config_hash = %s WHERE site_id = %s"

# В stations и vehicles нет site_id (схема §2.5): одна база — одна площадка. Строка processor другой площадки
# или строки состояния при пустом next_offset — значит, в базе состояние другой конфигурации (смена [site] id).
_FOREIGN_STATE = """
SELECT (SELECT array_agg(site_id ORDER BY site_id) FROM processor WHERE site_id <> %s),
       EXISTS (SELECT FROM stations) OR EXISTS (SELECT FROM vehicles)
"""

_SELECT_STATIONS = """
SELECT station_uuid, occupant_uuid, occupied_at, last_release_at, last_published_queue
  FROM stations ORDER BY station_uuid
"""

_SELECT_VEHICLES = """
SELECT unit_uuid, home_station, last_ts, lat, lon, speed_kmh, phase, target_station,
       radius_station, radius_entered_at, arrived_at, dump_visited, decided, predicted_start
  FROM vehicles ORDER BY unit_uuid
"""

# Одна инструкция = одна неявная транзакция = один round-trip (в autocommit).
# Upsert-ы срабатывают, только если UPDATE нашёл строку с моим epoch: при фенсинге не пишется ничего.
_CHECKPOINT = """
WITH p AS (
    UPDATE processor
       SET next_offset = %(next_offset)s, now_ts = %(now_ts)s,
           msgs_accepted = %(accepted)s, msgs_not_newer = %(not_newer)s, msgs_invalid = %(invalid)s,
           updated_at = now()
     WHERE site_id = %(site_id)s AND epoch = %(epoch)s
    RETURNING 1
), s AS (
    INSERT INTO stations (station_uuid, occupant_uuid, occupied_at, last_release_at, last_published_queue, updated_at)
    SELECT u.*, now()
      FROM unnest(%(s_uuid)s::text[], %(s_occupant)s::text[], %(s_occupied_at)s::bigint[],
                  %(s_last_release_at)s::bigint[], %(s_last_published)s::text[])
           AS u(station_uuid, occupant_uuid, occupied_at, last_release_at, last_published_queue)
     WHERE EXISTS (SELECT FROM p)
    ON CONFLICT (station_uuid) DO UPDATE SET
        occupant_uuid = EXCLUDED.occupant_uuid,
        occupied_at = EXCLUDED.occupied_at,
        last_release_at = EXCLUDED.last_release_at,
        last_published_queue = EXCLUDED.last_published_queue,
        updated_at = EXCLUDED.updated_at
), v AS (
    INSERT INTO vehicles (unit_uuid, home_station, last_ts, lat, lon, speed_kmh, phase, target_station,
                          radius_station, radius_entered_at, arrived_at, dump_visited, decided, predicted_start,
                          updated_at)
    SELECT u.*, now()
      FROM unnest(%(v_uuid)s::text[], %(v_home)s::text[], %(v_last_ts)s::bigint[],
                  %(v_lat)s::float8[], %(v_lon)s::float8[], %(v_speed)s::float8[],
                  %(v_phase)s::text[], %(v_target)s::text[], %(v_radius_station)s::text[],
                  %(v_radius_entered_at)s::bigint[], %(v_arrived_at)s::bigint[], %(v_dump_visited)s::boolean[],
                  %(v_decided)s::boolean[], %(v_predicted_start)s::bigint[])
           AS u(unit_uuid, home_station, last_ts, lat, lon, speed_kmh, phase, target_station,
                radius_station, radius_entered_at, arrived_at, dump_visited, decided, predicted_start)
     WHERE EXISTS (SELECT FROM p)
    ON CONFLICT (unit_uuid) DO UPDATE SET
        home_station = EXCLUDED.home_station,
        last_ts = EXCLUDED.last_ts,
        lat = EXCLUDED.lat,
        lon = EXCLUDED.lon,
        speed_kmh = EXCLUDED.speed_kmh,
        phase = EXCLUDED.phase,
        target_station = EXCLUDED.target_station,
        radius_station = EXCLUDED.radius_station,
        radius_entered_at = EXCLUDED.radius_entered_at,
        arrived_at = EXCLUDED.arrived_at,
        dump_visited = EXCLUDED.dump_visited,
        decided = EXCLUDED.decided,
        predicted_start = EXCLUDED.predicted_start,
        updated_at = EXCLUDED.updated_at
)
SELECT count(*) FROM p
"""


def _opt_int(x: int | None) -> int | None:
    return None if x is None else int(x)


def schema_diff(columns: Mapping[str, Iterable[str]]) -> str:
    """Расхождение колонок таблиц в базе со схемой кода; пустая строка — совпадают."""
    parts = []
    for table in sorted(SCHEMA):
        have = set(columns.get(table, ()))
        missing, extra = sorted(SCHEMA[table] - have), sorted(have - SCHEMA[table])
        if missing:
            parts.append(f"{table}: нет колонок {', '.join(missing)}")
        if extra:
            parts.append(f"{table}: лишние колонки {', '.join(extra)}")
    return "; ".join(parts)


def checkpoint_params(
    site_id: str, epoch: int, next_offset: int, counters: Counters, state: SiteState
) -> dict[str, Any]:
    """Параметры чекпоинта: строки по ключу (стабильный порядок блокировок), типы приведены явно."""
    sts = [state.stations[k] for k in sorted(state.stations)]
    vs = [state.vehicles[k] for k in sorted(state.vehicles)]
    return {
        "site_id": site_id,
        "epoch": epoch,
        "next_offset": next_offset,
        "now_ts": _opt_int(state.now),
        "accepted": counters.accepted,
        "not_newer": counters.not_newer,
        "invalid": counters.invalid,
        "s_uuid": [s.station_uuid for s in sts],
        "s_occupant": [s.occupant_uuid for s in sts],
        "s_occupied_at": [_opt_int(s.occupied_at) for s in sts],
        "s_last_release_at": [_opt_int(s.last_release_at) for s in sts],
        "s_last_published": [s.last_published_queue for s in sts],
        "v_uuid": [v.unit_uuid for v in vs],
        "v_home": [v.home_station for v in vs],
        "v_last_ts": [int(v.last_ts) for v in vs],
        "v_lat": [float(v.lat) for v in vs],
        "v_lon": [float(v.lon) for v in vs],
        "v_speed": [float(v.speed_kmh) for v in vs],
        "v_phase": [v.phase for v in vs],
        "v_target": [v.target_station for v in vs],
        "v_radius_station": [v.radius_station for v in vs],
        "v_radius_entered_at": [_opt_int(v.radius_entered_at) for v in vs],
        "v_arrived_at": [_opt_int(v.arrived_at) for v in vs],
        "v_dump_visited": [bool(v.dump_visited) for v in vs],
        "v_decided": [bool(v.decided) for v in vs],
        "v_predicted_start": [_opt_int(v.predicted_start) for v in vs],
    }


class PgStore:
    """Единственный писатель таблиц processor / stations / vehicles.

    Соединение в autocommit: транзакции явные (claim, migrate), а чекпоинт — одна инструкция.
    При autocommit off psycopg отдельно синхронизирует неявный BEGIN, и чекпоинт стоит три round-trip вместо одного.
    """

    def __init__(self, dsn: str, migrations_dir: Path = MIGRATIONS_DIR) -> None:
        self._migrations = migrations_dir
        # statement_timeout исполняет сервер; обрыв сети ловят только клиентские сроки: данные без подтверждения
        # дольше 10 с (tcp_user_timeout) и молчание простаивающего соединения (keepalive 5 + 3 × 2 с) — ошибка
        self._conn = psycopg.connect(
            dsn,
            autocommit=True,
            connect_timeout=5,
            keepalives=1,
            keepalives_idle=5,
            keepalives_interval=2,
            keepalives_count=3,
            tcp_user_timeout=10_000,
            options=PG_OPTIONS,
        )

    def migrate(self) -> None:
        """Применить migrations/*.sql по порядку имён (CREATE TABLE IF NOT EXISTS) и сверить колонки с кодом.

        Существующую таблицу CREATE TABLE IF NOT EXISTS не меняет: том от другой версии схемы — отказ
        schema_changed (make reset), а не UndefinedColumn на claim или чекпоинте и цикл рестартов.
        """
        files = sorted(self._migrations.glob("*.sql"))
        if not files:
            raise FileNotFoundError(f"нет миграций в {self._migrations} (каталог задаёт VQ_MIGRATIONS)")
        with self._conn.transaction():
            self._conn.execute("SELECT pg_advisory_xact_lock(%s)", (MIGRATE_LOCK_KEY,))
            for path in files:
                self._conn.execute(path.read_bytes())
            columns: dict[str, list[str]] = {}
            for table, column in self._conn.execute(_COLUMNS, (list(SCHEMA),)).fetchall():
                columns.setdefault(table, []).append(column)
            diff = schema_diff(columns)
            if diff:
                raise StartRefused("schema_changed", diff)

    def claim(self, site_id: str, logic_version: str, config_hash: str) -> Claim:
        """Забрать площадку: epoch + 1 и чтение состояния — одна транзакция, строго в порядке §2.4.

        Отказ (StartRefused) откатывает транзакцию целиком: ни epoch, ни новая строка processor не остаются.
        """
        conn = self._conn
        with conn.transaction():
            conn.execute(_INSERT_PROCESSOR, (site_id, logic_version, config_hash))
            row = conn.execute(_BUMP_EPOCH, (site_id,)).fetchone()
            if row is None:  # строка есть всегда: её создал INSERT выше или прошлый claim
                raise RuntimeError(f"нет строки processor для {site_id}")
            epoch, next_offset, now_ts, db_version, db_hash, accepted, not_newer, invalid = row
            versions_differ = (db_version, db_hash) != (logic_version, config_hash)
            if versions_differ and next_offset is not None:
                reason = "logic_version_changed" if db_version != logic_version else "config_changed"
                raise StartRefused(
                    reason,
                    f"в Postgres logic_version={db_version} config_hash={db_hash}, "
                    f"в коде logic_version={logic_version} config_hash={config_hash}",
                )
            others, has_rows = conn.execute(_FOREIGN_STATE, (site_id,)).fetchone() or (None, False)
            if others:
                raise StartRefused(
                    "config_changed", f"в Postgres состояние другой площадки ({', '.join(others)}), в коде {site_id}"
                )
            if next_offset is None and has_rows:
                raise StartRefused("config_changed", "в Postgres есть строки stations/vehicles, а next_offset пуст")
            if versions_differ:  # состояния нет: версии просто переписываются
                conn.execute(_SET_VERSIONS, (logic_version, config_hash, site_id))
            state = None
            if next_offset is not None:
                # колонки называются как поля модели: строки собираются по именам, не по позициям
                stations = conn.cursor(row_factory=class_row(Station)).execute(_SELECT_STATIONS).fetchall()
                vehicles = conn.cursor(row_factory=class_row(Vehicle)).execute(_SELECT_VEHICLES).fetchall()
                state = SiteState(
                    now=now_ts,
                    vehicles={v.unit_uuid: v for v in vehicles},
                    stations={s.station_uuid: s for s in stations},
                )
        return Claim(epoch, next_offset, now_ts, Counters(accepted, not_newer, invalid), state)

    def checkpoint(self, site_id: str, epoch: int, next_offset: int, counters: Counters, state: SiteState) -> bool:
        """Записать состояние и offset атомарно. False — epoch уже не мой (фенсинг), ничего не записано."""
        params = checkpoint_params(site_id, epoch, next_offset, counters, state)
        row = self._conn.execute(_CHECKPOINT, params).fetchone()
        return row is not None and row[0] == 1

    def close(self) -> None:
        self._conn.close()
