"""PgStore на настоящем Postgres (§6.2): epoch, фенсинг, побитовое состояние, рестарт без потерь и повторов,
сторож вызовов на замёрзшем сервере."""

import copy
import json
import random
import subprocess
import sys
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Any

import psycopg
import pytest

from tests.dispatcher_helpers import line_site, scenario
from vq.core.engine import LOGIC_VERSION, Engine
from vq.core.model import SERVED, SERVICE, TO_DUMP, TO_STATION, WAITING, SiteState, Station, Vehicle
from vq.dispatcher.store import Counters, PgStore, StartRefused, checkpoint_params

pytestmark = pytest.mark.integration

IMAGE = "postgres:18.4-trixie"
SITE = "site-1"
HASH = "h" * 64
ROOT = Path(__file__).resolve().parents[1]


@pytest.fixture(scope="module")
def pg() -> Iterator[Any]:
    from testcontainers.community.postgres import PostgresContainer

    with PostgresContainer(IMAGE, driver=None) as container:
        yield container


@pytest.fixture(scope="module")
def dsn(pg: Any) -> str:
    return pg.get_connection_url()


@pytest.fixture
def admin(dsn: str) -> Iterator[psycopg.Connection]:
    """Отдельное соединение: видит только зафиксированное. Каждый тест — с чистой схемы."""
    with psycopg.connect(dsn, autocommit=True) as conn:
        conn.execute("DROP TABLE IF EXISTS processor, stations, vehicles")
        yield conn


@pytest.fixture
def store(dsn: str, admin: psycopg.Connection) -> Iterator[PgStore]:
    s = PgStore(dsn)
    s.migrate()
    yield s
    s.close()


def tables(admin: psycopg.Connection) -> dict[str, list[tuple]]:
    """Полный снимок таблиц, включая updated_at: «ничего не изменилось» проверяется буквально."""
    return {t: admin.execute(f"SELECT * FROM {t} ORDER BY 1").fetchall() for t in ("processor", "stations", "vehicles")}


def tricky_state() -> SiteState:
    """Значения на границах точности и NULL: float с хвостом, денормализованные, −0.0, NULL против '[]'."""
    vehicles = {
        "T-02": Vehicle(
            "T-02",
            "S1",
            4_102_444_799,
            0.1 + 0.2,
            1e-300,
            49.12880000000001,
            WAITING,
            "S2",
            radius_station="S2",
            radius_entered_at=1_789_473_600,
            arrived_at=1_789_473_601,
            dump_visited=False,
            decided=True,
            predicted_start=1_789_473_830,
        ),
        "T-01": Vehicle("T-01", "S1", 1_000_000_000, -0.0, 5e-324, 0.0, TO_DUMP, "S1", dump_visited=True),
        "Т-кир": Vehicle(
            "Т-кир",
            "S2",
            1_789_473_599,
            89.99999999999999,
            -179.99999999999997,
            299.99999999999994,
            SERVICE,
            "S2",
            radius_station='Ст-кир "q" ✓',
            radius_entered_at=1,
            arrived_at=2,
            decided=True,
        ),
        "T-04": Vehicle("T-04", "S2", 1_789_473_000, 49.1288, 142.66010000000003, 36.0, SERVED, "S2"),
        "T-03": Vehicle("T-03", "S1", 1_789_473_001, -49.1288, -142.6601, 0.5, TO_STATION, "S1", predicted_start=-5),
        "T-05": Vehicle(  # в радиусе нецелевой станции: вход запомнен, цель другая
            "T-05", "S1", 1_789_473_002, 49.2, 142.7, 36.0, TO_STATION, "S2", radius_station="S1", radius_entered_at=7
        ),
    }
    stations = {
        "S2": Station("S2", "Т-кир", 1_789_473_500, 1_789_473_400, "[]"),
        "S1": Station("S1", None, None, None, None),
        "S3": Station(
            "S3",
            None,
            None,
            1_789_473_000,
            '[{"unit_uuid":"Т-кир \\"q\\" \\\\ ✓","eta":null,"service_start":"2026-09-15T11:59:00Z"}]',
        ),
    }
    return SiteState(now=4_102_444_799, vehicles=vehicles, stations=stations)


def float_bits(state: SiteState) -> dict[str, tuple[str, str, str]]:
    return {u: (v.lat.hex(), v.lon.hex(), v.speed_kmh.hex()) for u, v in state.vehicles.items()}


# --- миграция и claim ---------------------------------------------------------------


def test_migrate_is_idempotent_and_creates_spec_columns(store, admin):
    store.migrate()
    cols = admin.execute(
        "SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns "
        "WHERE table_schema = 'public' ORDER BY table_name, ordinal_position"
    ).fetchall()
    assert ("vehicles", "lat", "double precision", "NO") in cols
    assert ("vehicles", "radius_station", "text", "YES") in cols
    assert ("vehicles", "predicted_start", "bigint", "YES") in cols
    assert ("stations", "last_published_queue", "text", "YES") in cols
    assert ("processor", "next_offset", "bigint", "YES") in cols
    assert len(cols) == 10 + 6 + 15
    vehicle_cols = [c[1] for c in cols if c[0] == "vehicles"]
    assert vehicle_cols[vehicle_cols.index("radius_station") + 1] == "radius_entered_at"


def test_session_has_spec_timeouts_keepalives_and_autocommit(store):
    """§2.4: таймауты сессии и TCP keepalive действительно применены к соединению PgStore.

    keepalives_interval/count и tcp_user_timeout — клиентские сроки на обрыв сети: без них запрос в оборванное
    соединение ждёт TCP-ретраев около 15 минут, а мёртвое простаивающее соединение находится через ~11 минут.
    """
    conn = store._conn
    names = ("statement_timeout", "lock_timeout", "idle_in_transaction_session_timeout")
    shown = {name: conn.execute(f"SHOW {name}").fetchone()[0] for name in names}
    assert shown == {"statement_timeout": "5s", "lock_timeout": "2s", "idle_in_transaction_session_timeout": "10s"}
    params = conn.info.get_parameters()
    keys = ("connect_timeout", "keepalives", "keepalives_idle", "keepalives_interval", "keepalives_count")
    assert {k: params.get(k) for k in (*keys, "tcp_user_timeout")} == {
        "connect_timeout": "5",
        "keepalives": "1",
        "keepalives_idle": "5",
        "keepalives_interval": "2",
        "keepalives_count": "3",
        "tcp_user_timeout": "10000",
    }
    assert conn.autocommit  # чекпоинт — одна инструкция за один round-trip


def test_claim_creates_row_and_increments_epoch(store, admin):
    first = store.claim(SITE, LOGIC_VERSION, HASH)
    assert (first.epoch, first.next_offset, first.now_ts, first.state) == (1, None, None, None)
    assert first.counters == Counters()
    second = store.claim(SITE, LOGIC_VERSION, HASH)
    assert second.epoch == 2
    assert admin.execute("SELECT epoch, next_offset FROM processor WHERE site_id = %s", (SITE,)).fetchone() == (2, None)


def test_stale_schema_refuses_start_and_rolls_back(dsn, store, admin):
    """Том со схемой другой версии кода (до колонки radius_station): CREATE TABLE IF NOT EXISTS её не добавит.

    Без проверки claim падал бы на UndefinedColumn (выход 1 и цикл рестартов), а при пустом состоянии — первый
    чекпоинт. Отказ schema_changed с подсказкой make reset называет и недостающие, и лишние колонки.
    """
    claim = store.claim(SITE, LOGIC_VERSION, HASH)
    assert store.checkpoint(SITE, claim.epoch, 5, Counters(5, 0, 0), tricky_state())
    admin.execute("ALTER TABLE vehicles DROP COLUMN radius_station")
    admin.execute("ALTER TABLE stations ADD COLUMN legacy text")
    before = tables(admin)
    restarted = PgStore(dsn)
    try:
        with pytest.raises(StartRefused) as refused:
            restarted.migrate()
    finally:
        restarted.close()
    assert refused.value.reason == "schema_changed" and refused.value.hint == "make reset"
    assert refused.value.detail == "stations: лишние колонки legacy; vehicles: нет колонок radius_station"
    assert tables(admin) == before


# --- фенсинг ------------------------------------------------------------------------


def test_checkpoint_with_old_epoch_is_rejected_and_changes_nothing(dsn, store, admin):
    cfg = line_site()
    stream = scenario(cfg, horizon=120)
    zombie = Engine(cfg)
    for r in stream[:300]:
        zombie.apply(r)
    old = store.claim(SITE, LOGIC_VERSION, HASH)
    assert store.checkpoint(SITE, old.epoch, 300, Counters(300, 0, 0), zombie.state)
    other = PgStore(dsn)  # второй экземпляр забирает площадку
    try:
        new = other.claim(SITE, LOGIC_VERSION, HASH)
        assert new.epoch == old.epoch + 1 and new.next_offset == 300
        before = tables(admin)
        for r in stream[300:600]:  # зомби продолжает и пытается записать
            zombie.apply(r)
        assert not store.checkpoint(SITE, old.epoch, 600, Counters(600, 0, 0), zombie.state)
        assert tables(admin) == before
        assert other.checkpoint(SITE, new.epoch, 600, Counters(600, 0, 0), zombie.state)
        assert tables(admin) != before
    finally:
        other.close()


# --- побитовое состояние ---------------------------------------------------------------


def test_state_round_trips_bit_exact(store, admin):
    state = tricky_state()
    claim = store.claim(SITE, LOGIC_VERSION, HASH)
    assert store.checkpoint(SITE, claim.epoch, 123, Counters(5, 6, 7), state)
    got = store.claim(SITE, LOGIC_VERSION, HASH)
    assert (got.next_offset, got.now_ts, got.counters) == (123, 4_102_444_799, Counters(5, 6, 7))
    assert got.state == state
    assert float_bits(got.state) == float_bits(state)
    assert got.state.vehicles["T-01"].lat.hex() == "-0x0.0p+0"
    assert [(u, v.radius_station) for u, v in got.state.vehicles.items()] == [
        ("T-01", None),
        ("T-02", "S2"),
        ("T-03", None),
        ("T-04", None),
        ("T-05", "S1"),
        ("Т-кир", 'Ст-кир "q" ✓'),
    ]
    assert admin.execute("SELECT count(*) FROM vehicles WHERE radius_station IS NULL").fetchone() == (3,)
    assert got.state.stations["S1"].last_published_queue is None
    assert got.state.stations["S2"].last_published_queue == "[]"
    assert list(got.state.vehicles) == sorted(state.vehicles)  # ORDER BY ключу
    assert list(got.state.stations) == ["S1", "S2", "S3"]
    assert admin.execute("SELECT count(*) FROM stations WHERE last_published_queue IS NULL").fetchone() == (1,)


def test_row_order_and_repeated_upserts_do_not_matter(store):
    state = tricky_state()
    claim = store.claim(SITE, LOGIC_VERSION, HASH)
    rng = random.Random(7)
    for i in range(5):  # тот же набор строк в разном порядке словарей, с изменениями между чекпоинтами
        units = list(state.vehicles)
        rng.shuffle(units)
        state = SiteState(state.now, {u: state.vehicles[u] for u in units}, dict(reversed(state.stations.items())))
        state.vehicles["T-04"].last_ts += 1
        state.stations["S1"].last_published_queue = None if i % 2 else "[]"
        assert store.checkpoint(SITE, claim.epoch, 10 + i, Counters(i, 0, 0), state)
    got = store.claim(SITE, LOGIC_VERSION, HASH).state
    assert got == state and float_bits(got) == float_bits(state)


def test_empty_state_with_null_now_round_trips(store):
    state = Engine(line_site()).state  # только станции: после пачки невалидных сообщений
    claim = store.claim(SITE, LOGIC_VERSION, HASH)
    assert store.checkpoint(SITE, claim.epoch, 3, Counters(0, 0, 3), state)
    got = store.claim(SITE, LOGIC_VERSION, HASH)
    assert (got.next_offset, got.now_ts, got.counters) == (3, None, Counters(0, 0, 3))
    assert got.state == state


def test_checkpoint_params_are_sorted_by_key():
    params = checkpoint_params(SITE, 1, 2, Counters(), tricky_state())
    assert params["v_uuid"] == sorted(tricky_state().vehicles)
    assert params["s_uuid"] == ["S1", "S2", "S3"]


# --- рестарт: тот же поток через настоящий Postgres -----------------------------------------


def test_restart_from_postgres_is_exactly_once_and_byte_identical(store):
    cfg = line_site()
    stream = scenario(cfg)
    ref = Engine(cfg)
    ref_outputs = [ref.apply(r).outputs for r in stream]
    # точки разреза — сразу после сообщений, давших выходы: повтор такого сообщения обязан дать ноль записей
    produced = [k for k in range(1, len(stream)) if ref_outputs[k - 1]]
    decisions = [k for k in produced if any(o.topic == "decision.v1" for o in ref_outputs[k - 1])]
    cuts = sorted({*produced[:: max(1, len(produced) // 6)], decisions[0], decisions[-1]})
    first = Engine(cfg)
    done = 0
    for k in cuts:
        for r in stream[done:k]:
            first.apply(r)
        done = k
        own = store.claim(SITE, LOGIC_VERSION, HASH)
        assert store.checkpoint(SITE, own.epoch, k, Counters(k, 0, 0), first.state)
        restored = store.claim(SITE, LOGIC_VERSION, HASH)
        assert restored.next_offset == k and restored.state is not None
        assert restored.state == first.state
        engine = Engine(cfg, restored.state)
        assert engine.apply(stream[k - 1]).outputs == ()  # последнее учтённое не применяется второй раз
        assert [engine.apply(r).outputs for r in stream[k:]] == ref_outputs[k:]
        assert engine.state == ref.state


# --- версии логики и конфигурации -------------------------------------------------------------


@pytest.mark.parametrize(
    ("logic_version", "cfg_hash", "reason"),
    [
        ("v-next", HASH, "logic_version_changed"),
        (LOGIC_VERSION, "0" * 64, "config_changed"),
        ("v-next", "0" * 64, "logic_version_changed"),
    ],
)
def test_version_mismatch_with_state_refuses_and_rolls_back(store, admin, logic_version, cfg_hash, reason):
    claim = store.claim(SITE, LOGIC_VERSION, HASH)
    assert store.checkpoint(SITE, claim.epoch, 5, Counters(5, 0, 0), copy.deepcopy(tricky_state()))
    before = tables(admin)
    with pytest.raises(StartRefused) as refused:
        store.claim(SITE, logic_version, cfg_hash)
    assert refused.value.reason == reason and refused.value.hint == "make reset"
    assert tables(admin) == before  # epoch не увеличен: транзакция откачена
    assert store.claim(SITE, LOGIC_VERSION, HASH).epoch == claim.epoch + 1


def test_version_mismatch_with_empty_state_is_accepted_and_updated(store, admin):
    first = store.claim(SITE, LOGIC_VERSION, HASH)
    second = store.claim(SITE, "v-next", "0" * 64)
    assert second.epoch == first.epoch + 1 and second.state is None
    row = admin.execute("SELECT logic_version, config_hash FROM processor WHERE site_id = %s", (SITE,)).fetchone()
    assert row == ("v-next", "0" * 64)


def test_state_of_another_site_refuses_and_rolls_back(store, admin):
    """Смена [site] id без make reset (§2.5): в базе состояние другой площадки — отказ config_changed.

    В stations и vehicles нет site_id, и claim новой площадки иначе загрузил бы чужие станции и машины.
    """
    claim = store.claim(SITE, LOGIC_VERSION, HASH)
    assert store.checkpoint(SITE, claim.epoch, 5, Counters(5, 0, 0), tricky_state())
    before = tables(admin)
    with pytest.raises(StartRefused) as refused:
        store.claim("site-2", LOGIC_VERSION, "0" * 64)
    assert refused.value.reason == "config_changed" and SITE in refused.value.detail
    assert refused.value.hint == "make reset"
    assert tables(admin) == before  # строка site-2 не создана: транзакция откачена
    assert store.claim(SITE, LOGIC_VERSION, HASH).epoch == claim.epoch + 1  # своя площадка работает дальше


def test_other_site_claimed_without_checkpoint_also_refuses(store, admin):
    """Экземпляр другой площадки уже забрал базу, но ещё ничего не записал: он может записать в любой момент."""
    store.claim(SITE, LOGIC_VERSION, HASH)
    before = tables(admin)
    with pytest.raises(StartRefused) as refused:
        store.claim("site-2", LOGIC_VERSION, HASH)
    assert refused.value.reason == "config_changed"
    assert tables(admin) == before


def test_state_rows_without_processor_offset_refuse(store, admin):
    """Строки stations/vehicles при пустом next_offset — состояние неизвестного происхождения: отказ, а не загрузка."""
    claim = store.claim(SITE, LOGIC_VERSION, HASH)
    assert store.checkpoint(SITE, claim.epoch, 5, Counters(5, 0, 0), tricky_state())
    admin.execute("DELETE FROM processor")
    before = tables(admin)
    with pytest.raises(StartRefused) as refused:
        store.claim(SITE, LOGIC_VERSION, HASH)
    assert refused.value.reason == "config_changed"
    assert tables(admin) == before


# --- сторож: замёрзший Postgres ------------------------------------------------------------------

# Процессор с настоящим PgStore в отдельном процессе: сторож завершает процесс через os._exit, как в проде.
STALL_CHILD = """
import sys
from tests.dispatcher_helpers import FakeSink, batch, example
from tests.helpers import two_stations
from vq.dispatcher import loop
from vq.dispatcher.store import PgStore
from vq.logs import setup_logging

setup_logging()  # site, epoch и offset строки выхода — из контекста процессора
cfg = two_stations()
deadline = loop.Deadline(float(sys.argv[2]))  # в проде 15 с; выход — настоящий os._exit(1)
p = loop.Processor(cfg, "cfg-hash", PgStore(sys.argv[1]), FakeSink([]), deadline=deadline)
p.on_assign(0, 100)
print("ready", file=sys.stderr, flush=True)
sys.stdin.readline()
p.handle_batch(batch(example(cfg), 0))
print("returned", file=sys.stderr, flush=True)
"""


def test_frozen_postgres_exits_1_within_deadline(pg, dsn, store):
    """§2.4 «exit(1) при таймауте Postgres» — и когда сервер заморожен (docker pause).

    statement_timeout исполняет сам сервер, а TCP-сегменты за замёрзший процесс подтверждает ядро: без сторожа
    чекпоинт висел бы до разморозки, heartbeat librdkafka держал бы партицию, а /metrics отвечал бы.
    """
    deadline_s = 1.0
    child = subprocess.Popen(
        [sys.executable, "-c", STALL_CHILD, dsn, str(deadline_s)],
        stdin=subprocess.PIPE,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=ROOT,
    )
    assert child.stdin is not None and child.stderr is not None
    assert child.stderr.readline().strip() == "ready"
    cid = pg.get_wrapped_container().id
    subprocess.run(["docker", "pause", cid], check=True, capture_output=True)
    try:
        t0 = time.monotonic()
        child.stdin.write("go\n")
        child.stdin.flush()
        try:
            code: int | None = child.wait(timeout=deadline_s + 9)
        except subprocess.TimeoutExpired:
            child.kill()
            code = None
        took = time.monotonic() - t0
    finally:
        subprocess.run(["docker", "unpause", cid], check=True, capture_output=True)
    out, err = child.communicate(timeout=10)
    assert code == 1, f"процесс не вышел за {took:.1f} с (код {code}); stderr: {err[-500:]}"
    assert took < deadline_s + 2
    last = json.loads(out.strip().splitlines()[-1])
    assert {k: last[k] for k in ("event", "level", "op", "site", "epoch", "offset", "next_offset")} == {
        "event": "exit_postgres_stalled",
        "level": "ERROR",
        "op": "checkpoint",
        "site": "test",
        "epoch": 1,
        "offset": 0,
        "next_offset": 5,
    }
