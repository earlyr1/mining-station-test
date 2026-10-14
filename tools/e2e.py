"""make e2e и make e2e-chaos (§6.3): сквозной прогон поднятого стенда с хоста.

make уже выполнил `make reset UP_PROFILES=` (свежие тома, стенд без демо-генератора) и передал VQ_KAFKA и VQ_PG_DSN
(опубликованные порты на 127.0.0.1), VQ_UID/VQ_GID и имя проекта compose из .env. Сценарий:

1. Генератор x20, час симуляции, seed 42, на переднем плане:
   `docker compose run --rm --use-aliases -T generator vq-generator --speed 20 --duration 3600 --seed 42`;
   нужны код 0 и summary.error = null (./out/summary.json).
   С --chaos во время прогона две аварии dispatcher:
   - на 25 % — точка аварии VQ_CRASH_FILE (`docker compose exec -T dispatcher touch /tmp/vq-crash`): процесс
     выходит с кодом 137 после flush ближайшей пачки с записями и до её чекпоинта, так повтор детерминирован;
   - около 60 % (момент из random.Random(seed)) — `kill -KILL -1` изнутри контейнера (PID 1 — tini).
   `docker compose kill` не годится: Docker считает его ручной остановкой, и restart: unless-stopped не срабатывает.
   Для каждой аварии — RestartCount до и после, код выхода (событие die) и время от выхода процесса
   (State.FinishedAt) до старта контейнера (State.StartedAt), claim (строка лога), нового epoch в Postgres
   (опрос каждые 50 мс) и первого чекпоинта после claim (updated_at). Все моменты — по часам ВМ Docker:
   State.*, лог dispatcher, clock_timestamp() и updated_at Postgres.
2. High watermark telemetry.v1; ждём, пока processor.next_offset его догонит.
3. vq-verify в образе приложения (`docker compose run --rm --no-deps -T dispatcher vq-verify`): JSON последней строки.
4. Проверки PASS/FAIL с числами; с --chaos — ещё таблица восстановления для README.
   Код выхода 0, только если прошли все. Итог — JSON последней строкой stdout и в out/<режим>.json.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import subprocess
import sys
import threading
import time
import traceback
from collections.abc import Callable, Mapping, Sequence
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from functools import partial
from pathlib import Path
from typing import IO, Any

import psycopg
from confluent_kafka import Consumer, TopicPartition

from vq.config import load_config
from vq.dispatcher.loop import CRASH_EXIT_CODE
from vq.dispatcher.main import TELEMETRY_TOPIC
from vq.tools.verify import OUTPUT_TOPICS

ROOT = Path(__file__).resolve().parents[1]
OUT = ROOT / "out"
CRASH_FILE = "/tmp/vq-crash"  # VQ_CRASH_FILE сервиса dispatcher в docker-compose.yml
SPEED, DURATION, SEED = 20, 3600, 42
POLL_S = 0.05  # опрос строки processor во время аварий
RESTART_TIMEOUT_S = 60.0
RECOVERY_TIMEOUT_S = 60.0
CATCH_UP_TIMEOUT_S = 300.0
GENERATOR_GRACE_S = 180.0  # сверх duration / speed: старт контейнера, досылка отложенного, flush
VERIFY_TIMEOUT_S = 900.0
CRASH_TITLES = {
    "crash_file": "авария 1: точка аварии VQ_CRASH_FILE",
    "kill": "авария 2: kill -KILL -1 изнутри контейнера",
}


# --- чистые части -------------------------------------------------------------------------------------------

_ISO = re.compile(r"(\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d)(?:\.(\d+))?(Z|[+-]\d\d:\d\d)")


def parse_time(s: str | None) -> datetime | None:
    """Время Docker (RFC 3339, до наносекунд) или строки лога → UTC; нулевое время Docker (0001-…) → None."""
    if not s:
        return None
    m = _ISO.fullmatch(s)
    if m is None:
        raise ValueError(f"не время ISO 8601: {s!r}")
    base, frac, tz = m.groups()
    if base.startswith("0001-"):
        return None
    micro = (frac or "").ljust(6, "0")[:6]
    return datetime.fromisoformat(f"{base}.{micro}{'+00:00' if tz == 'Z' else tz}").astimezone(UTC)


def crash_schedule(seed: int, run_s: float) -> tuple[float, float]:
    """Секунды от старта генератора: точка аварии — на 25 % прогона, kill — около 60 %, момент из Random(seed)."""
    return 0.25 * run_s, run_s * random.Random(seed).uniform(0.55, 0.65)


def last_json(text: str) -> dict[str, Any] | None:
    """JSON-объект последней непустой строки вывода инструмента (vq-verify, строка лога) или None."""
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return None
    try:
        obj = json.loads(lines[-1])
    except ValueError:
        return None
    return obj if isinstance(obj, dict) else None


def logged_summary(events: Sequence[Mapping[str, Any]]) -> dict[str, Any] | None:
    """Итог генератора из последнего события summary его лога (§4.4); None — такого события нет."""
    found = next((e.get("summary") for e in reversed(events) if e.get("event") == "summary"), None)
    return found if isinstance(found, dict) else None


@dataclass(frozen=True)
class PgSample:
    """Опрос строки processor: at — clock_timestamp() Postgres, updated_at — момент последнего claim или чекпоинта."""

    at: datetime
    epoch: int
    next_offset: int | None
    updated_at: datetime


def after_claim(
    samples: Sequence[PgSample], epoch_before: int, claim_offset: int | None
) -> tuple[datetime | None, datetime | None, datetime | None]:
    """(первый опрос с новым epoch, момент claim по Postgres, момент первого чекпоинта после claim).

    claim увеличивает epoch и ставит updated_at, не трогая next_offset; чекпоинт двигает next_offset. Если опрос
    застал строку уже после первого чекпоинта, момент claim по Postgres неизвестен (None) — он есть в логе.
    claim_offset — next_offset из строки claim в логе; None — берётся next_offset последнего опроса до claim.
    """
    idx = next((i for i, s in enumerate(samples) if s.epoch > epoch_before), None)
    if idx is None:
        return None, None, None
    seen = samples[idx]
    base = claim_offset
    if base is None:
        base = samples[idx - 1].next_offset if idx > 0 else seen.next_offset
    claim_pg = seen.updated_at if seen.next_offset == base else None
    ck = next((s for s in samples[idx:] if s.next_offset != base), None)
    return seen.at, claim_pg, None if ck is None else ck.updated_at


@dataclass
class Crash:
    """Одна авария dispatcher и восстановление после неё; моменты — по часам ВМ Docker."""

    kind: str  # crash_file | kill
    planned_s: float  # секунд от старта генератора
    restarts_before: int
    epoch_before: int | None
    sim_s: int | None = None  # примерная секунда симуляции
    restarts_after: int | None = None
    exit_code: int | None = None
    exited: datetime | None = None  # State.FinishedAt: процесс вышел
    started: datetime | None = None  # State.StartedAt: контейнер перезапущен
    claimed: datetime | None = None  # строка claim в логе
    claim_offset: int | None = None  # next_offset из claim: с него повтор
    epoch_after: int | None = None
    epoch_seen: datetime | None = None  # первый опрос Postgres с новым epoch
    claim_pg: datetime | None = None  # updated_at строки processor от claim, если опрос его застал
    checkpoint: datetime | None = None  # updated_at первого чекпоинта после claim
    records: int | None = None  # записей в пачке, после которой сработала точка аварии
    error: str | None = None


def recovery(c: Crash) -> dict[str, float | None]:
    """Секунды от выхода процесса до старта контейнера, claim, нового epoch в Postgres и первого чекпоинта."""

    def since_exit(t: datetime | None) -> float | None:
        return None if t is None or c.exited is None else round((t - c.exited).total_seconds(), 2)

    return {
        "restart_s": since_exit(c.started),
        "claim_s": since_exit(c.claimed),
        "epoch_seen_s": since_exit(c.epoch_seen),
        "checkpoint_s": since_exit(c.checkpoint),
    }


def crash_ok(c: Crash) -> bool:
    """Контейнер перезапущен политикой, процесс вышел с 137, новый claim и чекпоинт; точка аварии — после записей.

    137 у обеих аварий: os._exit точки аварии и 128 + SIGKILL у kill -KILL.
    """
    return (
        c.error is None
        and c.restarts_after is not None
        and c.restarts_after > c.restarts_before
        and c.exit_code == CRASH_EXIT_CODE
        and c.epoch_before is not None
        and c.epoch_after is not None
        and c.epoch_after > c.epoch_before
        and c.checkpoint is not None
        and (c.kind != "crash_file" or (c.records or 0) >= 1)
    )


def _num(x: float | None) -> str:
    return "—" if x is None else f"{x:.2f}"


def recovery_table(crashes: Sequence[Crash]) -> list[str]:
    """Таблица восстановления (Markdown) для README: строка на аварию."""
    lines = [
        "| Авария | Момент | Код выхода | RestartCount | выход → старт контейнера, с | выход → claim, с "
        "| выход → новый epoch виден в Postgres (опрос 50 мс), с | выход → первый чекпоинт (updated_at), с "
        "| записей в пачке |",
        "|---|---|---|---|---|---|---|---|---|",
    ]
    for c in crashes:
        r = recovery(c)
        name = CRASH_TITLES.get(c.kind, c.kind).split(": ", 1)[-1]
        moment = f"{c.planned_s:.0f} с прогона" + ("" if c.sim_s is None else f" (≈ {c.sim_s} с симуляции)")
        records = "—" if c.records is None else str(c.records)
        lines.append(
            f"| {name} | {moment} | {'—' if c.exit_code is None else c.exit_code} "
            f"| {c.restarts_before} → {'—' if c.restarts_after is None else c.restarts_after} "
            f"| {_num(r['restart_s'])} | {_num(r['claim_s'])} | {_num(r['epoch_seen_s'])} "
            f"| {_num(r['checkpoint_s'])} | {records} |"
        )
    return lines


@dataclass(frozen=True)
class Check:
    key: str
    name: str
    ok: bool
    detail: str


@dataclass
class Facts:
    """Всё, что собрал прогон: по этим данным считаются проверки (evaluate)."""

    chaos: bool
    gen_rc: int | None = None
    summary: dict[str, Any] | None = None
    hwm: int | None = None
    processor: dict[str, Any] | None = None
    waited_s: float | None = None  # None — не догнал за CATCH_UP_TIMEOUT_S
    verify_rc: int | None = None
    report: dict[str, Any] | None = None
    restarts_before: int | None = None
    restarts_after: int | None = None
    crashes: list[Crash] = field(default_factory=list)


def dig(obj: Any, *path: str) -> Any:
    for p in path:
        if not isinstance(obj, Mapping):
            return None
        obj = obj.get(p)
    return obj


def _topics(c: Mapping[str, Any] | None) -> str:
    c = c or {}
    return ", ".join(f"{t} {c.get(t)}" for t in OUTPUT_TOPICS)


# --- проверки §6.3: по функции на строку отчёта; нет данных (упал шаг) — провал, а не исключение ------------


def _generator(f: Facts) -> Check:
    s = f.summary or {}
    return Check(
        "generator",
        "генератор: код 0, summary.error = null",
        f.gen_rc == 0 and f.summary is not None and s.get("error") is None,
        f"код {f.gen_rc}, error {s.get('error')!r}, stopped_by {s.get('stopped_by')}, тиков {s.get('ticks')}, "
        f"отправлено {s.get('produced')}",
    )


def _hwm(f: Facts) -> Check:
    produced = dig(f.summary, "produced")
    return Check(
        "hwm",
        "telemetry.v1: high watermark = summary.produced",
        f.hwm is not None and f.hwm == produced,
        f"high watermark {f.hwm}, produced {produced}",
    )


def _caught_up(f: Facts) -> Check:
    nxt = None if f.processor is None else f.processor.get("next_offset") or 0
    waited = "не догнал" if f.waited_s is None else f"ждали {f.waited_s:.1f} с"
    return Check(
        "caught_up",
        "dispatcher догнал лог: processor.next_offset = high watermark",
        f.hwm is not None and nxt == f.hwm,
        f"next_offset {nxt}, {waited}",
    )


def _verify(f: Facts) -> Check:
    rep = f.report or {}
    return Check(
        "verify",
        "verify: queue.v1 и decision.v1 совпали с офлайн-прогоном ядра",
        f.verify_rc == 0 and rep.get("ok") is True,
        f"код {f.verify_rc}, N {rep.get('n')}, записей до N: {_topics(dig(rep, 'log', 'kept'))}, "
        f"расхождений {dig(rep, 'mismatches', 'total')}, счётчики сошлись: {dig(rep, 'counters', 'ok')}",
    )


def _drops(f: Facts) -> Check:
    nn, exp = dig(f.processor, "msgs_not_newer"), dig(f.summary, "expected_drops")
    return Check(
        "drops",
        "отброшено = ожидалось: processor.msgs_not_newer = summary.expected_drops",
        nn is not None and nn == exp,
        f"{nn} и {exp}",
    )


def _already_serving(f: Facts) -> Check:
    ign = dig(f.summary, "decisions", "ignored") or {}
    return Check(
        "already_serving",
        "рекомендации не опоздали: summary.decisions.ignored.already_serving = 0",
        f.summary is not None and ign.get("already_serving") == 0,
        f"ignored {json.dumps(ign, ensure_ascii=False)}, applied {dig(f.summary, 'decisions', 'applied')} "
        f"из received {dig(f.summary, 'decisions', 'received')}",
    )


def _fsm_resync(f: Facts) -> Check:
    rs = dig(f.report, "offline", "fsm_resync")
    return Check(
        "fsm_resync",
        "самолечение и конец заезда в радиусе разгрузки не срабатывали: fsm_resync офлайн-прогона = 0",
        rs == 0,
        f"fsm_resync {rs}",
    )


def _decision_kinds(f: Facts) -> Check:
    d = dig(f.report, "offline", "decisions") or {}
    return Check(
        "decision_kinds",
        "есть и recommended, и no_gain",
        (d.get("recommended") or 0) > 0 and (d.get("no_gain") or 0) > 0,
        ", ".join(f"{k} {v}" for k, v in d.items()) or "нет данных",
    )


def _occupations(f: Facts) -> Check:
    occ = dig(f.report, "offline", "occupations") or {}
    lo = occ.get("per_unit_min")
    return Check(
        "occupations",
        "каждая машина прошла цикл: занятий станции на машину ≥ 1",
        lo is not None and lo >= 1,
        f"занятий {occ.get('total')}, на машину {lo}..{occ.get('per_unit_max')}, "
        f"без занятий {occ.get('units_without')}",
    )


def _invariants(f: Facts) -> Check:
    inv = dig(f.report, "invariants") or {}
    return Check(
        "invariants",
        "инварианты офлайн-прогона: занявшая — в service у своей станции, машина — в одной строке очередей",
        inv.get("ok") is True and inv.get("violations") == 0,
        f"нарушений {inv.get('violations')}",
    )


def _restarts(f: Facts) -> Check:
    want = len(CRASH_TITLES) if f.chaos else 0
    grown = None if f.restarts_before is None or f.restarts_after is None else f.restarts_after - f.restarts_before
    return Check(
        "restarts",
        f"перезапусков dispatcher ровно {want}" + (" (по одному на аварию)" if f.chaos else " (без аварий)"),
        grown == want,
        f"RestartCount {f.restarts_before} → {f.restarts_after}",
    )


def _crash(kind: str, f: Facts) -> Check:
    name = f"{CRASH_TITLES[kind]}: перезапуск политикой, claim и чекпоинт"
    c = next((c for c in f.crashes if c.kind == kind), None)
    if c is None:
        return Check(kind, name, False, "не выполнялась")
    r = recovery(c)
    detail = (
        f"RestartCount {c.restarts_before} → {c.restarts_after}, код выхода {c.exit_code}, "
        f"epoch {c.epoch_before} → {c.epoch_after}; от выхода процесса: старт контейнера {_num(r['restart_s'])} с, "
        f"claim {_num(r['claim_s'])} с, новый epoch виден в Postgres {_num(r['epoch_seen_s'])} с, "
        f"первый чекпоинт {_num(r['checkpoint_s'])} с"
    )
    if kind == "crash_file":
        detail += f"; записей в пачке {c.records}"
    if c.error:
        detail += f"; ошибка: {c.error}"
    return Check(kind, name, crash_ok(c), detail)


def _replays(f: Facts) -> Check:
    replays = dig(f.report, "log", "replays") or {}
    total = sum(v for v in replays.values() if isinstance(v, int))
    crash_file = next((c for c in f.crashes if c.kind == "crash_file"), None)
    need = max(1, 0 if crash_file is None else crash_file.records or 0)
    return Check(
        "replays",
        "verify показал повтор: повторов ≥ 1 и не меньше записей пачки точки аварии",
        f.report is not None and total >= need,
        f"повторов {_topics(replays)}, всего {total}; нужно не меньше {need}",
    )


def _replays_identical(f: Facts) -> Check:
    differ = dig(f.report, "mismatches", "by_kind", "replay_differs") or 0
    return Check(
        "replays_identical",
        "все повторы совпали байт в байт",
        f.report is not None and differ == 0,
        f"повторов с другими байтами {differ}",
    )


def _epoch(f: Facts) -> Check:
    first = f.crashes[0].epoch_before if f.crashes else None
    last = dig(f.processor, "epoch")
    return Check(
        "epoch",
        "epoch вырос не меньше чем на 2",
        first is not None and last is not None and last - first >= 2,
        f"epoch до первой аварии {first}, в конце {last}",
    )


RUN_CHECKS: tuple[Callable[[Facts], Check], ...] = (
    _generator,
    _hwm,
    _caught_up,
    _verify,
    _drops,
    _already_serving,
    _fsm_resync,
    _decision_kinds,
    _occupations,
    _invariants,
    _restarts,
)
CHAOS_CHECKS: tuple[Callable[[Facts], Check], ...] = (
    *(partial(_crash, kind) for kind in CRASH_TITLES),
    _replays,
    _replays_identical,
    _epoch,
)


def evaluate(f: Facts) -> list[Check]:
    """Проверки §6.3 по собранным данным: каждого прогона, а с --chaos ещё аварий и повторов."""
    return [check(f) for check in RUN_CHECKS + (CHAOS_CHECKS if f.chaos else ())]


# --- Docker -------------------------------------------------------------------------------------------------


def sh(args: Sequence[str], timeout: float = 120.0) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        list(args), cwd=ROOT, capture_output=True, text=True, timeout=timeout, stdin=subprocess.DEVNULL
    )


def compose(*args: str, timeout: float = 120.0) -> subprocess.CompletedProcess[str]:
    return sh(["docker", "compose", *args], timeout=timeout)


class Container:
    """Контейнер сервиса compose: docker inspect, события die и JSON-строки лога."""

    def __init__(self, service: str) -> None:
        r = compose("ps", "-q", service)
        self.id = r.stdout.strip()
        if r.returncode != 0 or not self.id:
            raise RuntimeError(f"контейнер {service} не запущен ({r.stderr.strip()}): make e2e начинается с make reset")

    def inspect(self) -> dict[str, Any]:
        r = sh(["docker", "inspect", self.id], timeout=30)
        if r.returncode != 0:
            raise RuntimeError(f"docker inspect: {r.stderr.strip()}")
        info: dict[str, Any] = json.loads(r.stdout)[0]
        return info

    def exit_codes(self, since: float, until: float) -> list[int]:
        r = sh(
            [
                "docker",
                "events",
                "--since",
                f"{since:.3f}",
                "--until",
                f"{until:.3f}",
                "--filter",
                f"container={self.id}",
                "--filter",
                "event=die",
                "--format",
                "{{json .}}",
            ],
            timeout=30,
        )
        codes = []
        for line in r.stdout.splitlines():
            code = dig(json.loads(line), "Actor", "Attributes", "exitCode") if line.strip() else None
            if code is not None:
                codes.append(int(code))
        return codes

    def log_events(self, since: float) -> list[dict[str, Any]]:
        r = sh(["docker", "logs", "--since", f"{since:.3f}", self.id], timeout=60)
        out = []
        for line in (r.stdout + "\n" + r.stderr).splitlines():
            try:
                d = json.loads(line)
            except ValueError:
                continue
            if isinstance(d, dict) and "event" in d:
                out.append(d)
        return out


# --- Postgres и Kafka ---------------------------------------------------------------------------------------

_PROCESSOR = """
SELECT epoch, next_offset, now_ts, msgs_accepted, msgs_not_newer, msgs_invalid, updated_at
  FROM processor WHERE site_id = %s
"""
_POLL = "SELECT epoch, next_offset, updated_at, clock_timestamp() FROM processor WHERE site_id = %s"


def processor_row(dsn: str, site: str) -> dict[str, Any] | None:
    """Строка processor площадки; None — dispatcher ещё не применил схему или не забирал партицию."""
    try:
        with psycopg.connect(dsn, autocommit=True, connect_timeout=5) as conn:
            row = conn.execute(_PROCESSOR, (site,)).fetchone()
    except psycopg.errors.UndefinedTable:
        return None
    if row is None:
        return None
    keys = ("epoch", "next_offset", "now_ts", "msgs_accepted", "msgs_not_newer", "msgs_invalid", "updated_at")
    return dict(zip(keys, row, strict=True))


class PgPoller:
    """Фоновый опрос строки processor каждые POLL_S; моменты — по часам Postgres (clock_timestamp).

    Опросы дописывает фоновый поток, читает основной: доступ к списку — под замком.
    """

    def __init__(self, dsn: str, site: str) -> None:
        self.dsn, self.site = dsn, site
        self.errors = 0
        self._samples: list[PgSample] = []
        self._lock = threading.Lock()
        self._halt = threading.Event()
        self._thread = threading.Thread(target=self._run, name="vq-e2e-poller", daemon=True)

    def start(self) -> None:
        self._thread.start()

    def stop(self) -> None:
        self._halt.set()
        if self._thread.is_alive():
            self._thread.join(timeout=5)

    def mark(self) -> int:
        """Отметка «сейчас»: since(mark) вернёт только следующие опросы."""
        with self._lock:
            return len(self._samples)

    def since(self, mark: int) -> list[PgSample]:
        with self._lock:
            return self._samples[mark:]

    def latest(self) -> PgSample | None:
        with self._lock:
            return self._samples[-1] if self._samples else None

    def _run(self) -> None:
        conn: psycopg.Connection[Any] | None = None
        while not self._halt.is_set():
            t0 = time.monotonic()
            try:
                if conn is None:
                    conn = psycopg.connect(self.dsn, autocommit=True, connect_timeout=5)
                row = conn.execute(_POLL, (self.site,)).fetchone()
                if row is not None:
                    sample = PgSample(at=row[3], epoch=row[0], next_offset=row[1], updated_at=row[2])
                    with self._lock:
                        self._samples.append(sample)
            except psycopg.Error:
                self.errors += 1
                if conn is not None:
                    conn.close()
                conn = None
                self._halt.wait(0.5)
            self._halt.wait(max(0.0, POLL_S - (time.monotonic() - t0)))
        if conn is not None:
            conn.close()


def high_watermark(bootstrap: str, topic: str = TELEMETRY_TOPIC) -> int:
    c = Consumer({"bootstrap.servers": bootstrap, "group.id": "vq-e2e", "enable.auto.commit": False})
    try:
        _, high = c.get_watermark_offsets(TopicPartition(topic, 0), timeout=10, cached=False)
        return int(high)
    finally:
        c.close()


# --- генератор, аварии, verify ------------------------------------------------------------------------------


def say(text: str) -> None:
    print(text, flush=True)


class Generator:
    """`docker compose run` генератора: JSON-лог в stdout (итог — событие summary), прочее (librdkafka, трассировка)
    в stderr; строки обоих печатаются с префиксом."""

    def __init__(self, args: Sequence[str]) -> None:
        self.launched = time.monotonic()
        self.started: float | None = None  # событие start в логе генератора: первый тик
        self.events: list[dict[str, Any]] = []  # JSON-строки лога
        self.proc = subprocess.Popen(
            list(args),
            cwd=ROOT,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            bufsize=1,
        )
        assert self.proc.stdout is not None and self.proc.stderr is not None
        self._readers = [
            threading.Thread(target=self._read, args=(self.proc.stdout,), daemon=True),
            threading.Thread(target=self._read, args=(self.proc.stderr,), daemon=True),
        ]
        for t in self._readers:
            t.start()

    def _read(self, stream: IO[str]) -> None:
        for raw in stream:
            line = raw.rstrip("\n")
            event = last_json(line)
            if event is not None:
                self.events.append(event)
                if self.started is None and event.get("event") == "start":
                    self.started = time.monotonic()
            say(f"  [генератор] {line[:300]}")

    def t0(self) -> float:
        return self.started if self.started is not None else self.launched

    def running(self) -> bool:
        return self.proc.poll() is None

    def wait(self, timeout: float) -> int | None:
        """Код выхода; None — не завершился за timeout и остановлен."""
        try:
            rc: int | None = self.proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            say(f"  генератор не завершился за {timeout:.0f} с: SIGTERM")
            self.proc.terminate()
            try:
                self.proc.wait(timeout=60)
            except subprocess.TimeoutExpired:
                self.proc.kill()
            rc = None
        for t in self._readers:  # вывод дочитан: событие summary — в self.events
            t.join(timeout=5)
        return rc


def wait_restart(box: Container, before: int) -> dict[str, Any]:
    """Ждёт, пока RestartCount вырастет и контейнер снова будет running; иначе TimeoutError."""
    deadline = time.monotonic() + RESTART_TIMEOUT_S
    while True:
        info = box.inspect()
        st = info["State"]
        if info["RestartCount"] > before and st.get("Running") and not st.get("Restarting"):
            return info
        if time.monotonic() > deadline:
            raise TimeoutError(
                f"dispatcher не перезапустился за {RESTART_TIMEOUT_S:.0f} с: Status {st.get('Status')}, "
                f"RestartCount {info['RestartCount']}"
            )
        time.sleep(0.1)


def strike(kind: str) -> None:
    """Авария: взвести точку аварии (crash_file) или убить все процессы контейнера, кроме tini (kill)."""
    if kind == "crash_file":
        armed = compose("exec", "-T", "dispatcher", "touch", CRASH_FILE, timeout=30)
        if armed.returncode != 0:
            raise RuntimeError(f"не удалось взвести точку аварии: {armed.stderr.strip()}")
    else:
        # Код exec не важен: оболочку exec может добить выход PID 1 (tini) вместе с контейнером.
        compose("exec", "-T", "dispatcher", "sh", "-c", "kill -KILL -1", timeout=30)


def measure(c: Crash, box: Container, poller: PgPoller, mark: int, t_host: float) -> None:
    """Восстановление после аварии — в c: перезапуск, claim с новым epoch, первый чекпоинт, chaos_crash, код выхода.

    mark — отметка опросов Postgres до аварии; t_host — время хоста перед ней (логи и события Docker).
    """
    assert c.epoch_before is not None
    info = wait_restart(box, c.restarts_before)
    st = info["State"]
    c.restarts_after = info["RestartCount"]
    c.exited, c.started = parse_time(st.get("FinishedAt")), parse_time(st.get("StartedAt"))
    deadline = time.monotonic() + RECOVERY_TIMEOUT_S
    while True:
        events = box.log_events(t_host - 1)
        claim = next((e for e in events if e.get("event") == "claim" and (e.get("epoch") or 0) > c.epoch_before), None)
        if claim is not None:
            c.claimed, c.claim_offset, c.epoch_after = parse_time(claim["ts"]), claim["next_offset"], claim["epoch"]
            c.epoch_seen, c.claim_pg, c.checkpoint = after_claim(poller.since(mark), c.epoch_before, c.claim_offset)
            if c.checkpoint is not None:
                break
        if time.monotonic() > deadline:
            raise TimeoutError(f"нет claim и чекпоинта за {RECOVERY_TIMEOUT_S:.0f} с после перезапуска")
        time.sleep(0.2)
    fired = next((e for e in events if e.get("event") == "chaos_crash"), None)
    if fired is not None:
        c.records = fired.get("records")
    codes = box.exit_codes(t_host - 1, time.time() + 1)
    c.exit_code = codes[0] if codes else None


def crash(kind: str, box: Container, poller: PgPoller, planned_s: float, sim_s: int) -> Crash:
    """Авария dispatcher (kind: crash_file | kill) и замер восстановления."""
    last = poller.latest()
    c = Crash(kind, planned_s, box.inspect()["RestartCount"], None if last is None else last.epoch, sim_s=sim_s)
    mark, t_host = poller.mark(), time.time()
    try:
        if c.epoch_before is None:
            raise RuntimeError("строки processor ещё нет: dispatcher не забирал партицию")
        strike(kind)
        measure(c, box, poller, mark, t_host)
    except Exception as e:  # авария не удалась или не измерена: это провал её проверки, а не всего прогона
        c.error = f"{type(e).__name__}: {e}"
    finally:
        if kind == "crash_file":  # не сработавшая точка аварии не должна выстрелить потом
            compose("exec", "-T", "dispatcher", "rm", "-f", CRASH_FILE, timeout=30)
    r = recovery(c)
    say(
        f"  {CRASH_TITLES[kind]} на {planned_s:.0f} с (≈ {sim_s} с симуляции): код выхода {c.exit_code}, "
        f"RestartCount {c.restarts_before} → {c.restarts_after}, epoch {c.epoch_before} → {c.epoch_after}, "
        f"записей в пачке {'—' if c.records is None else c.records}; от выхода: старт {_num(r['restart_s'])} с, "
        f"claim {_num(r['claim_s'])} с, "
        f"epoch виден в Postgres {_num(r['epoch_seen_s'])} с, первый чекпоинт {_num(r['checkpoint_s'])} с"
        + (f"; ОШИБКА: {c.error}" if c.error else "")
    )
    return c


def run_chaos(gen: Generator, box: Container, poller: PgPoller, seed: int, speed: int, run_s: float) -> list[Crash]:
    crashes: list[Crash] = []
    for kind, at in zip(CRASH_TITLES, crash_schedule(seed, run_s), strict=True):
        while gen.running() and time.monotonic() < gen.t0() + at:
            time.sleep(0.05)
        if not gen.running():
            last = poller.latest()
            c = Crash(kind, at, box.inspect()["RestartCount"], None if last is None else last.epoch)
            c.error = "генератор завершился раньше"
            crashes.append(c)
            continue
        crashes.append(crash(kind, box, poller, at, int((time.monotonic() - gen.t0()) * speed)))
    return crashes


def wait_caught_up(dsn: str, site: str, hwm: int) -> tuple[dict[str, Any] | None, float | None]:
    """Ждёт processor.next_offset = hwm с печатью прогресса; (строка processor, сколько ждали или None)."""
    t0 = time.monotonic()
    shown = -5.0
    while True:
        row = processor_row(dsn, site)
        nxt = (row or {}).get("next_offset") or 0
        waited = time.monotonic() - t0
        if nxt == hwm:
            return row, waited
        if waited > CATCH_UP_TIMEOUT_S:
            return row, None
        if waited - shown >= 5.0:
            say(f"  dispatcher: next_offset {nxt} из {hwm}, отставание {hwm - nxt}, ждём {waited:.0f} с")
            shown = waited
        time.sleep(0.5)


def run_verify() -> tuple[int | None, dict[str, Any] | None]:
    r = compose("run", "--rm", "--no-deps", "-T", "dispatcher", "vq-verify", timeout=VERIFY_TIMEOUT_S)
    rep = last_json(r.stdout)
    lines = [line for line in r.stdout.splitlines() if line.strip()]
    for line in lines[:-1] if rep is not None else lines:
        say(f"  [verify] {line}")
    if rep is None or r.returncode not in (0, 1):
        say(f"  [verify] код {r.returncode}, stderr: {r.stderr.strip()[-2000:]}")
    return r.returncode, rep


def read_summary(gen: Generator) -> dict[str, Any] | None:
    """Итог генератора: out/summary.json, а если файла нет — событие summary из его лога."""
    path = OUT / "summary.json"
    if path.exists():
        data = json.loads(path.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else None
    return logged_summary(gen.events)


# --- прогон и отчёт -----------------------------------------------------------------------------------------


def run_stack(args: argparse.Namespace, facts: Facts, elapsed: dict[str, float | None]) -> None:
    """Шаги 1–3 на поднятом стенде: факты — в facts, время шагов — в elapsed.

    Упавший шаг печатается и не прерывает отчёт: проверки покажут, чего не хватило.
    """
    env = os.environ
    bootstrap = env.get("VQ_KAFKA") or f"127.0.0.1:{env.get('VQ_KAFKA_PORT', '19092')}"
    dsn = env.get("VQ_PG_DSN") or f"postgresql://vq:vq@127.0.0.1:{env.get('VQ_PG_PORT', '5432')}/vq"
    site = load_config(ROOT / "config" / "site.toml").site_id
    run_s = args.duration / args.speed
    poller = PgPoller(dsn, site)
    say(
        f"make {mode_of(args)}: проект {env.get('COMPOSE_PROJECT_NAME')}, Kafka {bootstrap}; генератор x{args.speed}, "
        f"{args.duration} с симуляции (≈ {run_s:.0f} с), seed {args.seed}"
    )
    if args.chaos:
        arm, kill = crash_schedule(args.seed, run_s)
        say(f"аварии dispatcher: точка аварии на {arm:.0f} с, kill -KILL -1 на {kill:.0f} с от старта генератора")
    try:
        box = Container("dispatcher")
        facts.restarts_before = box.inspect()["RestartCount"]
        (OUT / "summary.json").unlink(missing_ok=True)
        poller.start()
        gen_cmd = ["docker", "compose", "run", "--rm", "--use-aliases", "-T", "generator", "vq-generator"]
        gen_cmd += ["--speed", str(args.speed), "--duration", str(args.duration), "--seed", str(args.seed)]
        say("$ " + " ".join(gen_cmd))
        gen = Generator(gen_cmd)
        if args.chaos:
            facts.crashes = run_chaos(gen, box, poller, args.seed, args.speed, run_s)
        facts.gen_rc = gen.wait(run_s + GENERATOR_GRACE_S)
        elapsed["generator"] = round(time.monotonic() - gen.launched, 1)
        poller.stop()
        facts.summary = read_summary(gen)
        facts.hwm = high_watermark(bootstrap)
        say(f"генератор: код {facts.gen_rc} за {elapsed['generator']} с; telemetry.v1 high watermark {facts.hwm}")
        facts.processor, facts.waited_s = wait_caught_up(dsn, site, facts.hwm)
        elapsed["catch_up"] = None if facts.waited_s is None else round(facts.waited_s, 1)
        say("$ docker compose run --rm --no-deps -T dispatcher vq-verify")
        t_verify = time.monotonic()
        facts.verify_rc, facts.report = run_verify()
        elapsed["verify"] = round(time.monotonic() - t_verify, 1)
        facts.restarts_after = box.inspect()["RestartCount"]
    except Exception:  # шаг упал: проверки покажут, чего не хватило
        say("ОШИБКА прогона:\n" + traceback.format_exc())
    finally:
        poller.stop()


def print_checks(mode: str, facts: Facts, checks: Sequence[Check], elapsed: Mapping[str, float | None]) -> None:
    """Шаг 4: проверки PASS/FAIL, таблица восстановления (--chaos) и главные числа прогона."""
    say("")
    say(f"== Проверки make {mode} ==")
    for c in checks:
        say(f"{'PASS' if c.ok else 'FAIL'}  {c.name}: {c.detail}")
    if facts.chaos:
        say("")
        say("== Восстановление dispatcher: секунды от выхода процесса (State.FinishedAt), часы ВМ Docker ==")
        for line in recovery_table(facts.crashes):
            say(line)
    s, off = facts.summary or {}, dig(facts.report, "offline") or {}
    say("")
    say(
        f"генератор: отправлено {s.get('produced')}, сбои {json.dumps(s.get('faults'))}, ожидаемых отбросов "
        f"{s.get('expected_drops')}, решений получено {dig(s, 'decisions', 'received')}, "
        f"применено {dig(s, 'decisions', 'applied')}"
    )
    say(
        f"dispatcher: отброшено {dig(facts.processor, 'msgs_not_newer')}, принято "
        f"{dig(facts.processor, 'msgs_accepted')}, невалидных {dig(facts.processor, 'msgs_invalid')}, "
        f"epoch {dig(facts.processor, 'epoch')}; решения ядра: {json.dumps(off.get('decisions'))}"
    )
    say(f"повторов в логе: {_topics(dig(facts.report, 'log', 'replays'))}")
    say(
        "время, с: генератор {generator}, догон {catch_up}, verify {verify}, всего {total}".format(
            **{k: elapsed.get(k) for k in ("generator", "catch_up", "verify", "total")}
        )
    )


def report(mode: str, facts: Facts, checks: Sequence[Check], elapsed: Mapping[str, float | None]) -> dict[str, Any]:
    """Итог прогона в JSON: проверки, восстановление, числа генератора, processor и verify."""
    rep = facts.report or {}
    return {
        "ok": all(c.ok for c in checks),
        "mode": mode,
        "checks": [asdict(c) for c in checks],
        "recovery": [{**asdict(c), **recovery(c)} for c in facts.crashes],
        "summary": facts.summary,
        "telemetry_high_watermark": facts.hwm,
        "processor": facts.processor,
        "verify": {k: rep.get(k) for k in ("ok", "n", "telemetry", "log", "mismatches", "counters", "offline")},
        "elapsed_s": elapsed,
    }


def mode_of(args: argparse.Namespace) -> str:
    return "e2e-chaos" if args.chaos else "e2e"


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="tools/e2e.py", description="Сквозной прогон стенда (§6.3)")
    p.add_argument("--chaos", action="store_true", help="две аварии dispatcher во время прогона")
    p.add_argument("--speed", type=int, default=SPEED)
    p.add_argument("--duration", type=int, default=DURATION, help="секунд симуляции")
    p.add_argument("--seed", type=int, default=SEED, help="seed генератора и момента kill")
    args = p.parse_args(argv)
    mode = mode_of(args)
    facts = Facts(chaos=args.chaos)
    elapsed: dict[str, float | None] = {}
    t_start = time.monotonic()
    run_stack(args, facts, elapsed)
    elapsed["total"] = round(time.monotonic() - t_start, 1)

    checks = evaluate(facts)
    print_checks(mode, facts, checks, elapsed)
    result = report(mode, facts, checks, elapsed)
    failed = [c.key for c in checks if not c.ok]
    say(f"ИТОГ make {mode}: " + ("PASS" if not failed else f"FAIL ({', '.join(failed)})"))
    OUT.mkdir(exist_ok=True)
    text = json.dumps(result, ensure_ascii=False, default=str)
    (OUT / f"{mode}.json").write_text(text + "\n", encoding="utf-8")
    say(text)
    return 0 if not failed else 1


if __name__ == "__main__":
    sys.exit(main())
