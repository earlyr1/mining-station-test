"""vq-verify (§6.3): queue.v1 и decision.v1 против офлайн-прогона ядра по telemetry.v1[0, N).

N = processor.next_offset. Ядро прогоняется с пустого состояния, каждый выход помечается offset своего сообщения.
Из выходных топиков берутся записи с vq-src-offset < N. Записи с одинаковыми (топик, ключ, vq-src-offset) —
повторы после падения: обязаны совпадать байт в байт, считаются и схлопываются. Затем по каждому ключу
последовательность в порядке vq-src-offset обязана совпасть с офлайн-последовательностью.
Дополнительно: точные счётчики processor равны офлайн-счётчикам; инварианты офлайн-прогона после каждого сообщения.

Вывод: строки сводки для человека, затем один JSON последней строкой stdout (его разбирает make e2e).
Код выхода 0 — всё сошлось, 1 — нет. Запускается в образе приложения: `docker compose run --rm dispatcher vq-verify`.
"""

import argparse
import json
import os
import time
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any, Protocol

import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException, TopicPartition

from vq.config import SiteConfig, config_hash, load_config
from vq.core.codec import parse_src_offset
from vq.core.decision import DECISION_TOPIC
from vq.core.engine import LOGIC_VERSION, QUEUE_TOPIC, Engine
from vq.core.model import SERVICE, Event, SiteState, StepResult
from vq.dispatcher.main import DEFAULT_CONFIG, DEFAULT_KAFKA, DEFAULT_PG_DSN, TELEMETRY_TOPIC

OUTPUT_TOPICS = (QUEUE_TOPIC, DECISION_TOPIC)
COUNTERS = ("accepted", "not_newer", "invalid")
DECISION_KINDS = ("recommended", "no_gain", "stale_telemetry")
KEEP = 10  # сколько расхождений и нарушений показывать подробно; считаются все
EXCERPT = 60  # байт контекста по обе стороны от первого различия
READ_BATCH = 10_000
READ_TIMEOUT_S = 30.0  # столько секунд без новых записей — ошибка чтения
WATERMARK_TIMEOUT_S = 10.0


@dataclass(frozen=True)
class Record:
    """Запись топика или офлайн-выход: src — vq-src-offset (None — нет или не число), pos — offset записи."""

    topic: str
    key: bytes | None
    src: int | None
    value: bytes | None
    pos: int | None = None


@dataclass(frozen=True)
class ProcessorRow:
    """Строка processor (§2.5): offset, epoch, версии и точные счётчики из последнего чекпоинта."""

    epoch: int
    next_offset: int | None
    logic_version: str
    config_hash: str
    accepted: int
    not_newer: int
    invalid: int

    @property
    def counters(self) -> dict[str, int]:
        return {"accepted": self.accepted, "not_newer": self.not_newer, "invalid": self.invalid}


# --- офлайн-прогон ---------------------------------------------------------------------------


class EngineLike(Protocol):
    state: SiteState

    def apply(self, raw: bytes | None) -> StepResult: ...


@dataclass
class Offline:
    """Офлайн-прогон ядра: выходы с src, счётчики сообщений, события и нарушения инвариантов."""

    outputs: list[Record] = field(default_factory=list)
    messages: int = 0
    counters: dict[str, int] = field(default_factory=lambda: dict.fromkeys(COUNTERS, 0))
    decisions: dict[str, int] = field(default_factory=lambda: dict.fromkeys(DECISION_KINDS, 0))
    fsm_resync: int = 0
    occupations: dict[str, int] = field(default_factory=dict)  # машина → число занятий станции
    prediction_errors: int = 0
    releases: dict[str, int] = field(default_factory=lambda: {"timer": 0, "exit": 0})
    violations: list[dict[str, Any]] = field(default_factory=list)  # первые KEEP
    violation_count: int = 0

    def observe(self, events: Iterable[Event]) -> bool:
        """Учесть события шага ядра; True — сообщение принято (accepted), то есть могло изменить состояние."""
        accepted = False
        for ev in events:
            kind = ev.kind
            if kind in self.counters:
                self.counters[kind] += 1
                accepted = accepted or kind == "accepted"
            elif kind == "decision":
                key = "recommended" if ev.get("result") == "recommended" else str(ev.get("reason"))
                self.decisions[key] = self.decisions.get(key, 0) + 1
            elif kind == "occupied":
                unit = str(ev.get("unit"))
                self.occupations[unit] = self.occupations.get(unit, 0) + 1
            elif kind == "released":
                reason = str(ev.get("reason"))
                self.releases[reason] = self.releases.get(reason, 0) + 1
            elif kind == "fsm_resync":
                self.fsm_resync += 1
            elif kind == "prediction_error":
                self.prediction_errors += 1
        return accepted


def check_occupants(state: SiteState) -> list[dict[str, Any]]:
    """Занявшая станцию машина — в фазе service, и её цель — эта же станция (§3.6)."""
    out: list[dict[str, Any]] = []
    for uuid in sorted(state.stations):
        unit = state.stations[uuid].occupant_uuid
        if unit is None:
            continue
        v = state.vehicles.get(unit)
        if v is None or v.phase != SERVICE or v.target_station != uuid:
            phase, target = (None, None) if v is None else (v.phase, v.target_station)
            out.append({"kind": "occupant", "station": uuid, "unit": unit, "phase": phase, "target": target})
    return out


def check_rows(latest: Mapping[str, Sequence[str]]) -> list[dict[str, Any]]:
    """Машина — не больше чем в одной строке последних опубликованных очередей всех станций (§3.7)."""
    where: dict[str, list[str]] = {}
    for station in sorted(latest):
        for unit in latest[station]:
            where.setdefault(unit, []).append(station)
    return [{"kind": "queue_rows", "unit": u, "stations": s} for u, s in sorted(where.items()) if len(s) > 1]


def _queue_units(value: bytes) -> tuple[str, ...]:
    return tuple(row["unit_uuid"] for row in json.loads(value)["queue"])


def run_offline(
    cfg: SiteConfig,
    telemetry: Iterable[tuple[int, bytes | None]],
    engine: EngineLike | None = None,
    keep: int = KEEP,
) -> Offline:
    """Прогон ядра с пустого состояния по (offset, значение) в порядке лога; инварианты — после каждого сообщения."""
    eng: EngineLike = engine if engine is not None else Engine(cfg)
    off = Offline(occupations=dict.fromkeys(cfg.assignment, 0))
    latest: dict[str, tuple[str, ...]] = {}  # станция → машины последней опубликованной очереди
    for offset, value in telemetry:
        step = eng.apply(value)
        off.messages += 1
        queues_changed = False
        for o in step.outputs:
            off.outputs.append(Record(o.topic, o.key, offset, o.value))
            if o.topic == QUEUE_TOPIC:
                latest[o.key.decode()] = _queue_units(o.value)
                queues_changed = True
        accepted = off.observe(step.events)
        # невалидные и не новые сообщения состояние не меняют; очереди меняются только публикацией
        found = (check_occupants(eng.state) if accepted else []) + (check_rows(latest) if queues_changed else [])
        for v in found:
            off.violation_count += 1
            if len(off.violations) < keep:
                off.violations.append({"offset": offset, **v})
    return off


# --- сравнение лога с офлайн-прогоном ----------------------------------------------------------


@dataclass
class Comparison:
    """Итог сверки: записи по топикам, повторы, сверенные ключи, расхождения (все — по видам, первые — подробно)."""

    keep: int = KEEP
    read: Counter[str] = field(default_factory=Counter)
    kept: Counter[str] = field(default_factory=Counter)  # vq-src-offset < N, вместе с повторами
    after_n: Counter[str] = field(default_factory=Counter)  # vq-src-offset ≥ N: не сверяются
    no_src: Counter[str] = field(default_factory=Counter)
    replays: Counter[str] = field(default_factory=Counter)
    keys: Counter[str] = field(default_factory=Counter)
    by_kind: Counter[str] = field(default_factory=Counter)
    first: list[dict[str, Any]] = field(default_factory=list)

    @property
    def total(self) -> int:
        return sum(self.by_kind.values())

    @property
    def ok(self) -> bool:
        return self.total == 0

    def add(self, m: dict[str, Any]) -> None:
        """Расхождение: считается всегда, подробно сохраняются первые keep."""
        self.by_kind[m["kind"]] += 1
        if len(self.first) < self.keep:
            self.first.append(m)


def _diff_at(a: bytes, b: bytes) -> int:
    n = min(len(a), len(b))
    return next((i for i in range(n) if a[i] != b[i]), n)


def _excerpt(value: bytes | None, at: int = 0) -> str | None:
    """Кусок значения вокруг байта at: так видно первое различие, а не килобайт очереди."""
    if value is None:
        return None
    start, end = max(0, at - EXCERPT), at + EXCERPT
    text = value[start:end].decode("utf-8", "replace")
    return ("…" if start > 0 else "") + text + ("…" if end < len(value) else "")


def _key(key: bytes | None) -> str | None:
    return None if key is None else key.decode("utf-8", "replace")


def _differs(
    kind: str, topic: str, key: bytes | None, src: int | None, want: bytes | None, got: bytes | None
) -> dict[str, Any]:
    at = _diff_at(want or b"", got or b"")
    return {
        "kind": kind,
        "topic": topic,
        "key": _key(key),
        "src": src,
        "diff_at": at,
        "expected": _excerpt(want, at),
        "got": _excerpt(got, at),
    }


ByKey = dict[tuple[str, bytes | None], dict[int, Record]]  # (топик, ключ) → src → запись


def compare(offline: Iterable[Record], log: Iterable[Record], n: int, keep: int = KEEP) -> Comparison:
    """Сверка записей лога (в порядке топика) с офлайн-выходами (в порядке прогона) для vq-src-offset < n.

    Порядок расхождений в отчёте: проход по логу, дубли офлайн-прогона, затем по ключам с самого раннего src.
    """
    res = Comparison(keep=keep)
    logged = _collapse_replays(log, n, res)
    _diff(_by_key(offline, res), logged, res)
    return res


def _collapse_replays(log: Iterable[Record], n: int, res: Comparison) -> ByKey:
    """Записи лога с vq-src-offset < n по ключам; повторы (тот же топик, ключ и src) схлопнуты и посчитаны.

    Повтор обязан совпасть с первой записью байт в байт (replay_differs). Новая запись с src меньше уже виденного
    по ключу — reordered: потребитель (§2.6) отбросил бы её как повтор.
    """
    seen: dict[tuple[str, bytes | None, int], Record] = {}
    last_src: dict[tuple[str, bytes | None], int] = {}
    for r in log:
        res.read[r.topic] += 1
        if r.src is None:
            res.no_src[r.topic] += 1
            res.add({"kind": "no_src_offset", "topic": r.topic, "key": _key(r.key), "src": None, "pos": r.pos})
            continue
        if r.src >= n:
            res.after_n[r.topic] += 1
            continue
        res.kept[r.topic] += 1
        first = seen.get((r.topic, r.key, r.src))
        if first is not None:  # повтор после падения: тот же шаг ядра ещё раз
            res.replays[r.topic] += 1
            if r.value != first.value:
                m = _differs("replay_differs", r.topic, r.key, r.src, first.value, r.value)
                res.add({**m, "pos": r.pos, "first_pos": first.pos})
            continue
        seen[(r.topic, r.key, r.src)] = r
        prev = last_src.get((r.topic, r.key))
        if prev is not None and r.src < prev:
            m = {"kind": "reordered", "topic": r.topic, "key": _key(r.key), "src": r.src, "after_src": prev}
            res.add({**m, "pos": r.pos})
        else:
            last_src[(r.topic, r.key)] = r.src
    logged: ByKey = {}
    for (topic, key, src), r in seen.items():
        logged.setdefault((topic, key), {})[src] = r
    return logged


def _by_key(offline: Iterable[Record], res: Comparison) -> ByKey:
    """Офлайн-выходы по ключам. §2.3: шаг публикует не больше одной записи на ключ — иначе повтор неотличим."""
    expected: ByKey = {}
    for o in offline:
        assert o.src is not None
        by_src = expected.setdefault((o.topic, o.key), {})
        if o.src in by_src:
            res.add({"kind": "offline_duplicate", "topic": o.topic, "key": _key(o.key), "src": o.src})
            continue
        by_src[o.src] = o
    return expected


def _diff(expected: ByKey, logged: ByKey, res: Comparison) -> None:
    """По каждому ключу последовательность лога против офлайн-последовательности: missing, extra, value_differs."""
    found: list[tuple[int, str, bytes, str, Record | None, Record | None]] = []
    for tk in set(expected) | set(logged):
        res.keys[tk[0]] += 1
        want, got = expected.get(tk, {}), logged.get(tk, {})
        for src in set(want) | set(got):
            w, g = want.get(src), got.get(src)
            kind = "missing" if g is None else "extra" if w is None else "value_differs" if w.value != g.value else ""
            if kind:
                found.append((src, tk[0], tk[1] or b"", kind, w, g))
    for src, topic, key, kind, w, g in sorted(found, key=lambda x: x[:3]):  # сначала самое раннее расхождение
        if kind == "missing":
            expected_value = None if w is None else w.value
            res.add({"kind": kind, "topic": topic, "key": _key(key), "src": src, "expected": _excerpt(expected_value)})
        elif kind == "extra":
            assert g is not None
            res.add(
                {"kind": kind, "topic": topic, "key": _key(key), "src": src, "pos": g.pos, "got": _excerpt(g.value)}
            )
        else:
            assert w is not None and g is not None
            res.add({**_differs(kind, topic, key, src, w.value, g.value), "pos": g.pos})


def compare_counters(processor: Mapping[str, int], offline: Mapping[str, int]) -> list[dict[str, Any]]:
    """Точные счётчики из чекпоинта против офлайн-прогона: должны совпасть все три."""
    return [
        {"counter": c, "processor": processor[c], "offline": offline[c]} for c in COUNTERS if processor[c] != offline[c]
    ]


# --- сверка целиком ------------------------------------------------------------------------------


class Sources(Protocol):
    """Откуда verify читает: строка processor, водяные знаки и записи партиции 0 топика."""

    def processor(self, site_id: str) -> ProcessorRow | None: ...

    def watermarks(self, topic: str) -> tuple[int, int]: ...

    def read(self, topic: str, start: int, end: int) -> Iterator[Record]: ...

    def close(self) -> None: ...


def _per_topic(c: Mapping[str, int]) -> dict[str, int]:
    return {t: c.get(t, 0) for t in OUTPUT_TOPICS}


def empty_report(site: str | None, errors: Sequence[str] = ()) -> dict[str, Any]:
    """Отчёт со всеми разделами; ok — только когда сверка прошла целиком."""
    zero = _per_topic({})
    return {
        "ok": False,
        "site": site,
        "n": None,
        "epoch": None,
        "errors": list(errors),
        "versions": None,
        "telemetry": {"low": None, "high": None, "read": 0},
        "offline": None,
        "log": {
            "read": zero,
            "kept": zero,
            "after_n": zero,
            "no_src_offset": zero,
            "replays": zero,
            "keys_compared": zero,
        },
        "mismatches": {"total": 0, "by_kind": {}, "first": []},
        "counters": {"ok": None, "processor": None, "offline": None, "diff": []},
        "invariants": {"ok": None, "violations": 0, "first": []},
        "elapsed_s": {},
    }


def _offline_section(off: Offline) -> dict[str, Any]:
    per_unit = list(off.occupations.values()) or [0]
    return {
        "messages": off.messages,
        "outputs": _per_topic(Counter(o.topic for o in off.outputs)),
        "counters": off.counters,
        "fsm_resync": off.fsm_resync,
        "decisions": off.decisions,
        "occupations": {
            "total": sum(per_unit),
            "per_unit_min": min(per_unit),
            "per_unit_max": max(per_unit),
            "units_without": sorted(u for u, k in off.occupations.items() if k == 0),
        },
        "prediction_errors": off.prediction_errors,
        "releases": off.releases,
    }


def _telemetry(records: Iterable[Record]) -> Iterator[tuple[int, bytes | None]]:
    for r in records:
        if r.pos is None:
            raise ValueError(f"{TELEMETRY_TOPIC}: запись без offset")
        yield r.pos, r.value


def verify(cfg: SiteConfig, chash: str, sources: Sources, keep: int = KEEP) -> dict[str, Any]:
    """Вся сверка §6.3 над источниками; возвращает отчёт (см. empty_report), ok — итог."""
    t0 = time.perf_counter()
    rep = empty_report(cfg.site_id)
    errors: list[str] = rep["errors"]
    row = sources.processor(cfg.site_id)
    if row is None:
        errors.append(f"в таблице processor нет строки площадки {cfg.site_id}: dispatcher ещё не забирал партицию")
        return rep
    n = row.next_offset or 0
    rep.update(n=n, epoch=row.epoch)
    rep["counters"]["processor"] = row.counters
    versions_ok = (row.logic_version, row.config_hash) == (LOGIC_VERSION, chash)
    rep["versions"] = {
        "ok": versions_ok,
        "logic_version": {"processor": row.logic_version, "code": LOGIC_VERSION},
        "config_hash": {"processor": row.config_hash, "code": chash},
    }
    if row.logic_version != LOGIC_VERSION:
        errors.append(
            f"logic_version в processor {row.logic_version} ≠ {LOGIC_VERSION} в коде: перепрогон не той логикой"
        )
    if row.config_hash != chash:
        errors.append(f"config_hash в processor {row.config_hash[:12]}… ≠ {chash[:12]}… VQ_CONFIG: другая площадка")
    low, high = sources.watermarks(TELEMETRY_TOPIC)
    rep["telemetry"].update(low=low, high=high)
    if n > 0 and low > 0:
        errors.append(
            f"{TELEMETRY_TOPIC} начинается с offset {low} (low > 0): перепрогон с пустого состояния невозможен"
        )
    if n > high:
        errors.append(f"processor.next_offset {n} больше high watermark {high} {TELEMETRY_TOPIC}: лог создан заново")
    if errors:
        return rep

    off = run_offline(cfg, _telemetry(sources.read(TELEMETRY_TOPIC, 0, n)), keep=keep)
    t1 = time.perf_counter()
    rep["telemetry"]["read"] = off.messages
    rep["offline"] = _offline_section(off)
    rep["counters"]["offline"] = off.counters
    if off.messages != n:
        errors.append(f"{TELEMETRY_TOPIC}: прочитано {off.messages} из {n} сообщений [0, N)")
        return rep

    def log() -> Iterator[Record]:
        for topic in OUTPUT_TOPICS:
            lo, hi = sources.watermarks(topic)
            yield from sources.read(topic, lo, hi)

    cmp = compare(off.outputs, log(), n, keep)
    t2 = time.perf_counter()
    diff = compare_counters(row.counters, off.counters)
    rep["log"] = {
        "read": _per_topic(cmp.read),
        "kept": _per_topic(cmp.kept),
        "after_n": _per_topic(cmp.after_n),
        "no_src_offset": _per_topic(cmp.no_src),
        "replays": _per_topic(cmp.replays),
        "keys_compared": _per_topic(cmp.keys),
    }
    rep["mismatches"] = {"total": cmp.total, "by_kind": dict(sorted(cmp.by_kind.items())), "first": cmp.first}
    rep["counters"].update(ok=not diff, diff=diff)
    rep["invariants"] = {"ok": off.violation_count == 0, "violations": off.violation_count, "first": off.violations}
    rep["elapsed_s"] = {
        "telemetry_offline": round(t1 - t0, 2),
        "outputs": round(t2 - t1, 2),
        "total": round(t2 - t0, 2),
    }
    rep["ok"] = cmp.ok and not diff and off.violation_count == 0
    return rep


# --- вывод -----------------------------------------------------------------------------------------


def _topics(c: Mapping[str, int]) -> str:
    return ", ".join(f"{t} {c.get(t, 0)}" for t in OUTPUT_TOPICS)


def summary_lines(rep: Mapping[str, Any]) -> list[str]:
    """Сводка для человека; всё, что в ней есть, есть и в JSON."""
    lines = [f"vq-verify: площадка {rep['site']}, N = processor.next_offset = {rep['n']}, epoch {rep['epoch']}"]
    lines += [f"ОШИБКА: {e}" for e in rep["errors"]]
    tele, off, log = rep["telemetry"], rep["offline"], rep["log"]
    if off is not None:
        el = rep["elapsed_s"]
        took = f", {el['telemetry_offline']} с" if "telemetry_offline" in el else ""
        lines.append(f"{TELEMETRY_TOPIC}[0, N): прочитано {tele['read']}; офлайн-прогон ядра с пустого состояния{took}")
        lines.append(f"офлайн-выходы: {_topics(off['outputs'])}")
    c = rep["counters"]
    if c["ok"] is not None:  # сверка дошла до выходных топиков
        for t in OUTPUT_TOPICS:
            lines.append(
                f"лог {t}: прочитано {log['read'][t]}, до N {log['kept'][t]}, после N {log['after_n'][t]}, "
                f"повторов {log['replays'][t]}, без vq-src-offset {log['no_src_offset'][t]}"
            )
        lines.append(f"ключей сверено: {_topics(log['keys_compared'])}; расхождений {rep['mismatches']['total']}")
    for m in rep["mismatches"]["first"]:
        extra = json.dumps({k: v for k, v in m.items() if k not in ("kind", "topic", "key", "src")}, ensure_ascii=False)
        lines.append(f"  расхождение {m['kind']}: {m['topic']} ключ {m['key']} src {m['src']} {extra}")
    if c["processor"] is not None and c["offline"] is not None:
        p, o = c["processor"], c["offline"]
        pairs = ", ".join(f"{k} {p[k]} {'=' if p[k] == o[k] else '≠'} {o[k]}" for k in COUNTERS)
        lines.append(f"счётчики processor / офлайн: {pairs}")
    inv = rep["invariants"]
    if inv["ok"] is not None:
        lines.append(f"инварианты офлайн-прогона: нарушений {inv['violations']}")
        lines += [f"  нарушение {json.dumps(v, ensure_ascii=False)}" for v in inv["first"]]
    if off is not None:
        d, occ = off["decisions"], off["occupations"]
        lines.append(
            f"события офлайн: самолечение {off['fsm_resync']}; решения "
            + ", ".join(f"{k} {d.get(k, 0)}" for k in DECISION_KINDS)
            + f"; занятий {occ['total']}, на машину {occ['per_unit_min']}..{occ['per_unit_max']}"
            + f", без занятий: {', '.join(occ['units_without']) or 'нет'}; ошибок прогноза {off['prediction_errors']}"
        )
    lines.append("ИТОГ: " + ("OK" if rep["ok"] else "РАСХОЖДЕНИЕ"))
    return lines


def print_report(rep: Mapping[str, Any]) -> None:
    for line in summary_lines(rep):
        print(line)
    print(json.dumps(rep, ensure_ascii=False), flush=True)


# --- Kafka и Postgres ------------------------------------------------------------------------------

_SELECT_PROCESSOR = """
SELECT epoch, next_offset, logic_version, config_hash, msgs_accepted, msgs_not_newer, msgs_invalid
  FROM processor WHERE site_id = %s
"""


class KafkaPgSources:
    """Postgres — только чтение строки processor; Kafka — assign партиции 0 с явного offset, без группы и коммитов."""

    def __init__(self, bootstrap: str, dsn: str) -> None:
        self._bootstrap = bootstrap
        self._dsn = dsn
        self._meta = Consumer(self._conf())

    def _conf(self) -> dict[str, Any]:
        return {
            "bootstrap.servers": self._bootstrap,
            "group.id": "vq-verify",  # Consumer требует; assign без subscribe в группу не вступает
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "enable.partition.eof": True,
            "auto.offset.reset": "error",
            "client.id": "vq-verify",
        }

    def processor(self, site_id: str) -> ProcessorRow | None:
        with psycopg.connect(self._dsn, autocommit=True, connect_timeout=5) as conn:
            try:
                row = conn.execute(_SELECT_PROCESSOR, (site_id,)).fetchone()
            except psycopg.errors.UndefinedTable:  # dispatcher ещё не применял схему
                return None
        return None if row is None else ProcessorRow(*row)

    def watermarks(self, topic: str) -> tuple[int, int]:
        low, high = self._meta.get_watermark_offsets(
            TopicPartition(topic, 0), timeout=WATERMARK_TIMEOUT_S, cached=False
        )
        return int(low), int(high)

    def read(self, topic: str, start: int, end: int) -> Iterator[Record]:
        """Записи партиции 0 с offset в [start, end), по порядку."""
        if end <= start:
            return
        c = Consumer(self._conf())
        try:
            c.assign([TopicPartition(topic, 0, start)])
            deadline = time.monotonic() + READ_TIMEOUT_S
            while True:
                msgs = c.consume(num_messages=READ_BATCH, timeout=1.0)
                if not msgs:
                    if time.monotonic() > deadline:
                        raise TimeoutError(f"{topic}: нет записей {READ_TIMEOUT_S:.0f} с, ждали до offset {end - 1}")
                    continue
                deadline = time.monotonic() + READ_TIMEOUT_S
                for m in msgs:
                    err, pos = m.error(), m.offset()
                    if err is not None:
                        if err.code() != KafkaError._PARTITION_EOF:
                            raise KafkaException(err)
                        if pos is not None and pos >= end:  # конец партиции: компакция могла убрать хвост
                            return
                        continue
                    if pos is None:
                        continue
                    if pos >= end:
                        return
                    yield Record(topic, m.key(), parse_src_offset(m.headers()), m.value(), pos)
                    if pos >= end - 1:
                        return
        finally:
            c.close()

    def close(self) -> None:
        self._meta.close()


def open_sources(env: Mapping[str, str]) -> Sources:
    return KafkaPgSources(env.get("VQ_KAFKA", DEFAULT_KAFKA), env.get("VQ_PG_DSN", DEFAULT_PG_DSN))


def main(argv: Sequence[str] | None = None) -> int:
    argparse.ArgumentParser(
        prog="vq-verify", description="Сверка queue.v1 и decision.v1 с офлайн-прогоном ядра (§6.3)"
    ).parse_args(argv)
    env = os.environ
    cfg_path = env.get("VQ_CONFIG", DEFAULT_CONFIG)
    site: str | None = None
    try:
        cfg = load_config(cfg_path)
        site = cfg.site_id
        chash = config_hash(cfg_path)
        sources = open_sources(env)
        try:
            rep = verify(cfg, chash, sources)
        finally:
            sources.close()
    except Exception as e:  # нет связи с Kafka или Postgres, битый конфиг: всё равно один JSON последней строкой
        rep = empty_report(site, [f"{type(e).__name__}: {e}"])
    print_report(rep)
    return 0 if rep["ok"] else 1
