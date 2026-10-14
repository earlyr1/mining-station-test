"""vq-verify (§6.3): сверка выходных топиков с офлайн-прогоном ядра — чистые функции на синтетических записях.

Kafka и Postgres здесь подменены словарями (FakeSources); живой прогон — make e2e / make verify.
"""

import io
import json
from collections.abc import Iterator
from contextlib import redirect_stdout
from dataclasses import replace

import pytest

from tests.helpers import NOON, at, msg, one_station
from vq.core.decision import DECISION_TOPIC
from vq.core.engine import LOGIC_VERSION, QUEUE_TOPIC
from vq.core.model import SERVICE, WAITING, SiteState, Station, StepResult, Vehicle, event
from vq.core.model import Output as CoreOutput
from vq.tools.verify import (
    TELEMETRY_TOPIC,
    ProcessorRow,
    Record,
    check_occupants,
    check_rows,
    compare,
    compare_counters,
    main,
    print_report,
    run_offline,
    verify,
)

Q, D = QUEUE_TOPIC, DECISION_TOPIC


def rec(topic: str, key: str, src: int | None, value: str, pos: int | None = None) -> Record:
    return Record(topic, key.encode(), src, value.encode(), pos)


def kinds(cmp_result) -> list[tuple[str, str, int | None]]:
    return [(m["kind"], m["key"], m["src"]) for m in cmp_result.first]


# --- сравнение лога с офлайн-прогоном -------------------------------------------------------

OFFLINE = [rec(Q, "S1", 0, "a"), rec(D, "T1", 2, "d"), rec(Q, "S1", 2, "b"), rec(Q, "S2", 3, "c")]


def test_log_equal_to_offline_is_ok() -> None:
    log = [rec(Q, "S1", 0, "a", 0), rec(Q, "S1", 2, "b", 1), rec(Q, "S2", 3, "c", 2), rec(D, "T1", 2, "d", 0)]
    r = compare(OFFLINE, log, n=4)
    assert r.ok and r.total == 0
    assert r.kept == {Q: 3, D: 1}
    assert r.keys == {Q: 2, D: 1}
    assert r.replays == {}


def test_identical_replay_is_collapsed_and_counted() -> None:
    """Повтор хвоста после падения: те же записи ещё раз, байт в байт — не расхождение, а повтор."""
    log = [
        rec(Q, "S1", 0, "a", 0),
        rec(Q, "S1", 2, "b", 1),
        rec(Q, "S1", 0, "a", 2),  # повтор
        rec(Q, "S1", 2, "b", 3),  # повтор
        rec(Q, "S2", 3, "c", 4),
        rec(D, "T1", 2, "d", 0),
        rec(D, "T1", 2, "d", 1),  # повтор решения
    ]
    r = compare(OFFLINE, log, n=4)
    assert r.ok, r.first
    assert r.replays == {Q: 2, D: 1}
    assert r.kept == {Q: 5, D: 2}


def test_replay_with_different_bytes_fails() -> None:
    log = [rec(Q, "S1", 0, "a", 0), rec(Q, "S1", 2, "b", 1), rec(Q, "S1", 2, "B", 2), rec(Q, "S2", 3, "c", 3)]
    log.append(rec(D, "T1", 2, "d", 0))
    r = compare(OFFLINE, log, n=4)
    assert not r.ok
    assert kinds(r) == [("replay_differs", "S1", 2)]
    assert r.first[0]["pos"] == 2 and r.first[0]["first_pos"] == 1
    assert r.replays == {Q: 1}


def test_missing_record_fails() -> None:
    log = [rec(Q, "S1", 0, "a", 0), rec(Q, "S2", 3, "c", 1), rec(D, "T1", 2, "d", 0)]
    r = compare(OFFLINE, log, n=4)
    assert kinds(r) == [("missing", "S1", 2)]
    assert r.first[0]["expected"] == "b"


def test_extra_record_fails() -> None:
    log = [rec(Q, "S1", 0, "a", 0), rec(Q, "S1", 1, "x", 1), rec(Q, "S1", 2, "b", 2), rec(Q, "S2", 3, "c", 3)]
    log.append(rec(D, "T1", 2, "d", 0))
    r = compare(OFFLINE, log, n=4)
    assert kinds(r) == [("extra", "S1", 1)]
    assert r.first[0]["got"] == "x" and r.first[0]["pos"] == 1


def test_different_value_at_same_src_fails_with_position_of_first_difference() -> None:
    offline = [rec(Q, "S1", 0, '{"queue":[1,2,3]}')]
    log = [rec(Q, "S1", 0, '{"queue":[1,9,3]}', 5)]
    r = compare(offline, log, n=1)
    assert kinds(r) == [("value_differs", "S1", 0)]
    m = r.first[0]
    assert m["diff_at"] == 12 and m["pos"] == 5
    assert "1,2,3" in m["expected"] and "1,9,3" in m["got"]


def test_values_swapped_between_offsets_fail() -> None:
    """Те же байты, но не у тех vq-src-offset: последовательность по ключу не совпадает."""
    offline = [rec(Q, "S1", 0, "a"), rec(Q, "S1", 2, "b")]
    log = [rec(Q, "S1", 0, "b", 0), rec(Q, "S1", 2, "a", 1)]
    r = compare(offline, log, n=3)
    assert kinds(r) == [("value_differs", "S1", 0), ("value_differs", "S1", 2)]


def test_reordered_first_occurrences_fail() -> None:
    """Первое появление записи с меньшим vq-src-offset после большего — не повтор: потребитель её бы отбросил."""
    offline = [rec(Q, "S1", 0, "a"), rec(Q, "S1", 2, "b")]
    log = [rec(Q, "S1", 2, "b", 0), rec(Q, "S1", 0, "a", 1)]
    r = compare(offline, log, n=3)
    assert kinds(r) == [("reordered", "S1", 0)]
    assert r.first[0]["after_src"] == 2 and r.first[0]["pos"] == 1


def test_order_across_keys_is_free() -> None:
    offline = [rec(Q, "S1", 0, "a"), rec(Q, "S2", 1, "c"), rec(Q, "S1", 2, "b")]
    log = [rec(Q, "S2", 1, "c", 0), rec(Q, "S1", 2, "b", 1), rec(Q, "S1", 0, "a", 2)]
    r = compare(offline, log, n=3)
    assert kinds(r) == [("reordered", "S1", 0)]  # внутри S1 порядок нарушен, между S1 и S2 — нет
    log = [rec(Q, "S2", 1, "c", 0), rec(Q, "S1", 0, "a", 1), rec(Q, "S1", 2, "b", 2)]
    assert compare(offline, log, n=3).ok


def test_same_key_in_different_topics_is_compared_separately() -> None:
    offline = [rec(Q, "X", 0, "q"), rec(D, "X", 0, "d")]
    assert compare(offline, [rec(D, "X", 0, "d", 0), rec(Q, "X", 0, "q", 0)], n=1).ok
    r = compare(offline, [rec(Q, "X", 0, "q", 0), rec(Q, "X", 0, "q", 1)], n=1)
    assert kinds(r) == [("missing", "X", 0)] and r.first[0]["topic"] == D


def test_records_at_or_after_n_are_ignored() -> None:
    offline = [rec(Q, "S1", 0, "a"), rec(Q, "S1", 2, "b")]
    log = [rec(Q, "S1", 0, "a", 0), rec(Q, "S1", 2, "b", 1), rec(Q, "S1", 3, "zz", 2), rec(Q, "S9", 9, "yy", 3)]
    r = compare(offline, log, n=3)
    assert r.ok
    assert r.after_n == {Q: 2}
    assert r.kept == {Q: 2}
    assert r.keys == {Q: 1}


def test_two_offline_outputs_with_same_key_and_src_fail() -> None:
    """§2.3: шаг публикует не больше одной записи на ключ; иначе повтор в логе не отличить от второй записи шага."""
    offline = [rec(Q, "S1", 0, "a"), rec(Q, "S1", 0, "b")]
    r = compare(offline, [rec(Q, "S1", 0, "a", 0), rec(Q, "S1", 0, "b", 1)], n=1)
    assert ("offline_duplicate", "S1", 0) in kinds(r)
    assert not r.ok


def test_record_without_src_offset_fails() -> None:
    offline = [rec(Q, "S1", 0, "a")]
    r = compare(offline, [rec(Q, "S1", 0, "a", 0), rec(Q, "S1", None, "a", 1)], n=1)
    assert kinds(r) == [("no_src_offset", "S1", None)]


def test_report_keeps_first_mismatches_and_counts_all() -> None:
    offline = [rec(Q, "S1", i, "v") for i in range(30)]
    r = compare(offline, [], n=30, keep=10)
    assert r.total == 30 and len(r.first) == 10
    assert r.by_kind == {"missing": 30}


def test_counters_must_match_exactly() -> None:
    same = {"accepted": 10, "not_newer": 2, "invalid": 1}
    assert compare_counters(same, dict(same)) == []
    diff = compare_counters(same, {"accepted": 10, "not_newer": 3, "invalid": 1})
    assert diff == [{"counter": "not_newer", "processor": 2, "offline": 3}]


# --- инварианты офлайн-прогона -----------------------------------------------------------------


def vehicle(unit: str, phase: str, target: str) -> Vehicle:
    return Vehicle(unit, target, NOON, 0.0, 0.0, 0.0, phase, target)


def test_occupant_must_serve_its_own_station() -> None:
    state = SiteState(
        now=NOON,
        vehicles={
            "T1": vehicle("T1", SERVICE, "S1"),
            "T2": vehicle("T2", WAITING, "S2"),
            "T3": vehicle("T3", SERVICE, "S1"),
        },
        stations={"S1": Station("S1", "T1", NOON), "S2": Station("S2", "T2", NOON), "S3": Station("S3", "T3", NOON)},
    )
    got = check_occupants(state)
    assert [(v["station"], v["unit"]) for v in got] == [("S2", "T2"), ("S3", "T3")]
    assert (
        check_occupants(SiteState(NOON, {"T1": vehicle("T1", SERVICE, "S1")}, {"S1": Station("S1", "T1", NOON)})) == []
    )


def test_occupant_unknown_vehicle_is_a_violation() -> None:
    got = check_occupants(SiteState(NOON, {}, {"S1": Station("S1", "T9", NOON)}))
    assert [(v["station"], v["unit"]) for v in got] == [("S1", "T9")]


def test_vehicle_in_two_latest_queues_is_a_violation() -> None:
    assert check_rows({"S1": ("T1", "T2"), "S2": ("T3",)}) == []
    got = check_rows({"S1": ("T1", "T2"), "S2": ("T2", "T3"), "S3": ("T2",)})
    assert [(v["unit"], v["stations"]) for v in got] == [("T2", ["S1", "S2", "S3"])]
    got = check_rows({"S1": ("T1", "T1")})
    assert [(v["unit"], v["stations"]) for v in got] == [("T1", ["S1", "S1"])]


# --- офлайн-прогон ядра -------------------------------------------------------------------------

CFG = one_station()


def stream() -> list[tuple[int, bytes | None]]:
    """T1 стоит у S1 (занимает), его дубль, мусор, T2 въезжает в зону решения. Offsets с пропуском."""
    t1 = msg("T1", NOON, at(CFG, "S1", 0.0, 10.0), speed=0.0)
    t2 = msg("T2", NOON + 1, at(CFG, "S1", 180.0, 1200.0))
    return [(10, t1), (11, t1), (12, b"{not json"), (14, t2)]


def test_offline_outputs_are_tagged_with_offset_of_their_message() -> None:
    off = run_offline(CFG, stream())
    assert [(o.topic, o.key, o.src) for o in off.outputs] == [(Q, b"S1", 10), (D, b"T2", 14), (Q, b"S1", 14)]
    assert json.loads(off.outputs[1].value) == {
        "unit_uuid": "T2",
        "at": "2026-09-15T12:00:01Z",
        "result": "rejected",
        "reason": "no_gain",
    }
    assert off.messages == 4


def test_offline_counts_messages_and_events() -> None:
    off = run_offline(CFG, stream())
    assert off.counters == {"accepted": 2, "not_newer": 1, "invalid": 1}
    assert off.decisions == {"recommended": 0, "no_gain": 1, "stale_telemetry": 0}
    assert off.fsm_resync == 0
    assert off.occupations["T1"] == 1 and off.occupations["T2"] == 0
    assert set(off.occupations) == set(CFG.assignment)  # все машины площадки, в том числе без занятий
    assert off.prediction_errors == 0
    assert off.violations == [] and off.violation_count == 0


class FakeEngine:
    """Ядро с заранее заданными шагами: так run_offline проверяется на нарушениях, которых настоящее ядро не даёт."""

    def __init__(self, steps: list[StepResult], states: list[SiteState]) -> None:
        self.steps, self.states = iter(steps), iter(states)
        self.state = SiteState()

    def apply(self, raw: bytes | None) -> StepResult:
        self.state = next(self.states)
        return next(self.steps)


def queue_out(station: str, *units: str) -> CoreOutput:
    rows = [{"unit_uuid": u, "eta": None, "service_start": "x", "free_at": "y", "wait_seconds": 0} for u in units]
    value = json.dumps({"station_uuid": station, "at": "t", "queue": rows}).encode()
    return CoreOutput(Q, station.encode(), value)


def test_offline_run_checks_invariants_after_every_message() -> None:
    ok_state = SiteState(NOON, {"T1": vehicle("T1", SERVICE, "S1")}, {"S1": Station("S1", "T1", NOON)})
    bad_state = SiteState(NOON, {"T1": vehicle("T1", WAITING, "S1")}, {"S1": Station("S1", "T1", NOON)})
    acc = (event("accepted"),)
    steps = [
        StepResult((queue_out("S1", "T1", "T2"),), acc),
        StepResult((queue_out("S2", "T2"),), acc),  # T2 теперь в последних очередях S1 и S2
        StepResult((), acc),  # ядро сломало занятую станцию
        StepResult((queue_out("S1", "T1"),), acc),  # T2 остался только в S2
    ]
    engine = FakeEngine(steps, [ok_state, ok_state, bad_state, ok_state])
    off = run_offline(CFG, [(0, b""), (1, b""), (2, b""), (3, b"")], engine=engine)
    assert [(v["offset"], v["kind"]) for v in off.violations] == [(1, "queue_rows"), (2, "occupant")]
    assert off.violation_count == 2


# --- сверка целиком: источники подменены ------------------------------------------------------------


class FakeSources:
    """Kafka и Postgres для verify(): водяные знаки, записи топиков и строка processor."""

    def __init__(self, topics: dict[str, list[Record]], row: ProcessorRow | None, low: int = 0) -> None:
        self.topics, self.row, self.low = topics, row, low
        self.reads: list[tuple[str, int, int]] = []

    def processor(self, site_id: str) -> ProcessorRow | None:
        assert site_id == CFG.site_id
        return self.row

    def watermarks(self, topic: str) -> tuple[int, int]:
        recs = self.topics.get(topic, [])
        return (self.low if topic == TELEMETRY_TOPIC else 0), len(recs) + (self.low if topic == TELEMETRY_TOPIC else 0)

    def read(self, topic: str, start: int, end: int) -> Iterator[Record]:
        self.reads.append((topic, start, end))
        for r in self.topics.get(topic, []):
            assert r.pos is not None
            if start <= r.pos < end:
                yield r

    def close(self) -> None:
        self.closed = True


CHASH = "c0ffee"


def telemetry_records(n_extra: int = 0) -> list[Record]:
    """Поток stream() с подряд идущими offsets 0..3 и n_extra сообщениями после N."""
    raws = [v for _, v in stream()] + [msg("T1", NOON + 100 + i, at(CFG, "S1", 0.0, 10.0), 0.0) for i in range(n_extra)]
    return [Record(TELEMETRY_TOPIC, None, None, v, i) for i, v in enumerate(raws)]


def outputs_of(records: list[Record]) -> dict[str, list[Record]]:
    """Выходы офлайн-прогона как записи лога (с позициями в своих топиках)."""
    off = run_offline(CFG, [(r.pos, r.value) for r in records if r.pos is not None])
    by_topic: dict[str, list[Record]] = {Q: [], D: []}
    for o in off.outputs:
        by_topic[o.topic].append(replace(o, pos=len(by_topic[o.topic])))
    return by_topic


def row(n: int | None, accepted: int = 2, not_newer: int = 1, invalid: int = 1) -> ProcessorRow:
    return ProcessorRow(3, n, LOGIC_VERSION, CHASH, accepted, not_newer, invalid)


def test_verify_happy_path_with_replay_and_tail_after_n() -> None:
    tele = telemetry_records(n_extra=2)
    outs = outputs_of(tele)  # выходы и для двух сообщений после N
    q = outs[Q]
    q.insert(1, replace(q[0], pos=None))  # повтор первой очереди сразу за ней
    outs[Q] = [replace(r, pos=i) for i, r in enumerate(q)]
    src = FakeSources({TELEMETRY_TOPIC: tele, **outs}, row(4))
    rep = verify(CFG, CHASH, src)
    assert rep["ok"], rep
    assert rep["n"] == 4 and rep["telemetry"]["read"] == 4
    assert (TELEMETRY_TOPIC, 0, 4) in src.reads  # telemetry.v1 читается ровно [0, N)
    assert rep["log"]["replays"] == {Q: 1, D: 0}
    assert rep["log"]["after_n"][Q] >= 1
    assert rep["counters"]["ok"] and rep["counters"]["processor"] == {"accepted": 2, "not_newer": 1, "invalid": 1}
    assert rep["offline"]["decisions"]["no_gain"] == 1
    assert rep["offline"]["occupations"]["per_unit_min"] == 0 and rep["offline"]["occupations"]["per_unit_max"] == 1


def test_verify_fails_on_counter_mismatch() -> None:
    tele = telemetry_records()
    rep = verify(CFG, CHASH, FakeSources({TELEMETRY_TOPIC: tele, **outputs_of(tele)}, row(4, not_newer=0)))
    assert not rep["ok"]
    assert rep["counters"]["diff"] == [{"counter": "not_newer", "processor": 0, "offline": 1}]


def test_verify_fails_on_forged_record() -> None:
    tele = telemetry_records()
    outs = outputs_of(tele)
    outs[Q][0] = replace(outs[Q][0], value=outs[Q][0].value.replace(b"T1", b"T7"))
    rep = verify(CFG, CHASH, FakeSources({TELEMETRY_TOPIC: tele, **outs}, row(4)))
    assert not rep["ok"]
    assert rep["mismatches"]["by_kind"] == {"value_differs": 1}


def test_verify_fails_on_invariant_violation(monkeypatch: pytest.MonkeyPatch) -> None:
    """Настоящее ядро нарушений не даёт: проверка строк очередей подменена, чтобы увидеть влияние на итог."""
    import vq.tools.verify as mod

    tele = telemetry_records()
    outs = outputs_of(tele)
    monkeypatch.setattr(mod, "check_rows", lambda latest: [{"kind": "queue_rows", "unit": "T1", "stations": ["S1"]}])
    rep = verify(CFG, CHASH, FakeSources({TELEMETRY_TOPIC: tele, **outs}, row(4)))
    assert not rep["ok"]
    assert rep["mismatches"]["total"] == 0 and rep["counters"]["ok"]
    assert rep["invariants"]["violations"] == 2  # очереди публиковались на сообщениях 0 и 3
    assert [v["offset"] for v in rep["invariants"]["first"]] == [0, 3]


@pytest.mark.parametrize(
    ("proc", "low", "chash", "error"),
    [
        (None, 0, CHASH, "processor"),
        (row(4), 1, CHASH, "low"),
        (row(4), 0, "other", "config_hash"),
        (replace(row(4), logic_version="0"), 0, CHASH, "logic_version"),
        (row(9), 0, CHASH, "high"),
    ],
)
def test_verify_refuses_when_offline_rerun_is_meaningless(
    proc: ProcessorRow | None, low: int, chash: str, error: str
) -> None:
    tele = telemetry_records()
    rep = verify(CFG, chash, FakeSources({TELEMETRY_TOPIC: tele}, proc, low=low))
    assert not rep["ok"]
    assert any(error in e for e in rep["errors"]), rep["errors"]


def test_verify_with_nothing_processed_yet_is_ok_and_empty() -> None:
    rep = verify(CFG, CHASH, FakeSources({TELEMETRY_TOPIC: telemetry_records()}, row(None, 0, 0, 0)))
    assert rep["ok"] and rep["n"] == 0 and rep["telemetry"]["read"] == 0


def test_short_telemetry_read_is_an_error() -> None:
    tele = telemetry_records()

    class Short(FakeSources):
        def read(self, topic: str, start: int, end: int) -> Iterator[Record]:
            yield from list(super().read(topic, start, end))[: 3 if topic == TELEMETRY_TOPIC else None]

    rep = verify(CFG, CHASH, Short({TELEMETRY_TOPIC: tele, **outputs_of(tele)}, row(4)))
    assert not rep["ok"] and any("3 из 4" in e for e in rep["errors"])


# --- вывод: человеку строки, программе — JSON последней строкой ------------------------------------


def test_report_is_printed_with_json_as_the_last_line() -> None:
    tele = telemetry_records()
    rep = verify(CFG, CHASH, FakeSources({TELEMETRY_TOPIC: tele, **outputs_of(tele)}, row(4)))
    buf = io.StringIO()
    with redirect_stdout(buf):
        print_report(rep)
    lines = buf.getvalue().splitlines()
    assert len(lines) > 3
    assert json.loads(lines[-1]) == json.loads(json.dumps(rep))
    assert all(not line.startswith("{") for line in lines[:-1])


def test_main_exit_code_follows_ok(monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    import vq.tools.verify as mod

    tele = telemetry_records()
    good = FakeSources({TELEMETRY_TOPIC: tele, **outputs_of(tele)}, row(4))
    monkeypatch.setattr(mod, "open_sources", lambda env: good)
    monkeypatch.setattr(mod, "config_hash", lambda path: CHASH)
    monkeypatch.setattr(mod, "load_config", lambda path: CFG)
    assert main([]) == 0
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["ok"] is True
    bad = FakeSources({TELEMETRY_TOPIC: tele, **outputs_of(tele)}, row(4, accepted=5))
    monkeypatch.setattr(mod, "open_sources", lambda env: bad)
    assert main([]) == 1
    assert json.loads(capsys.readouterr().out.splitlines()[-1])["ok"] is False


def test_main_reports_unreachable_sources_as_json(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    import vq.tools.verify as mod

    def boom(env: object) -> FakeSources:
        raise OSError("connection refused")

    monkeypatch.setattr(mod, "open_sources", boom)
    monkeypatch.setattr(mod, "config_hash", lambda path: CHASH)
    monkeypatch.setattr(mod, "load_config", lambda path: CFG)
    assert main([]) == 1
    last = json.loads(capsys.readouterr().out.splitlines()[-1])
    assert last["ok"] is False and "connection refused" in last["errors"][0]
