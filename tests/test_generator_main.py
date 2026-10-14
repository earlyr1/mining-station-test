"""Процесс генератора без Kafka: аргументы, темп, часы старта, итог, порядок цикла, продюсер, поток решений и сборка
run() на подделках клиентов."""

import argparse
import json
import logging
import os
import signal
import threading
import time
from collections import Counter
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import pytest
from confluent_kafka import KafkaError, KafkaException
from prometheus_client import CollectorRegistry

from tests.helpers import two_stations
from vq.config import load_config
from vq.core.codec import Telemetry, encode_telemetry
from vq.generator import main as gm
from vq.generator.metrics import GeneratorMetrics
from vq.generator.sim import DecisionStats, Sim
from vq.logs import describe

T0 = 1_789_473_600  # 2026-09-15T12:00:00Z
UNITS = ("T-01", "T-02")
SITE_TOML = Path(__file__).resolve().parents[1] / "config" / "site.toml"
POISON = b"[" * 100_000  # json.loads падает с RecursionError, а не ValueError
# at не в каноническом виде fmt_ts (месяц одной цифрой): strptime такое принимает, parse_ts — нет
NON_CANONICAL_AT = b'{"unit_uuid":"T1","at":"2026-9-15T12:00:00Z","result":"rejected","reason":"no_gain"}'


def rejected(unit: str) -> bytes:
    return f'{{"unit_uuid":"{unit}","at":"2026-09-15T12:00:00Z","result":"rejected","reason":"no_gain"}}'.encode()


def tel(unit: str, ts: int, speed: float = 36.0) -> Telemetry:
    return Telemetry(unit, ts, 49.2, 142.66, speed)


# --- подделки -------------------------------------------------------------


class FakeSim:
    """Каждый тик — по сообщению на машину; журнал вызовов для проверки порядка.

    hook(k) вызывается в начале тика k (сбой, сигнал, ожидание).
    """

    def __init__(
        self,
        start_ts: int,
        clock: "FakeClock | None" = None,
        tick_cost: float = 0.0,
        journal: list | None = None,
        hook: Callable[[int], None] | None = None,
    ) -> None:
        self.t = start_ts
        self.stats = DecisionStats()
        self.journal: list[tuple] = [] if journal is None else journal
        self._clock = clock
        self._cost = tick_cost
        self._hook = hook
        self._k = 0

    def offer_decision(self, raw: bytes) -> None:
        self.journal.append(("offer", raw))
        self.stats.received += 1

    def tick(self) -> list[Telemetry]:
        if self._hook is not None:
            self._hook(self._k)
        self._k += 1
        self.journal.append(("tick", self.t))
        if self._clock is not None:
            self._clock.t += self._cost
        out = [tel(u, self.t) for u in UNITS]
        self.t += 1
        return out


class HookedSim(Sim):
    """Настоящий симулятор; hook(k) вызывается в начале тика k."""

    def __init__(self, cfg, seed: int, start_ts: int, hook: Callable[[int], None]) -> None:
        super().__init__(cfg, seed, start_ts)
        self._hook = hook
        self._k = 0

    def tick(self) -> list[Telemetry]:
        self._hook(self._k)
        self._k += 1
        return super().tick()


class FakeInjector:
    """Каждое сообщение T-01 дублируется в конце прогона; drain отдаёт дубли."""

    def __init__(self, drain_error: Exception | None = None) -> None:
        self.faults: Counter = Counter()
        self.expected_drops = 0
        self.pushed: list[tuple[int, list[str]]] = []
        self._deferred: list[Telemetry] = []
        self._drain_error = drain_error

    def push(self, t: int, msgs: list[Telemetry]) -> list[Telemetry]:
        self.pushed.append((t, [m.unit_uuid for m in msgs]))
        for m in msgs:
            if m.unit_uuid == "T-01":
                self._deferred.append(m)
                self.faults["dup"] += 1
        return list(msgs)

    def drain(self) -> list[Telemetry]:
        if self._drain_error is not None:
            raise self._drain_error
        out, self._deferred = self._deferred, []
        self.expected_drops += len(out)
        return out


class FakeSink:
    def __init__(self) -> None:
        self.sent: list[Telemetry] = []
        self.polls = 0
        self.flushes: list[float] = []
        self.fatal: str | None = None

    @property
    def produced(self) -> int:
        return len(self.sent)

    def send(self, msg: Telemetry) -> None:
        self.sent.append(msg)

    def poll(self) -> None:
        self.polls += 1

    def flush(self, timeout: float) -> int:
        self.flushes.append(timeout)
        return 0


class FakeClock:
    def __init__(self, t: float = 1000.0) -> None:
        self.t = t
        self.sleeps: list[float] = []

    def __call__(self) -> float:
        return self.t

    def sleep(self, d: float) -> None:
        self.sleeps.append(d)
        self.t += d


# --- аргументы -------------------------------------------------------------


def test_defaults_match_spec():
    a = gm.parse_args([])
    assert a.speed == 1.0 and a.duration is None and a.seed == 42
    assert (a.dup_rate, a.late_rate, a.outage_rate) == (0.005, 0.002, 0.0005)
    assert a.summary == Path("/out/summary.json")
    assert not a.no_decisions and not a.allow_dirty


@pytest.mark.parametrize(("text", "speed"), [("1", 1.0), ("20", 20.0), ("2.5", 2.5), ("max", None), ("MAX", None)])
def test_parse_speed(text, speed):
    assert gm.parse_speed(text) == speed


@pytest.mark.parametrize("text", ["0", "-1", "abc", "nan", "inf", ""])
def test_parse_speed_rejects(text):
    with pytest.raises(argparse.ArgumentTypeError):
        gm.parse_speed(text)


def test_speed_max_requires_no_decisions(capsys):
    with pytest.raises(SystemExit) as e:
        gm.parse_args(["--speed", "max"])
    assert e.value.code == 2
    assert "--no-decisions" in capsys.readouterr().err
    a = gm.parse_args(["--speed", "max", "--no-decisions", "--duration", "3600"])
    assert a.speed is None and a.no_decisions and a.duration == 3600


def test_check_args_only_flags_max_without_no_decisions():
    ns = argparse.Namespace
    assert gm.check_args(ns(speed=None, no_decisions=False)) is not None
    assert gm.check_args(ns(speed=None, no_decisions=True)) is None
    assert gm.check_args(ns(speed=20.0, no_decisions=False)) is None


@pytest.mark.parametrize("argv", [["--dup-rate", "1.5"], ["--late-rate", "-0.1"], ["--duration", "0"], ["--seed", "x"]])
def test_bad_values_rejected(argv):
    with pytest.raises(SystemExit):
        gm.parse_args(argv)


# --- темп ------------------------------------------------------------------


def test_pacer_schedule():
    p = gm.Pacer(start_wall=100.0, speed=20.0)
    assert p.due(0) == 100.0
    assert p.due(20) == pytest.approx(101.0)
    assert p.due(1_000_000) == pytest.approx(100.0 + 50_000.0)  # без накопления ошибки


def test_pacer_max_never_waits():
    p = gm.Pacer(start_wall=100.0, speed=None)
    assert p.due(5) is None


def test_ticks_follow_schedule_without_drift():
    """Тик k уходит в start + k / N, сколько бы ни длился сам тик (пока он короче интервала)."""
    clock = FakeClock()
    sim = FakeSim(T0, clock=clock, tick_cost=0.3)
    times: list[float] = []
    gm.run_loop(
        sim,
        FakeInjector(),
        FakeSink(),
        gm.Pacer(clock(), 2.0),
        duration=5,
        stop=gm.StopFlag(),
        on_tick=lambda k, t: times.append(clock() - 0.3),  # момент начала тика
        clock=clock,
        sleep=clock.sleep,
    )
    assert times == pytest.approx([1000.0, 1000.5, 1001.0, 1001.5, 1002.0])
    assert max(clock.sleeps) <= 0.1 + 1e-9  # короткие паузы: сигнал не ждёт конца секунды


def test_max_speed_never_sleeps():
    clock = FakeClock()
    gm.run_loop(
        FakeSim(T0),
        FakeInjector(),
        FakeSink(),
        gm.Pacer(clock(), None),
        duration=50,
        stop=gm.StopFlag(),
        clock=clock,
        sleep=clock.sleep,
    )
    assert clock.sleeps == []


# --- основной цикл ----------------------------------------------------------


def test_loop_order_decisions_before_tick_and_push_with_tick_ts():
    clock = FakeClock()
    sim, inj, sink = FakeSim(T0), FakeInjector(), FakeSink()
    batches = iter([[], [b"d1", b"d2"], [], [b"d3"]])
    res = gm.run_loop(
        sim,
        inj,
        sink,
        gm.Pacer(clock(), None),
        duration=4,
        stop=gm.StopFlag(),
        decisions=lambda: next(batches),
        clock=clock,
        sleep=clock.sleep,
    )
    assert res.ticks == 4 and res.last_ts == T0 + 3
    assert sim.journal == [
        ("tick", T0),
        ("offer", b"d1"),
        ("offer", b"d2"),
        ("tick", T0 + 1),
        ("tick", T0 + 2),
        ("offer", b"d3"),
        ("tick", T0 + 3),
    ]
    assert inj.pushed == [(T0 + k, list(UNITS)) for k in range(4)]  # t — ts тика, сообщения тика по порядку
    assert [m.ts for m in sink.sent] == [T0, T0, T0 + 1, T0 + 1, T0 + 2, T0 + 2, T0 + 3, T0 + 3]
    assert sink.polls == 4


def test_loop_goes_on_past_a_decision_the_real_sim_cannot_parse():
    """GM-3: цикл не ловит исключения решений — их нет. Настоящий Sim сам считает мусор (здесь — JSON глубже
    предела рекурсии) в ignored.invalid и не бросает (тест свойства — в test_generator.py)."""
    sim = Sim(load_config(SITE_TOML), 1, T0)
    batches = iter([[rejected("T-01")], [POISON]])
    clock = FakeClock()
    res = gm.run_loop(
        sim,
        FakeInjector(),
        FakeSink(),
        gm.Pacer(clock(), None),
        duration=4,
        stop=gm.StopFlag(),
        decisions=lambda: next(batches, []),
        clock=clock,
    )
    assert res.ticks == 4
    assert (sim.stats.received, sim.stats.ignored) == (1, Counter({"invalid": 1}))


def test_finish_sends_drain_after_ticks_then_flushes():
    clock = FakeClock()
    sim, inj, sink = FakeSim(T0), FakeInjector(), FakeSink()
    gm.run_loop(sim, inj, sink, gm.Pacer(clock(), None), duration=3, stop=gm.StopFlag(), clock=clock)
    assert gm.finish(inj, sink, flush_timeout=7.0) == 0
    assert [(m.unit_uuid, m.ts) for m in sink.sent[-3:]] == [("T-01", T0), ("T-01", T0 + 1), ("T-01", T0 + 2)]
    assert sink.flushes == [7.0]
    assert sink.produced == 9


def test_stop_flag_ends_loop():
    clock = FakeClock()
    stop = gm.StopFlag()
    res = gm.run_loop(
        FakeSim(T0),
        FakeInjector(),
        FakeSink(),
        gm.Pacer(clock(), 1.0),
        duration=None,
        stop=stop,
        on_tick=lambda k, t: stop.set("SIGTERM") if k == 3 else None,
        clock=clock,
        sleep=clock.sleep,
    )
    assert res.ticks == 3 and stop.reason == "SIGTERM"


def test_stop_interrupts_wait_between_ticks():
    clock = FakeClock()
    stop = gm.StopFlag()

    def sleep(d: float) -> None:
        clock.sleep(d)
        if clock.t >= 1000.3:
            stop.set("SIGINT")

    res = gm.run_loop(
        FakeSim(T0),
        FakeInjector(),
        FakeSink(),
        gm.Pacer(clock(), 1.0),
        duration=10,
        stop=stop,
        clock=clock,
        sleep=sleep,
    )
    assert res.ticks == 1  # второй тик (в 1001.0) не наступил
    assert clock.t < 1000.5


def test_fatal_producer_stops_loop():
    """sink.fatal (produce() в send или страховка error_cb) останавливает цикл после тика. Фатальную ошибку, которую
    живой клиент бросает из poll()/flush(), разбирает run() — см. test_run_fatal_producer_error_*."""
    clock = FakeClock()
    sink = FakeSink()
    stop = gm.StopFlag()

    def fail(k: int, t: int) -> None:
        if k == 2:
            sink.fatal = "fatal"

    res = gm.run_loop(
        FakeSim(T0), FakeInjector(), sink, gm.Pacer(clock(), None), duration=10, stop=stop, on_tick=fail, clock=clock
    )
    assert res.ticks == 2 and stop.reason == "kafka_fatal"


def test_loop_progress_is_visible_after_an_exception():
    """Ход цикла обновляется на месте: после исключения run() знает, сколько тиков прошло."""
    clock = FakeClock()

    def boom(k: int) -> None:
        if k == 3:
            raise RuntimeError("sim bug")

    progress = gm.LoopResult()
    with pytest.raises(RuntimeError):
        gm.run_loop(
            FakeSim(T0, hook=boom),
            FakeInjector(),
            FakeSink(),
            gm.Pacer(clock(), None),
            duration=10,
            stop=gm.StopFlag(),
            clock=clock,
            progress=progress,
        )
    assert (progress.ticks, progress.last_ts) == (3, T0 + 2)


# --- часы старта ---------------------------------------------------------------


def test_start_clock_empty_topic_is_now():
    assert gm.start_clock(T0 + 0.9, None, UNITS) == T0


def test_start_clock_dirty_takes_max_ts_not_last_record():
    tail = [
        encode_telemetry(tel("T-01", T0 + 500)),
        encode_telemetry(tel("T-02", T0 + 499)),
        encode_telemetry(tel("T-01", T0 + 440)),  # опоздавшее сообщение в самом конце хвоста
    ]
    assert gm.start_clock(T0, tail, UNITS) == T0 + 501
    assert gm.start_clock(T0 + 10_000, tail, UNITS) == T0 + 10_000  # часы впереди хвоста — берутся часы


def test_start_clock_ignores_invalid_records():
    tail = [
        None,
        b"not json",
        encode_telemetry(tel("X-99", T0 + 9_999)),  # неизвестная машина
        json.dumps({"unit_uuid": "T-01", "ts": (T0 + 9_999) * 1000, "lat": 1, "lon": 1, "speed_kmh": 1}).encode(),
        encode_telemetry(tel("T-02", T0 + 7)),
    ]
    assert gm.start_clock(T0, tail, UNITS) == T0 + 8
    assert gm.start_clock(T0, [None, b"{}"], UNITS) == T0


# --- итог ------------------------------------------------------------------------


def test_summary_composition():
    stats = DecisionStats(received=12, applied=10, ignored=Counter({"stale_trip": 2}))
    s = gm.build_summary(
        produced=1000, faults=Counter({"dup": 5, "late": 2}), expected_drops=6, stats=stats, extra={"ticks": 25}
    )
    assert s == {
        "produced": 1000,
        "faults": {"dup": 5, "late": 2, "outage": 0},
        "expected_drops": 6,
        "decisions": {
            "received": 12,
            "applied": 10,
            "ignored": {"already_serving": 0, "stale_trip": 2},
            "duplicates": 0,
        },
        "ticks": 25,
    }
    assert list(s)[:4] == ["produced", "faults", "expected_drops", "decisions"]


def test_summary_keeps_unknown_reasons_and_sim_duplicates():
    stats = DecisionStats(ignored=Counter({"invalid": 1}), duplicates=3)
    s = gm.build_summary(produced=0, faults={}, expected_drops=0, stats=stats)
    assert s["decisions"]["ignored"] == {"already_serving": 0, "stale_trip": 0, "invalid": 1}
    assert s["decisions"]["duplicates"] == 3


def test_write_summary_creates_dir_and_replaces_the_file(tmp_path, capsys):
    """Итог — в файл атомарной заменой; в stdout его пишет строка лога summary (см. run), а не print."""
    path = tmp_path / "out" / "nested" / "summary.json"
    gm.prepare_summary_path(path)
    summary = {"produced": 3, "faults": {"dup": 0}, "expected_drops": 0, "decisions": {}}
    gm.write_summary(path, summary)
    assert json.loads(path.read_text()) == summary
    assert not path.with_name("summary.json.tmp").exists()
    assert capsys.readouterr().out == ""


def test_summary_of_a_fake_run():
    """Итог прогона на подделках: отправлено = тики x машины + досылка, отбросы — по инжектору."""
    clock = FakeClock()
    sim, inj, sink = FakeSim(T0), FakeInjector(), FakeSink()
    res = gm.run_loop(sim, inj, sink, gm.Pacer(clock(), None), duration=10, stop=gm.StopFlag(), clock=clock)
    gm.finish(inj, sink)
    s = gm.build_summary(
        produced=sink.produced,
        faults=inj.faults,
        expected_drops=inj.expected_drops,
        stats=sim.stats,
        extra={"ticks": res.ticks},
    )
    assert s["produced"] == 10 * len(UNITS) + 10
    assert s["faults"] == {"dup": 10, "late": 0, "outage": 0}
    assert s["expected_drops"] == 10 and s["ticks"] == 10


# --- продюсер ------------------------------------------------------------------------


class FakeProducer:
    def __init__(self, conf: dict, full_times: int = 0) -> None:
        self.conf = conf
        self.full_times = full_times
        self.produced: list[tuple] = []
        self.polls: list[float] = []

    def produce(self, topic, key=None, value=None, on_delivery=None):
        if self.full_times > 0:
            self.full_times -= 1
            raise BufferError("Local: Queue full")
        self.produced.append((topic, key, value, on_delivery))

    def poll(self, timeout: float) -> int:
        self.polls.append(timeout)
        return 0

    def flush(self, timeout: float) -> int:
        return 0


def test_producer_config_is_idempotent_acks_all_zstd():
    conf = gm.producer_conf("kafka:9092")
    assert conf["enable.idempotence"] is True and conf["acks"] == "all"
    assert conf["linger.ms"] == 5 and conf["compression.type"] == "zstd"


def test_sink_retries_on_buffer_error():
    made: list[FakeProducer] = []

    def make(conf: dict) -> FakeProducer:
        made.append(FakeProducer(conf, full_times=2))
        return made[0]

    sink = gm.KafkaSink(make, gm.producer_conf("k:1"))
    p = made[0]
    assert p.conf["error_cb"] == sink.on_error
    m = tel("T-07", T0)
    sink.send(m)
    assert p.polls == [0.1, 0.1]
    assert [(t, k, v) for t, k, v, _ in p.produced] == [("telemetry.v1", b"T-07", encode_telemetry(m))]
    assert sink.produced == 1


def test_sink_stops_on_produce_exception_instead_of_crashing():
    """produce() в фатальном состоянии: send не падает, цикл встаёт по sink.fatal. Живой клиент в том же тике бросает
    ошибку ещё и из poll() — этот путь проверяет test_run_fatal_producer_error_*[produce-then-poll]."""
    from confluent_kafka import KafkaError, KafkaException

    class FatalProducer(FakeProducer):
        def produce(self, topic, key=None, value=None, on_delivery=None):
            raise KafkaException(KafkaError(KafkaError._FATAL, "fatal idempotent producer error"))

    sink = gm.KafkaSink(lambda conf: FatalProducer(conf), {})
    clock = FakeClock()
    stop = gm.StopFlag()
    res = gm.run_loop(FakeSim(T0), FakeInjector(), sink, gm.Pacer(clock(), None), duration=5, stop=stop, clock=clock)
    assert res.ticks == 1 and stop.reason == "kafka_fatal"
    assert sink.produced == 0 and sink.delivery_failed == 2 and "fatal" in sink.fatal
    assert gm.finish(FakeInjector(), sink) == 0  # досылка не падает, итог можно записать


def test_sink_counts_delivery_failures_and_fatal_errors():
    producer = FakeProducer({})
    sink = gm.KafkaSink(lambda conf: producer, {})
    sink.send(tel("T-01", T0))
    sink.send(tel("T-02", T0))
    reports = [cb for *_, cb in producer.produced]  # отчёты о доставке librdkafka вызывает из poll и flush
    reports[0](None, None)
    reports[1]("Local: Message timed out", None)
    assert (sink.delivered, sink.delivery_failed, sink.last_delivery_error) == (1, 1, "Local: Message timed out")

    class Err:
        def __init__(self, fatal: bool) -> None:
            self._fatal = fatal

        def fatal(self) -> bool:
            return self._fatal

        def __str__(self) -> str:
            return "boom"

    sink.on_error(Err(False))
    assert sink.fatal is None
    # страховка: confluent-kafka 2.15.1 фатальную ошибку в error_cb не отдаёт, а бросает исключением (тесты ниже)
    sink.on_error(Err(True))
    assert sink.fatal == "boom"


# Исключения фатального producer в той форме, что бросает живой клиент (confluent-kafka 2.15.1, librdkafka 2.15.1,
# mock-кластер librdkafka): в error_cb такая ошибка не приходит. Те же формы — в test_dispatcher_main.py.
SYSTEM_ERROR_TEXT = "<class 'str'> returned a result with an exception set"


def fatal_producer_error() -> KafkaException:
    """poll() и flush(): KafkaException, у её KafkaError fatal() == True, код — первопричина."""
    return KafkaException(KafkaError(KafkaError.OUT_OF_ORDER_SEQUENCE_NUMBER, "Broker: Out of order", fatal=True))


def produce_after_fatal_error() -> KafkaException:
    """produce() после фатальной ошибки: код _FATAL, fatal() == False."""
    return KafkaException(KafkaError(KafkaError._FATAL, "Unable to produce message: Local: Fatal error"))


def not_fatal_producer_error() -> KafkaException:
    return KafkaException(KafkaError(KafkaError.MSG_SIZE_TOO_LARGE, "Unable to produce message: Too large"))


def system_error_from(cause: BaseException) -> SystemError:
    """flush() при ожидающем on_delivery: str(err) в on_delivery генератора поднимает SystemError «returned a result
    with an exception set», а исходное исключение остаётся в его __cause__ и __context__."""
    try:
        raise SystemError(SYSTEM_ERROR_TEXT) from cause
    except SystemError as e:
        e.__context__ = cause
        return e


def with_cause(e: BaseException, cause: BaseException) -> BaseException:
    e.__cause__ = cause  # как `raise e from cause`
    return e


def with_context(e: BaseException, context: BaseException, hidden: bool = False) -> BaseException:
    e.__context__ = context  # как исключение, брошенное при обработке context
    e.__suppress_context__ = hidden  # `raise … from None` прячет контекст
    return e


def cycle() -> BaseException:
    a, b = RuntimeError("a"), RuntimeError("b")
    a.__cause__, b.__cause__ = b, a
    return a


@pytest.mark.parametrize(
    "wrap",
    [
        pytest.param(lambda fatal: fatal, id="itself"),
        pytest.param(system_error_from, id="system-error-cause"),
        pytest.param(lambda fatal: with_context(RuntimeError("cleanup"), fatal), id="context"),
        pytest.param(lambda fatal: with_cause(RuntimeError("wrapper"), system_error_from(fatal)), id="nested"),
    ],
)
@pytest.mark.parametrize("make_fatal", [fatal_producer_error, produce_after_fatal_error], ids=["flag", "code"])
def test_fatal_kafka_exception_finds_the_fatal_error_in_the_exception_chain(wrap, make_fatal):
    """KafkaException с фатальной ошибкой клиента (fatal() или код _FATAL) — само исключение или его причина
    (__cause__, иначе не скрытый __context__): находится именно она, а не SystemError вокруг неё."""
    fatal = make_fatal()
    assert gm.fatal_kafka_exception(wrap(fatal)) is fatal


@pytest.mark.parametrize(
    "make_exc",
    [
        pytest.param(not_fatal_producer_error, id="not-fatal"),
        pytest.param(KafkaException, id="no-args"),
        pytest.param(lambda: KafkaException("не KafkaError"), id="not-a-kafka-error"),
        pytest.param(lambda: SystemError(SYSTEM_ERROR_TEXT), id="system-error-alone"),
        pytest.param(lambda: system_error_from(not_fatal_producer_error()), id="cause-not-fatal"),
        pytest.param(lambda: with_context(RuntimeError("x"), fatal_producer_error(), hidden=True), id="hidden"),
        pytest.param(cycle, id="cycle"),
    ],
)
def test_fatal_kafka_exception_is_none_without_a_fatal_error(make_exc):
    assert gm.fatal_kafka_exception(make_exc()) is None


# --- поток решений и проверка топика ----------------------------------------------------


class FakeMsg:
    def __init__(self, value, offset=0, error=None):
        self._value, self._offset, self._error = value, offset, error

    def value(self):
        return self._value

    def offset(self):
        return self._offset

    def error(self):
        return self._error


class FakeConsumer:
    def __init__(self, low: int = 0, high: int = 0, batches=None) -> None:
        self.low, self.high = low, high
        self.batches = list(batches or [])
        self.assigned: list = []
        self.closed = False
        self.unassigned = False

    def get_watermark_offsets(self, tp, timeout=None, cached=False):
        return self.low, self.high

    def assign(self, parts):
        self.assigned.append([(p.topic, p.partition, p.offset) for p in parts])

    def unassign(self):
        self.unassigned = True

    def consume(self, num_messages=1, timeout=-1):
        if self.batches:
            return self.batches.pop(0)
        time.sleep(min(timeout, 0.01))
        return []

    def poll(self, timeout=-1):
        while self.batches and not self.batches[0]:
            self.batches.pop(0)
        if self.batches:
            return self.batches[0].pop(0)
        time.sleep(min(max(timeout, 0.0), 0.01))
        return None

    def close(self):
        self.closed = True


class BatchingConsumer:
    """Семантика librdkafka: consume(num_messages=N, timeout) ждёт N записей или весь таймаут; poll — первую."""

    def __init__(self, high: int = 0) -> None:
        self.high = high
        self.assigned: list = []
        self.closed = False
        self.waiting = threading.Event()  # поток решений уже ждёт внутри poll/consume
        self.held_back = 0  # вызовы, что держали готовую запись до таймаута в ожидании полной пачки
        self._msgs: list[FakeMsg] = []
        self._woken = False
        self._cond = threading.Condition()

    def get_watermark_offsets(self, tp, timeout=None, cached=False):
        return 0, self.high

    def assign(self, parts):
        self.assigned.append([(p.topic, p.partition, p.offset) for p in parts])

    def put(self, msg: FakeMsg) -> None:
        with self._cond:
            self._msgs.append(msg)
            self._cond.notify_all()

    def wake(self) -> None:
        with self._cond:
            self._woken = True
            self._cond.notify_all()

    def _take(self, n: int, timeout: float) -> list[FakeMsg]:
        with self._cond:
            self.waiting.set()
            self._cond.wait_for(lambda: len(self._msgs) >= n or self._woken, None if timeout < 0 else timeout)
            if 0 < len(self._msgs) < n and not self._woken:
                self.held_back += 1
            out, self._msgs = self._msgs[:n], self._msgs[n:]
            return out

    def poll(self, timeout=-1):
        got = self._take(1, timeout)
        return got[0] if got else None

    def consume(self, num_messages=1, timeout=-1):
        return self._take(num_messages, timeout)

    def close(self):
        self.closed = True
        self.wake()


def test_decision_feed_starts_at_high_watermark_and_queues_values():
    consumer = FakeConsumer(
        low=0, high=17, batches=[[FakeMsg(b"a", 17), FakeMsg(None, 18), FakeMsg(b"x", 19, "err")], [FakeMsg(b"b", 20)]]
    )
    feed = gm.DecisionFeed(consumer)
    assert feed.start() == 17
    assert consumer.assigned == [[("decision.v1", 0, 17)]]
    got: list[bytes] = []
    deadline = time.monotonic() + 2.0
    while len(got) < 2 and time.monotonic() < deadline:
        got += feed.drain()
        time.sleep(0.01)
    feed.stop()
    assert got == [b"a", b"b"]  # tombstone и ошибка пропущены
    assert consumer.closed


def test_decision_feed_hands_over_a_lone_record_without_waiting_for_a_batch():
    """GM-1, §4.3: одно решение уходит в drain(), не дожидаясь пачки.

    consume(num_messages=N, timeout) в librdkafka ждёт N записей или весь таймаут: при x20 каждая рекомендация
    запаздывала бы ещё на секунды симуляции сверх задержки самого dispatcher. held_back подделки считает вызовы,
    которые держали готовую запись до таймаута, — проверка по событию, а не по часам.
    """
    consumer = BatchingConsumer(high=5)
    feed = gm.DecisionFeed(consumer, poll_timeout=2.0)
    assert feed.start() == 5
    try:
        assert consumer.waiting.wait(1.0)  # поток уже ждёт внутри вызова — запись придёт в середине ожидания
        consumer.put(FakeMsg(b"d1", 5))
        sent = time.monotonic()
        got: list[bytes] = []
        while not got and time.monotonic() - sent < 5.0:
            got = feed.drain()
            time.sleep(0.001)
        latency = time.monotonic() - sent
    finally:
        consumer.wake()
        feed.stop()
    assert got == [b"d1"] and consumer.held_back == 0
    assert latency < 2.0  # меньше таймаута опроса: ожидание пачки держало бы запись все 2 с
    assert consumer.closed


class StuckConsumer(FakeConsumer):
    """poll не возвращается, пока тест не отпустит: поток решений завис внутри librdkafka."""

    def __init__(self) -> None:
        super().__init__(high=3)
        self.entered = threading.Event()
        self.release = threading.Event()

    def poll(self, timeout=-1):
        self.entered.set()
        self.release.wait(10.0)
        return None


def test_decision_feed_stop_does_not_close_a_consumer_still_in_use(caplog):
    """close() рядом с poll() другого потока на том же клиенте — неопределённое поведение librdkafka (A4-18)."""
    consumer = StuckConsumer()
    feed = gm.DecisionFeed(consumer)
    feed.start()
    try:
        assert consumer.entered.wait(2.0)
        with caplog.at_level(logging.WARNING, logger="vq.generator"):
            feed.stop(timeout=0.05)
        assert not consumer.closed
        assert [r.getMessage() for r in caplog.records] == ["decisions_thread_alive"]
    finally:
        consumer.release.set()


def test_check_topic_refuses_non_empty_without_allow_dirty():
    with pytest.raises(gm.DirtyTopic, match="make reset"):
        gm.check_topic(FakeConsumer(low=0, high=5), UNITS, allow_dirty=False, wall_now=T0)


def test_check_topic_empty_starts_now():
    assert gm.check_topic(FakeConsumer(0, 0), UNITS, allow_dirty=False, wall_now=T0 + 0.5) == (T0, 0)


def test_check_topic_allow_dirty_reads_tail_and_warns(caplog):
    values = [encode_telemetry(tel("T-01", T0 + 50 + i)) for i in range(3)]
    batch = [FakeMsg(v, 2997 + i) for i, v in enumerate(values)]
    consumer = FakeConsumer(low=0, high=3000, batches=[batch])
    with caplog.at_level(logging.WARNING, logger="vq.generator"):
        start, high = gm.check_topic(consumer, UNITS, allow_dirty=True, wall_now=T0)
    assert (start, high) == (T0 + 53, 3000)
    assert consumer.assigned == [[("telemetry.v1", 0, 2000)]]  # последние 1000 записей
    assert consumer.unassigned
    # §4.4: --allow-dirty — с предупреждением в логе
    warns = [r for r in caplog.records if r.getMessage() == "dirty_start"]
    assert len(warns) == 1 and warns[0].levelno == logging.WARNING
    assert warns[0].fields["high"] == 3000 and warns[0].fields["start_ts"] == T0 + 53


def test_check_topic_allow_dirty_on_empty_topic_does_not_warn(caplog):
    with caplog.at_level(logging.WARNING, logger="vq.generator"):
        assert gm.check_topic(FakeConsumer(0, 0), UNITS, allow_dirty=True, wall_now=T0) == (T0, 0)
    assert not [r for r in caplog.records if r.getMessage() == "dirty_start"]


def test_read_tail_times_out_when_tail_is_incomplete():
    consumer = FakeConsumer(low=0, high=10, batches=[[FakeMsg(b"x", 3)]])
    with pytest.raises(TimeoutError):
        gm.read_tail(consumer, "telemetry.v1", 1000, timeout=0.05)


# --- run(): сборка процесса на подделках клиентов -----------------------------------------


class DeliveringProducer:
    """Продюсер с доставкой: колбэки on_delivery вызываются в poll/flush, как в librdkafka.

    raise_next[метод] — исключение, которое poll или flush бросит при следующем вызове, один раз: живой клиент бросает
    фатальную ошибку из того вызова, который её обслужил, а следующий flush уже доставляет отчёты и возвращает остаток.
    produce_error — исключение каждого produce() (после фатальной ошибки — KafkaException с кодом _FATAL).
    """

    def __init__(self, conf: dict, fail_first: int = 0) -> None:
        self.conf = conf
        self.fail_first = fail_first
        self.values: list[bytes] = []
        self.flushes = 0
        self.raise_next: dict[str, BaseException] = {}
        self.produce_error: BaseException | None = None
        self._pending: list[Callable] = []

    def produce(self, topic, key=None, value=None, on_delivery=None):
        if self.produce_error is not None:
            raise self.produce_error
        self.values.append(value)
        self._pending.append(on_delivery)

    def _raise_next(self, method: str) -> None:
        exc = self.raise_next.pop(method, None)
        if exc is not None:
            raise exc

    def _deliver(self) -> None:
        pending, self._pending = self._pending, []
        for cb in pending:
            if self.fail_first > 0:
                self.fail_first -= 1
                cb("Local: Message timed out", None)
            else:
                cb(None, None)

    def poll(self, timeout: float) -> int:
        self._raise_next("poll")
        self._deliver()
        return 0

    def flush(self, timeout: float) -> int:
        self.flushes += 1
        self._raise_next("flush")
        self._deliver()
        return 0


class FeedConsumer:
    """Consumer потока решений внутри run(): assign пишется в общий журнал, записи отдаются через poll."""

    def __init__(self, journal: list, high: int, values: tuple[bytes, ...] = (), fail: Exception | None = None) -> None:
        self.journal = journal
        self.high = high
        self.values = list(values)
        self.fail = fail
        self.closed = False
        # Опрос после последней записи: поток решений уже положил её в очередь (put идёт до следующего poll).
        self.served = threading.Event()

    def get_watermark_offsets(self, tp, timeout=None, cached=False):
        if self.fail is not None:
            raise self.fail
        return 0, self.high

    def assign(self, parts):
        self.journal.append(("assign", [(p.topic, p.partition, p.offset) for p in parts]))

    def poll(self, timeout=-1):
        if self.values:
            return FakeMsg(self.values.pop(0), self.high)
        self.served.set()
        time.sleep(min(max(timeout, 0.0), 0.005))
        return None

    def close(self):
        self.closed = True


@dataclass
class Run:
    code: int
    summary: dict
    journal: list
    made: dict[str, Any]
    registry: CollectorRegistry
    start_ts: int

    def metric(self, name: str, **labels: str) -> float | None:
        return self.registry.get_sample_value(name, labels)


def run_generator(
    tmp_path: Path,
    *extra: str,
    hook: Callable[[int, dict[str, Any]], None] | None = None,
    feed_values: tuple[bytes, ...] = (),
    feed_error: Exception | None = None,
    fail_first: int = 0,
    drain_error: Exception | None = None,
    duration: int = 20,
    real: bool = False,
) -> Run:
    """run() целиком: продюсер и consumer — подделки; итог читается из файла.

    Симулятор и инжектор — подделки (FakeSim, FakeInjector); real=True — настоящие: инжектор run() строит сам
    из аргументов, симулятор тоже, если нет hook. hook(k, made) вызывается в начале тика k; made — созданные
    run() подделки клиентов (consumer, producer).
    """
    path = tmp_path / "summary.json"
    args = gm.parse_args(["--speed", "500", "--duration", str(duration), "--summary", str(path), *extra])
    cfg = two_stations()
    journal: list = []
    made: dict[str, Any] = {}
    start_ts = int(time.time()) + 100_000  # впереди часов: run() не подменит его текущим временем
    sim_hook = None if hook is None else (lambda k: hook(k, made))
    sim: Any = None
    injector: Any = None
    if not real:
        sim, injector = FakeSim(start_ts, journal=journal, hook=sim_hook), FakeInjector(drain_error)
    elif sim_hook is not None:
        sim = HookedSim(cfg, args.seed, start_ts, sim_hook)

    def producer_cls(conf):
        made["producer"] = DeliveringProducer(conf, fail_first)
        return made["producer"]

    def consumer_cls(conf):
        made["consumer"] = FeedConsumer(journal, high=17, values=feed_values, fail=feed_error)
        return made["consumer"]

    registry = CollectorRegistry()
    code = gm.run(
        args,
        cfg,
        "kafka:9092",
        start_ts,
        GeneratorMetrics(registry),
        producer_cls,
        consumer_cls,
        sim=sim,
        injector=injector,
    )
    summary = json.loads(path.read_text()) if path.is_file() else {}
    return Run(code, summary, journal, made, registry, start_ts)


@pytest.fixture
def keep_signal_handlers():
    """run() ставит свои обработчики SIGTERM/SIGINT и обязан вернуть прежние."""
    before = (signal.getsignal(signal.SIGTERM), signal.getsignal(signal.SIGINT))
    yield
    after = (signal.getsignal(signal.SIGTERM), signal.getsignal(signal.SIGINT))
    signal.signal(signal.SIGTERM, before[0])
    signal.signal(signal.SIGINT, before[1])
    assert after == before


def test_run_assigns_decisions_at_high_watermark_before_the_first_tick(tmp_path, keep_signal_handlers):
    """§4.3: decision.v1 назначается на high watermark до первого тика — решения прошлых прогонов не исполняются."""
    r = run_generator(tmp_path)
    assert r.journal[0] == ("assign", [("decision.v1", 0, 17)])
    first_tick = next(i for i, e in enumerate(r.journal) if e[0] == "tick")
    assert first_tick > 0 and r.journal[first_tick] == ("tick", r.start_ts)
    assert r.made["consumer"].closed  # поток решений остановлен, consumer закрыт


def test_run_builds_the_sim_and_the_injector_from_the_arguments(tmp_path, keep_signal_handlers):
    """Без подмен run() строит Sim(cfg, --seed, start_ts) и FaultInjector с долями сбоев из аргументов."""
    r = run_generator(tmp_path, "--seed", "7", "--dup-rate", "1", "--late-rate", "0", "--outage-rate", "0", real=True)
    cfg = two_stations()
    first_tick = [encode_telemetry(m) for m in Sim(cfg, 7, r.start_ts).tick()]
    assert r.made["producer"].values[: len(first_tick)] == first_tick
    n = 20 * len(cfg.assignment)  # дубль у каждого сообщения; каждый дубль не новее уже отправленного
    assert r.summary["faults"] == {"dup": n, "late": 0, "outage": 0} and r.summary["expected_drops"] == n
    assert r.code == gm.EXIT_OK and r.summary["produced"] == 2 * n


def test_run_refuses_to_start_without_the_decision_feed(tmp_path, keep_signal_handlers):
    """§4.3: без decision.v1 замкнутого контура нет — код 1 до первого тика, consumer закрыт."""
    from confluent_kafka import KafkaError, KafkaException

    r = run_generator(tmp_path, feed_error=KafkaException(KafkaError(KafkaError._TIMED_OUT, "watermarks")))
    assert r.code == gm.EXIT_FAIL and r.summary == {}
    assert not [e for e in r.journal if e[0] == "tick"] and r.made["consumer"].closed


def test_run_until_duration_drains_writes_summary_and_syncs_metrics(tmp_path, caplog, keep_signal_handlers):
    """§4.4: по окончании --duration — досылка отложенного, итог в файл и строкой лога summary в stdout, код 0;
    метрики = итогу."""
    with caplog.at_level(logging.INFO, logger="vq.generator"):
        r = run_generator(tmp_path)
    s = r.summary
    assert r.code == gm.EXIT_OK
    assert (s["ticks"], s["start_ts"], s["last_ts"]) == (20, r.start_ts, r.start_ts + 19)
    assert s["produced"] == 20 * len(UNITS) + 20  # тики + досланные дубли
    assert s["expected_drops"] == 20 and s["faults"]["dup"] == 20
    assert (s["stopped_by"], s["delivery_failed"], s["error"]) == ("duration", 0, None)
    assert r.made["producer"].values[-20:] == [encode_telemetry(tel("T-01", r.start_ts + k)) for k in range(20)]
    assert r.made["producer"].flushes == 1
    logged = [rec for rec in caplog.records if rec.name == "vq.generator"]
    assert [rec.getMessage() for rec in logged][-2:] == ["summary", "stop"]
    assert logged[-2].fields == {"summary": s} and logged[-1].fields == {"reason": "duration", "ticks": 20}
    # итоговая синхронизация метрик: счётчики совпадают с итогом
    assert r.metric("vq_gen_telemetry_produced_total") == s["produced"]
    assert r.metric("vq_gen_expected_drops_total") == s["expected_drops"]
    assert r.metric("vq_gen_faults_injected_total", type="dup") == 20
    assert r.metric("vq_gen_sim_time_seconds") == s["last_ts"]


def test_run_sigterm_stops_drains_and_exits_0(tmp_path, keep_signal_handlers):
    def term(k: int, made: dict) -> None:
        if k == 2:
            handler = signal.getsignal(signal.SIGTERM)
            if not callable(handler):  # без обработчика run() сигнал убил бы pytest
                raise AssertionError(f"run() не поставил обработчик SIGTERM: {handler!r}")
            os.kill(os.getpid(), signal.SIGTERM)

    r = run_generator(tmp_path, hook=term, duration=1000)
    s = r.summary
    assert r.code == gm.EXIT_OK
    assert (s["stopped_by"], s["ticks"], s["error"]) == ("SIGTERM", 3, None)
    assert s["produced"] == 3 * len(UNITS) + 3 and s["expected_drops"] == 3  # отложенное досылается и по сигналу


def test_summary_stays_in_the_log_when_the_file_cannot_be_written(tmp_path, caplog, keep_signal_handlers):
    """Файл итога не записался — итог всё равно в stdout строкой summary, код 1."""
    (tmp_path / "summary.json").mkdir()  # путь итога занят каталогом: замена файла упадёт
    with caplog.at_level(logging.INFO, logger="vq.generator"):
        r = run_generator(tmp_path)
    assert r.code == gm.EXIT_FAIL and r.summary == {}
    logged = {rec.getMessage(): rec.fields for rec in caplog.records if rec.name == "vq.generator"}
    assert logged["summary"]["summary"]["produced"] == 20 * len(UNITS) + 20
    assert logged["summary_write_failed"]["path"] == str(tmp_path / "summary.json")


def test_run_failure_inside_the_loop_still_flushes_and_writes_summary(tmp_path, keep_signal_handlers):
    """GM-3: исключение в цикле — не трассировка и потерянный итог: отправленное доставляется (flush), итог
    с stopped_by=error, код 1. Отложенное инжектором после сбоя не досылается: итог и топик согласованы."""

    def boom(k: int, made: dict) -> None:
        if k == 3:
            raise RuntimeError("sim bug")

    r = run_generator(tmp_path, hook=boom)
    s = r.summary
    assert r.code == gm.EXIT_FAIL
    assert s["stopped_by"] == "error" and "sim bug" in s["error"]
    assert (s["ticks"], s["last_ts"]) == (3, r.start_ts + 2)
    assert s["produced"] == 3 * len(UNITS) == len(r.made["producer"].values) and s["expected_drops"] == 0
    assert r.made["producer"].flushes == 1 and r.made["consumer"].closed
    assert r.metric("vq_gen_telemetry_produced_total") == s["produced"]


def test_run_failure_in_the_final_drain_still_flushes_and_writes_summary(tmp_path, keep_signal_handlers):
    r = run_generator(tmp_path, drain_error=RuntimeError("drain bug"))
    s = r.summary
    assert r.code == gm.EXIT_FAIL
    assert s["stopped_by"] == "error" and "drain bug" in s["error"] and s["ticks"] == 20
    assert s["produced"] == 20 * len(UNITS)  # тики ушли, досылка не состоялась
    assert r.made["producer"].flushes == 1 and r.made["consumer"].closed


def fail_producer_at(
    k: int, raises: dict[str, Callable[[], BaseException]], produce: Callable[[], BaseException] | None = None
) -> Callable[[int, dict[str, Any]], None]:
    """hook для run_generator: с тика k продюсер бросает исключения — raises из poll/flush (по разу), produce из
    каждого produce()."""

    def hook(tick: int, made: dict[str, Any]) -> None:
        if tick == k:
            producer = made["producer"]
            producer.raise_next.update({method: make() for method, make in raises.items()})
            if produce is not None:
                producer.produce_error = produce()

    return hook


@pytest.mark.parametrize(
    ("raises", "produce", "ticks", "first"),
    [
        # тик 3: poll() обслужил фатальную ошибку и бросил её
        pytest.param({"poll": fatal_producer_error}, None, 3, fatal_producer_error, id="poll"),
        # так на живом клиенте при x20: produce() этого тика уже бросил ошибку с кодом _FATAL (её ловит send),
        # затем poll() бросил саму фатальную ошибку
        pytest.param(
            {"poll": fatal_producer_error},
            produce_after_fatal_error,
            3,
            produce_after_fatal_error,
            id="produce-then-poll",
        ),
        # так на живом клиенте при x2000: ошибка пришла в последнем flush(), когда ждали отчёты о доставке
        pytest.param(
            {"flush": lambda: system_error_from(fatal_producer_error())},
            None,
            20,
            fatal_producer_error,
            id="flush-system-error",
        ),
    ],
)
def test_run_fatal_producer_error_stops_with_kafka_fatal_and_names_it(
    tmp_path, caplog, keep_signal_handlers, raises, produce, ticks, first
):
    """§4.4: фатальная ошибка продюсера приходит не в error_cb, а исключением из poll()/flush() (из flush() при
    ожидающем on_delivery — SystemError с ней в __cause__). Итог: stopped_by kafka_fatal, в error — сама ошибка Kafka,
    а не SystemError вокруг неё, код 1; в run_incomplete.fatal — первая фатальная ошибка продюсера."""
    with caplog.at_level(logging.INFO, logger="vq.generator"):
        r = run_generator(tmp_path, hook=fail_producer_at(3, raises, produce))
    s = r.summary
    want = {"stopped_by": "kafka_fatal", "error": describe(fatal_producer_error()), "ticks": ticks}
    assert (r.code, {key: s[key] for key in want}) == (gm.EXIT_FAIL, want)
    logged = {rec.getMessage(): rec for rec in caplog.records if rec.name == "vq.generator"}
    assert logged["run_incomplete"].fields["fatal"] == str(first())
    # строка без стека, как exit_kafka_fatal у dispatcher: это ошибка клиента Kafka, а не сбой кода
    assert logged["kafka_fatal"].fields == {"error": s["error"], "ticks": ticks} and "run_failed" not in logged
    # уже отправленное доставляется: flush после ошибки (второй, если бросил первый); поток решений остановлен
    assert r.made["producer"].flushes == (2 if "flush" in raises else 1) and r.made["consumer"].closed


@pytest.mark.parametrize(
    ("raises", "ticks", "error"),
    [
        pytest.param({"poll": not_fatal_producer_error}, 3, describe(not_fatal_producer_error()), id="poll-not-fatal"),
        # голый SystemError из flush(), без фатальной ошибки в цепочке: прежний путь сбоя
        pytest.param(
            {"flush": lambda: SystemError(SYSTEM_ERROR_TEXT)},
            20,
            f"SystemError: {SYSTEM_ERROR_TEXT}",
            id="flush-system-error-alone",
        ),
    ],
)
def test_run_producer_exception_without_a_fatal_error_stays_an_error(
    tmp_path, caplog, keep_signal_handlers, raises, ticks, error
):
    """Не всякое исключение продюсера — фатальная ошибка клиента: прочие, как и сбой цикла, — stopped_by error и
    строка run_failed со стеком; sink.fatal не выставляется."""
    with caplog.at_level(logging.INFO, logger="vq.generator"):
        r = run_generator(tmp_path, hook=fail_producer_at(3, raises))
    s = r.summary
    want = {"stopped_by": "error", "error": error, "ticks": ticks}
    assert (r.code, {key: s[key] for key in want}) == (gm.EXIT_FAIL, want)
    logged = {rec.getMessage(): rec for rec in caplog.records if rec.name == "vq.generator"}
    assert logged["run_incomplete"].fields["fatal"] is None
    assert logged["run_failed"].exc_info is not None and "kafka_fatal" not in logged


def test_run_counts_a_decision_the_sim_cannot_take_and_goes_on(tmp_path, keep_signal_handlers):
    """GM-3: испорченная запись decision.v1 (и at не в каноническом виде) не останавливает генератор и видна как
    ignored{invalid} в итоге и в метриках. Путь настоящий: поток решений → цикл → Sim."""

    def wait_for_feed(k: int, made: dict) -> None:
        if k == 0:  # все записи уже в очереди потока решений: цикл отдаст их перед тиком 1
            assert made["consumer"].served.wait(2.0)

    feed = (POISON, NON_CANONICAL_AT, rejected("T1"))
    r = run_generator(tmp_path, hook=wait_for_feed, feed_values=feed, duration=50, real=True)
    s = r.summary
    assert r.code == gm.EXIT_OK and s["ticks"] == 50
    assert s["decisions"]["ignored"]["invalid"] == 2 and s["decisions"]["received"] == 1
    assert s["decisions"]["duplicates"] == 0  # тот же момент в каноническом виде — не повтор
    assert r.metric("vq_gen_decisions_ignored_total", reason="invalid") == 2
    assert r.metric("vq_gen_decisions_received_total") == 1


def test_run_delivery_failure_exits_1(tmp_path, keep_signal_handlers):
    r = run_generator(tmp_path, fail_first=1)
    s = r.summary
    assert r.code == gm.EXIT_FAIL
    assert (s["stopped_by"], s["delivery_failed"], s["error"]) == ("duration", 1, None)


def test_run_without_decisions_creates_no_consumer(tmp_path, keep_signal_handlers):
    r = run_generator(tmp_path, "--no-decisions")
    assert r.code == gm.EXIT_OK and "consumer" not in r.made
    assert not [e for e in r.journal if e[0] == "assign"]


# --- метрики ----------------------------------------------------------------------------


def test_metrics_sync_increments_by_delta():
    reg = CollectorRegistry()
    m = GeneratorMetrics(reg)
    assert reg.get_sample_value("vq_gen_faults_injected_total", {"type": "outage"}) == 0.0
    common = dict(received=3, applied=2, sim_time=T0)
    m.sync(produced=40, faults={"dup": 1}, expected_drops=1, ignored={}, delays=[2], **common)
    m.sync(
        produced=80, faults={"dup": 1, "late": 2}, expected_drops=4, ignored={"stale_trip": 1}, delays=[2, 6], **common
    )
    assert reg.get_sample_value("vq_gen_telemetry_produced_total") == 80
    assert reg.get_sample_value("vq_gen_faults_injected_total", {"type": "dup"}) == 1
    assert reg.get_sample_value("vq_gen_faults_injected_total", {"type": "late"}) == 2
    assert reg.get_sample_value("vq_gen_expected_drops_total") == 4
    assert reg.get_sample_value("vq_gen_decisions_received_total") == 3
    assert reg.get_sample_value("vq_gen_decisions_applied_total") == 2
    assert reg.get_sample_value("vq_gen_decisions_ignored_total", {"reason": "stale_trip"}) == 1
    assert reg.get_sample_value("vq_gen_decision_delay_sim_seconds_count") == 2  # каждая задержка — один раз
    assert reg.get_sample_value("vq_gen_decision_delay_sim_seconds_sum") == 8
    assert reg.get_sample_value("vq_gen_sim_time_seconds") == T0


# --- логи ---------------------------------------------------------------------------------


@pytest.fixture
def root_logger():
    """Корневой логгер после теста — как был: main() ставит на него обработчик stdout."""
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield root
    root.handlers[:] = handlers
    root.setLevel(level)


def test_main_writes_json_log_lines_to_stdout(tmp_path, monkeypatch, capsys, root_logger):
    """Лог генератора — JSON-строки vq.logs в stdout, как у dispatcher и витрины (A4-03)."""
    monkeypatch.setenv("VQ_CONFIG", str(tmp_path / "missing.toml"))
    assert gm.main(["--summary", str(tmp_path / "summary.json")]) == gm.EXIT_FAIL
    out, err = capsys.readouterr()
    (line,) = out.splitlines()
    doc = json.loads(line)
    assert list(doc)[:4] == ["ts", "level", "logger", "event"] and doc["ts"].endswith("Z")
    assert (doc["level"], doc["logger"], doc["event"]) == ("ERROR", "vq.generator", "start_failed")
    assert "missing.toml" in doc["error"] and err == ""
