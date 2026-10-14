"""vq-dispatcher main (§2.4, §6.1): настройки клиентов Kafka, одна партиция, ошибки и логи; run() и consume_loop
с поддельными Kafka и Postgres — связка, коды выхода, строки выхода."""

import json
import logging
import os
import signal
import threading
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import psycopg
import pytest
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition
from prometheus_client import CollectorRegistry

from tests.dispatcher_helpers import FakeStore, empty_claim, example, make, offline
from tests.helpers import NOON, two_stations
from vq.config import ConfigError, SiteConfig
from vq.core.codec import src_headers
from vq.core.engine import Engine
from vq.core.model import SiteState
from vq.dispatcher import main as dmain
from vq.dispatcher.loop import Deadline
from vq.dispatcher.metrics import DispatcherMetrics
from vq.dispatcher.store import MIGRATIONS_DIR, Claim, Counters, StartRefused
from vq.logs import JsonFormatter

# --- KafkaSink -----------------------------------------------------------------------------


class FakeProducer:
    def __init__(self, full: int = 0):
        self.full = full
        self.produced: list[tuple[str, bytes, bytes, object]] = []
        self.polls: list[float] = []

    def produce(self, topic, value, key, headers, on_delivery):
        if self.full:
            self.full -= 1
            raise BufferError("Local: Queue full")
        self.produced.append((topic, key, value, headers))

    def poll(self, timeout):
        self.polls.append(timeout)
        return 0

    def flush(self, timeout):
        return 3


class FakeRecord:
    def topic(self):
        return "queue.v1"


def test_kafka_sink_sends_header_and_retries_full_local_queue():
    producer = FakeProducer(full=2)
    sink = dmain.KafkaSink(producer)
    sink.send("queue.v1", b"S1", b"{}", 42)
    assert producer.produced == [("queue.v1", b"S1", b"{}", [("vq-src-offset", b"42")])]
    assert producer.polls == [0.1, 0.1, 0]
    assert sink.flush(10.0) == 3


def test_kafka_sink_keeps_first_delivery_error():
    sink = dmain.KafkaSink(FakeProducer())
    sink.on_delivery(None, FakeRecord())
    assert sink.failed is None
    sink.on_delivery(KafkaError(KafkaError._MSG_TIMED_OUT), FakeRecord())
    sink.on_delivery(KafkaError(KafkaError._TRANSPORT), FakeRecord())
    assert sink.failed == "queue.v1: Local: Message timed out"


# --- main: настройки клиентов, одна партиция, ошибки, логи ------------------------------------


def test_consumer_and_producer_settings_match_spec():
    assert dmain.consumer_config("redpanda:9092", "site-1") == {
        "bootstrap.servers": "redpanda:9092",
        "group.id": "dispatcher",
        "group.instance.id": "dispatcher-site-1",
        "session.timeout.ms": 10000,
        "heartbeat.interval.ms": 3000,
        "enable.auto.commit": False,
        "auto.offset.reset": "error",
        "group.protocol": "classic",
        "partition.assignment.strategy": "range",
    }
    assert dmain.consumer_config("k", "site-1", "dispatcher-site-1-b")["group.instance.id"] == "dispatcher-site-1-b"
    assert dmain.producer_config("redpanda:9092") == {
        "bootstrap.servers": "redpanda:9092",
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 5,
        "compression.type": "zstd",
        "message.timeout.ms": 30000,
    }


def test_settings_are_accepted_by_librdkafka():
    quiet = {"logger": logging.getLogger("test.quiet"), "error_cb": lambda err: None}
    consumer = Consumer({**dmain.consumer_config("127.0.0.1:1", "site-1"), **quiet})
    consumer.close()
    producer = Producer({**dmain.producer_config("127.0.0.1:1"), **quiet})
    assert producer.flush(0) == 0


@pytest.mark.parametrize("partitions", [None, 0, 2, 12])
def test_single_partition_check_refuses(partitions):
    with pytest.raises(StartRefused) as refused:
        dmain.check_single_partition("telemetry.v1", partitions)
    assert refused.value.reason == "partitions"


def test_single_partition_check_accepts_one():
    dmain.check_single_partition("telemetry.v1", 1)


@pytest.mark.parametrize(
    ("err", "kind"),
    [
        (KafkaError(KafkaError._TRANSPORT), "info"),
        (KafkaError(KafkaError._ALL_BROKERS_DOWN), "info"),
        (KafkaError(KafkaError._PARTITION_EOF), "info"),
        (KafkaError(KafkaError._AUTO_OFFSET_RESET), "offset_reset"),
        # так consume() отдаёт фатальную ошибку consumer (например, вытеснение по group.instance.id): код _FATAL,
        # fatal() == False — проверено на Redpanda v26.2.3
        (KafkaError(KafkaError._FATAL, "Fatal error: Broker: Static consumer fenced by other consumer"), "fatal"),
        (KafkaError(KafkaError.FENCED_INSTANCE_ID, fatal=True), "fatal"),
        (KafkaError(KafkaError.UNKNOWN_TOPIC_OR_PART), "other"),
    ],
)
def test_consume_error_classification(err, kind):
    assert dmain.classify_error(err) == kind


def test_kafka_error_callback_logs_and_raises_only_on_fatal(caplog):
    """error_cb: нефатальная ошибка — WARNING и работа дальше; фатальная — KafkaFatal, то есть выход 1 (страховка:
    confluent-kafka 2.15.1 фатальную ошибку в error_cb не отдаёт, а бросает исключением, см. тесты exit_line)."""
    p, *_ = make()
    p.on_assign(40, 50)
    fatal = KafkaError(KafkaError.OUT_OF_ORDER_SEQUENCE_NUMBER, "Broker: Out of order sequence number", fatal=True)
    with caplog.at_level(logging.WARNING, logger="vq.dispatcher"):
        dmain.on_kafka_error(KafkaError(KafkaError._TRANSPORT, "Disconnected"), p)
        with pytest.raises(dmain.KafkaFatal, match="Out of order sequence number"):
            dmain.on_kafka_error(fatal, p)
    got = [(r.levelname, r.fields["code"], r.fields["fatal"], r.fields["epoch"]) for r in caplog.records]
    assert got == [("WARNING", "_TRANSPORT", False, 7), ("ERROR", "OUT_OF_ORDER_SEQUENCE_NUMBER", True, 7)]


def test_offset_reset_reason():
    assert dmain.offset_reset_reason(3, 10, 20) == "state_older_than_log"
    assert dmain.offset_reset_reason(25, 0, 20) == "log_recreated"
    assert dmain.offset_reset_reason(None, -1, -1) == "state_older_than_log"


def test_json_log_line_has_site_epoch_offset_event():
    """§2.4: в каждой строке — site, epoch, offset и event; у строк не процессора (librdkafka) epoch и offset — null."""
    fmt = JsonFormatter(dmain.log_defaults("site-1"))
    rec = logging.LogRecord("vq.dispatcher", logging.INFO, __file__, 1, "claim", None, None)
    rec.fields = {"site": "site-1", "epoch": 3, "offset": 40, "low": 0}
    doc = json.loads(fmt.format(rec))
    assert list(doc) == ["ts", "level", "logger", "event", "site", "epoch", "offset", "low"]
    assert (doc["event"], doc["site"], doc["epoch"], doc["offset"], doc["low"]) == ("claim", "site-1", 3, 40, 0)
    foreign = logging.LogRecord("vq.kafka", logging.WARNING, __file__, 1, "FAIL|rdkafka#consumer-1| boom", None, None)
    doc = json.loads(fmt.format(foreign))
    assert {k: v for k, v in doc.items() if k != "ts"} == {
        "level": "WARNING",
        "logger": "vq.kafka",
        "event": "log",
        "site": "site-1",
        "epoch": None,
        "offset": None,
        "msg": "FAIL|rdkafka#consumer-1| boom",
    }


# --- main.run и consume_loop с поддельными Kafka и Postgres: связка, коды выхода, строки выхода -------------


class FakeConsumer:
    """confluent_kafka.Consumer в миниатюре: consume() проигрывает сценарий шагов.

    Шаг ("assign" | "revoke" | "lost", [партиции]) вызывает колбэк группы изнутри consume(), как librdkafka;
    ("msgs", [Message]) — ответ consume(). Сценарий кончился — выставляется stop (как по SIGTERM).
    """

    def __init__(
        self,
        script: list[tuple],
        stop: threading.Event,
        partitions: int,
        watermarks: tuple[int, int],
        close_error: Exception | None = None,
    ):
        self.script = list(script)
        self.stop = stop
        self.partitions = partitions
        self.watermarks = watermarks
        self.close_error = close_error
        self.conf: dict[str, Any] = {}
        self.callbacks: dict[str, Callable[..., None]] = {}
        self.calls: list[tuple] = []

    def __call__(self, conf: dict[str, Any]) -> "FakeConsumer":  # вместо конструктора confluent_kafka.Consumer
        self.conf = conf
        return self

    def list_topics(self, topic: str, timeout: float) -> SimpleNamespace:
        parts = dict.fromkeys(range(self.partitions))
        return SimpleNamespace(topics={topic: SimpleNamespace(error=None, partitions=parts)})

    def subscribe(self, topics: list[str], on_assign, on_revoke, on_lost) -> None:
        self.calls.append(("subscribe", topics))
        self.callbacks = {"assign": on_assign, "revoke": on_revoke, "lost": on_lost}

    def get_watermark_offsets(self, tp: TopicPartition, timeout: float | None = None, cached: bool = False):
        return self.watermarks

    def assign(self, partitions: list[TopicPartition]) -> None:
        self.calls.append(("assign", [(tp.topic, tp.partition, tp.offset) for tp in partitions]))

    def seek(self, tp: TopicPartition) -> None:
        self.calls.append(("seek", tp.offset))

    def commit(self, offsets: list[TopicPartition], asynchronous: bool) -> None:
        self.calls.append(("commit", [tp.offset for tp in offsets], asynchronous))

    def close(self) -> None:
        self.calls.append(("close",))
        if self.close_error is not None:
            raise self.close_error

    def consume(self, num_messages: int, timeout: float) -> list[Message]:
        assert (num_messages, timeout) == (500, 0.25)
        if not self.script:
            self.stop.set()
            return []
        kind, *args = self.script.pop(0)
        if kind in self.callbacks:
            self.callbacks[kind](self, args[0] if args else [TopicPartition("telemetry.v1", 0)])
            return []
        assert kind == "msgs", kind
        return args[0]

    def made(self, *kinds: str) -> list[tuple]:
        return [c for c in self.calls if c[0] in kinds]


class FakeKafkaProducer:
    """confluent_kafka.Producer: запоминает записи; raises — (метод, исключение), которое этот метод бросает.

    Так ведёт себя producer в фатальном состоянии: confluent-kafka 2.15.1 не отдаёт фатальную ошибку в error_cb,
    а бросает её из вызова, который обслуживал клиента (poll, flush, produce)."""

    def __init__(self, remaining: int, raises: tuple[str, BaseException] | None):
        self.conf: dict[str, Any] = {}
        self.produced: list[tuple] = []
        self.remaining = remaining
        self.raises = raises

    def __call__(self, conf: dict[str, Any]) -> "FakeKafkaProducer":
        self.conf = conf
        return self

    def _maybe_raise(self, method: str) -> None:
        if self.raises is not None and self.raises[0] == method:
            raise self.raises[1]

    def produce(self, topic, value, key, headers, on_delivery) -> None:
        self._maybe_raise("produce")
        self.produced.append((topic, key, value, headers))

    def poll(self, timeout: float) -> int:
        self._maybe_raise("poll")
        return 0

    def flush(self, timeout: float) -> int:
        self._maybe_raise("flush")
        return self.remaining


class FakePgStore(FakeStore):
    """PgStore: error — из checkpoint, migrate_stuck — migrate висит до события, *_error — исключения migrate, close."""

    def __init__(
        self,
        claim: Claim,
        accept: bool,
        error: Exception | None,
        migrate_error: Exception | None = None,
        migrate_stuck: threading.Event | None = None,
        close_error: Exception | None = None,
    ):
        super().__init__([], claim, accept)
        self.error = error
        self.migrate_error = migrate_error
        self.migrate_stuck = migrate_stuck
        self.close_error = close_error
        self.migrated = False
        self.closed = False

    def migrate(self) -> None:
        self.migrated = True
        if self.migrate_stuck is not None:
            assert self.migrate_stuck.wait(5)
        if self.migrate_error is not None:
            raise self.migrate_error

    def checkpoint(self, site_id: str, epoch: int, next_offset: int, counters: Counters, state: SiteState) -> bool:
        if self.error is not None:
            raise self.error
        return super().checkpoint(site_id, epoch, next_offset, counters, state)

    def close(self) -> None:
        self.closed = True
        if self.close_error is not None:
            raise self.close_error


class LogLines(logging.Handler):
    """Строки лога в JSON в момент записи, как в stdout: defaults форматтера run() меняет по ходу старта."""

    def __init__(self, formatter: logging.Formatter) -> None:
        super().__init__()
        self.setFormatter(formatter)
        self.lines: list[dict] = []

    def emit(self, record: logging.LogRecord) -> None:
        self.lines.append(json.loads(self.format(record)))


@dataclass
class Ran:
    code: int
    consumer: FakeConsumer
    store: FakePgStore
    producer: FakeKafkaProducer
    registry: CollectorRegistry
    lines: list[dict]  # строки лога vq.dispatcher в JSON, как их пишет JsonFormatter
    opened: list[tuple[str, Path]]  # попытки подключиться к Postgres: (dsn, каталог миграций)

    def events(self) -> list[str]:
        return [d["event"] for d in self.lines]

    def line(self, event: str) -> dict:
        (found,) = [d for d in self.lines if d["event"] == event]
        return found


@pytest.fixture
def run_dispatcher(monkeypatch, caplog) -> Callable[..., Ran]:
    def go(
        script: list[tuple],
        *,
        claim: Claim | None = None,
        accept: bool = True,
        store_error: Exception | None = None,
        remaining: int = 0,
        producer_raises: tuple[str, BaseException] | None = None,
        partitions: int = 1,
        watermarks: tuple[int, int] = (0, 100),
        env: dict[str, str] | None = None,
        pg_down: int = 0,
        pg_wait_s: float = 60.0,
        sigterm_after: int | None = None,
        migrate_error: Exception | None = None,
        migrate_stuck: threading.Event | None = None,
        deadline: Deadline | None = None,
        consumer_close_error: Exception | None = None,
        store_close_error: Exception | None = None,
        config_error: Exception | None = None,
    ) -> Ran:
        """pg_down — столько первых подключений к Postgres падают; sigterm_after — SIGTERM на этой попытке;
        config_error — исключение чтения site.toml."""
        cfg = two_stations()
        stop = threading.Event()
        consumer = FakeConsumer(script, stop, partitions, watermarks, consumer_close_error)
        store = FakePgStore(
            claim or empty_claim(), accept, store_error, migrate_error, migrate_stuck, store_close_error
        )
        producer = FakeKafkaProducer(remaining, producer_raises)
        reg = CollectorRegistry()
        opened: list[tuple[str, Path]] = []

        def open_pg(dsn: str, migrations_dir: Path) -> FakePgStore:
            opened.append((dsn, migrations_dir))
            if len(opened) == sigterm_after:
                stop.set()
            if len(opened) <= pg_down:
                raise psycopg.OperationalError(f"connection to server failed: Connection refused ({len(opened)})")
            return store

        def load_config(path: str) -> SiteConfig:
            if config_error is not None:
                raise config_error
            return cfg

        monkeypatch.setattr(dmain, "load_config", load_config)
        monkeypatch.setattr(dmain, "config_hash", lambda path: "cfg-hash")
        monkeypatch.setattr(dmain, "PgStore", open_pg)
        monkeypatch.setattr(dmain, "PG_RETRY_S", 0.0)
        monkeypatch.setattr(dmain, "PG_WAIT_S", pg_wait_s)
        if deadline is not None:
            monkeypatch.setattr(dmain, "Deadline", lambda: deadline)
        monkeypatch.setattr(dmain, "Producer", producer)
        monkeypatch.setattr(dmain, "Consumer", consumer)
        monkeypatch.setattr(dmain, "DispatcherMetrics", lambda stations: DispatcherMetrics(reg, stations=stations))
        monkeypatch.setattr(dmain, "start_http_server", lambda port: None)
        fmt = JsonFormatter()
        out = LogLines(fmt)
        dlog = logging.getLogger("vq.dispatcher")
        dlog.addHandler(out)
        try:
            with caplog.at_level(logging.INFO, logger="vq.dispatcher"):
                code = dmain.run(env or {}, fmt, stop)
        finally:
            dlog.removeHandler(out)
        return Ran(code, consumer, store, producer, reg, out.lines, opened)

    return go


def kafka_msgs(raws: list[bytes | None], start: int) -> list[Message]:
    return [Message(topic="telemetry.v1", partition=0, offset=start + i, value=r) for i, r in enumerate(raws)]


def kafka_error(code: int, reason: str = "") -> Message:
    return Message(topic="telemetry.v1", partition=0, error=KafkaError(code, reason or None))


# Исключения фатального producer в форме, которую бросает живой клиент (confluent-kafka 2.15.1, librdkafka 2.15.1):
# проверено на Redpanda v26.2.3 и без брокера через rd_kafka_test_fatal_error. В error_cb такая ошибка не приходит.


def fatal_producer_error() -> KafkaException:
    """poll() и flush(): KafkaException, у её KafkaError fatal() == True, код — первопричина."""
    return KafkaException(KafkaError(KafkaError.OUT_OF_ORDER_SEQUENCE_NUMBER, "Broker: Out of order", fatal=True))


def produce_after_fatal_error() -> KafkaException:
    """produce() после фатальной ошибки: код _FATAL, fatal() == False."""
    return KafkaException(KafkaError(KafkaError._FATAL, "Unable to produce message: Local: Fatal error"))


def system_error_from(cause: BaseException) -> SystemError:
    """flush() при ожидающем on_delivery: CPython поднимает SystemError «returned a result with an exception set»
    (msg.topic() в on_delivery), а исходное исключение остаётся в его __cause__ и __context__."""
    text = "<method 'topic' of 'confluent_kafka.cimpl.Message' objects> returned a result with an exception set"
    try:
        raise SystemError(text) from cause
    except SystemError as e:
        e.__context__ = cause
        return e


def test_run_assigns_explicit_offset_commits_reports_lag_and_closes_on_stop(run_dispatcher):
    cfg = two_stations()
    raws = example(cfg)
    r = run_dispatcher([("assign",), ("msgs", kafka_msgs(raws, 40))], watermarks=(40, 52))
    assert r.code == dmain.EXIT_OK
    assert r.consumer.conf["group.instance.id"] == "dispatcher-test" and r.store.migrated
    assert r.consumer.calls[0] == ("subscribe", ["telemetry.v1"])
    # assign с явным offset из claim (а не из committed), commit асинхронно на конец пачки, затем close
    assert r.consumer.made("assign", "seek", "commit", "close") == [
        ("assign", [("telemetry.v1", 0, 40)]),
        ("commit", [45], True),
        ("close",),
    ]
    assert r.registry.get_sample_value("vq_consumer_lag_messages") == 52 - 45
    sent = [(t, k, v, src_headers(s)) for t, k, v, s in offline(cfg, raws, 40)]
    assert r.producer.produced == sent
    assert r.store.checkpoints[-1][1] == 45 and r.store.closed
    assert r.events() == ["start", "claim", "decision", "decision", "stop"]
    assert (r.line("claim")["epoch"], r.line("claim")["offset"], r.line("stop")["offset"]) == (7, 40, 45)
    assert all(list(d)[:7] == ["ts", "level", "logger", "event", "site", "epoch", "offset"] for d in r.lines)


def test_run_seeks_back_to_expected_offset_after_dropped_batch(run_dispatcher):
    cfg = two_stations()
    raws = example(cfg)
    claim = Claim(3, 5, None, Counters(), SiteState())
    r = run_dispatcher(
        [("assign",), ("msgs", kafka_msgs(raws, 2)), ("msgs", kafka_msgs(raws, 5))], claim=claim, watermarks=(0, 10)
    )
    assert r.code == dmain.EXIT_OK
    assert r.consumer.made("assign", "seek", "commit") == [
        ("assign", [("telemetry.v1", 0, 5)]),
        ("seek", 5),
        ("commit", [10], True),
    ]
    assert r.line("batch_dropped")["got"] == 2 and r.store.checkpoints[-1][1] == 10


def test_run_ignores_empty_assignment_and_drops_engine_on_lost_partition(run_dispatcher):
    cfg = two_stations()
    r = run_dispatcher([("assign", []), ("assign",), ("lost", []), ("msgs", kafka_msgs(example(cfg), 0))])
    assert r.code == dmain.EXIT_OK
    assert len(r.store.claims) == 1  # пустое назначение (участников больше, чем партиций) — без claim и assign
    assert r.consumer.made("assign") == [("assign", [("telemetry.v1", 0, 0)])]
    assert r.producer.produced == [] and r.store.checkpoints == [] and r.consumer.made("commit") == []
    assert r.line("lost")["epoch"] == 7


def test_run_logs_informational_consume_errors_and_keeps_going(run_dispatcher):
    cfg = two_stations()
    transport = kafka_error(KafkaError._TRANSPORT, "Disconnected")
    r = run_dispatcher([("assign",), ("msgs", [transport, *kafka_msgs(example(cfg), 0)])], watermarks=(0, 5))
    assert r.code == dmain.EXIT_OK
    err = r.line("kafka_error")
    assert (err["level"], err["code"], err["epoch"], err["offset"]) == ("INFO", "_TRANSPORT", 7, 0)
    assert r.store.checkpoints[-1][1] == 5


def _kernel_boom(self: Engine, raw: bytes | None) -> Any:
    raise RuntimeError("ядро упало")


@pytest.mark.parametrize(
    ("case", "event"),
    [
        ("fenced", "exit_fenced"),
        ("delivery", "exit_delivery_failed"),
        ("postgres", "exit_postgres"),
        ("kernel", "exit_crash"),
        ("consumer_fatal", "exit_kafka_fatal"),
        ("producer_fatal", "exit_kafka_fatal"),
    ],
)
def test_run_failure_exits_1_and_exit_line_names_epoch_and_batch(run_dispatcher, monkeypatch, case, event):
    """Crash-only (§2.4): выход 1, consumer закрыт, строка выхода несёт site, epoch и offset упавшей пачки."""
    msgs = kafka_msgs(example(two_stations()), 40)
    kw: dict[str, Any] = {}
    if case == "fenced":
        kw["accept"] = False
    elif case == "delivery":
        kw["remaining"] = 2
    elif case == "postgres":
        kw["store_error"] = psycopg.errors.QueryCanceled("canceling statement due to statement timeout")
    elif case == "kernel":
        monkeypatch.setattr(Engine, "apply", _kernel_boom)
    elif case == "consumer_fatal":  # вытеснение по group.instance.id: код _FATAL, fatal() == False
        msgs = [kafka_error(KafkaError._FATAL, "Fatal error: Broker: Static consumer fenced by other consumer")]
    elif case == "producer_fatal":  # flush() фатального producer бросает KafkaException, а не зовёт error_cb
        kw["producer_raises"] = ("flush", fatal_producer_error())
    r = run_dispatcher([("assign",), ("msgs", msgs), ("msgs", kafka_msgs([None], 45))], watermarks=(40, 60), **kw)
    assert r.code == dmain.EXIT_FAILURE
    last = {k: r.lines[-1][k] for k in ("event", "level", "site", "epoch", "offset")}
    assert last == {"event": event, "level": "ERROR", "site": "test", "epoch": 7, "offset": 40}
    assert r.consumer.made("close") == [("close",)] and r.store.closed
    assert r.consumer.made("commit") == []  # следующая пачка не обрабатывалась


def not_fatal_producer_error() -> KafkaException:
    return KafkaException(KafkaError(KafkaError.MSG_SIZE_TOO_LARGE, "Unable to produce message: Too large"))


@pytest.mark.parametrize(
    ("method", "make_exc", "event"),
    [
        pytest.param("poll", fatal_producer_error, "exit_kafka_fatal", id="poll"),
        pytest.param(
            "flush", lambda: system_error_from(fatal_producer_error()), "exit_kafka_fatal", id="flush-system-error"
        ),
        pytest.param("produce", produce_after_fatal_error, "exit_kafka_fatal", id="produce"),
        # не всякая KafkaException — фатальная ошибка клиента: прочие остаются исключением со стеком
        pytest.param("produce", not_fatal_producer_error, "exit_crash", id="produce-not-fatal"),
    ],
)
def test_run_producer_exception_exits_1_and_exit_line_names_the_fatal_error(run_dispatcher, method, make_exc, event):
    """Фатальная ошибка producer приходит исключением из poll()/flush()/produce(), а при ожидающем on_delivery —
    SystemError с этой ошибкой в __cause__: строка выхода exit_kafka_fatal называет саму ошибку (а не SystemError)."""
    msgs = kafka_msgs(example(two_stations()), 40)
    r = run_dispatcher(
        [("assign",), ("msgs", msgs), ("msgs", kafka_msgs([None], 45))],
        watermarks=(40, 60),
        producer_raises=(method, make_exc()),
    )
    assert r.code == dmain.EXIT_FAILURE
    last = r.lines[-1]
    got = {k: last[k] for k in ("event", "level", "site", "epoch", "offset")}
    assert got == {"event": event, "level": "ERROR", "site": "test", "epoch": 7, "offset": 40}
    if event == "exit_kafka_fatal":
        assert last["error"].startswith("KafkaException: KafkaError{")
    assert r.consumer.made("close") == [("close",)] and r.store.closed
    assert r.consumer.made("commit") == [] and r.store.checkpoints == []  # пачка не дошла до чекпоинта


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
    ("make_exc", "event"),
    [
        pytest.param(fatal_producer_error, "exit_kafka_fatal", id="fatal-flag"),
        pytest.param(produce_after_fatal_error, "exit_kafka_fatal", id="fatal-code"),
        pytest.param(lambda: system_error_from(fatal_producer_error()), "exit_kafka_fatal", id="system-error-cause"),
        pytest.param(
            lambda: with_context(RuntimeError("cleanup"), fatal_producer_error()), "exit_kafka_fatal", id="context"
        ),
        pytest.param(
            lambda: with_cause(RuntimeError("wrapper"), system_error_from(fatal_producer_error())),
            "exit_kafka_fatal",
            id="nested",
        ),
        pytest.param(not_fatal_producer_error, "exit_crash", id="not-fatal"),
        pytest.param(KafkaException, "exit_crash", id="no-args"),
        pytest.param(lambda: KafkaException("не KafkaError"), "exit_crash", id="not-a-kafka-error"),
        pytest.param(lambda: SystemError("no cause"), "exit_crash", id="system-error-alone"),
        pytest.param(
            lambda: with_cause(SystemError("wrapper"), not_fatal_producer_error()), "exit_crash", id="cause-not-fatal"
        ),
        pytest.param(
            lambda: with_context(RuntimeError("x"), fatal_producer_error(), hidden=True), "exit_crash", id="hidden"
        ),
        pytest.param(cycle, "exit_crash", id="cycle"),
    ],
)
def test_exit_line_finds_a_fatal_kafka_error_in_the_exception_chain(caplog, make_exc, event):
    """Исключение само или его причина (__cause__, иначе не скрытый __context__) — KafkaException с фатальной ошибкой
    клиента (fatal() или код _FATAL): строка exit_kafka_fatal с этой ошибкой в error, код 1. Иначе exit_crash."""
    with caplog.at_level(logging.INFO, logger="vq.dispatcher"):
        code = dmain.exit_line(make_exc(), None)
    (rec,) = [r for r in caplog.records if r.name == "vq.dispatcher"]
    assert (code, rec.getMessage()) == (dmain.EXIT_FAILURE, event)
    if event == "exit_kafka_fatal":
        assert rec.fields["error"].startswith("KafkaException: KafkaError{")


@pytest.mark.parametrize(
    ("kw", "tail", "reason"),
    [
        ({"partitions": 2}, [], "partitions"),
        ({"claim": Claim(2, 500, NOON, Counters(), SiteState(now=NOON))}, [], "log_recreated"),
        ({}, [("msgs", [kafka_error(KafkaError._AUTO_OFFSET_RESET, "offset out of range")])], "state_older_than_log"),
    ],
)
def test_run_refusal_exits_2_with_reason_and_hint(run_dispatcher, kw, tail, reason):
    r = run_dispatcher([("assign",), *tail], **kw)
    assert r.code == dmain.EXIT_REFUSED
    last = r.lines[-1]
    assert (last["event"], last["reason"], last["hint"]) == ("start_refused", reason, "make reset")
    assert r.consumer.made("close") == [("close",)] and r.store.closed and r.store.checkpoints == []


# --- старт: ожидание Postgres, сторож миграции, схема, закрытие -----------------------------------------


def test_run_waits_for_postgres_then_starts(run_dispatcher):
    """Старт при недоступном Postgres ждёт его в процессе (§2.6): без ожидания каждый старт выходил бы за доли
    секунды, Docker удваивал бы паузу перед рестартом (до 60 с), и после возврата Postgres площадка ждала бы
    очередного рестарта. Попытка — раз в PG_RETRY_S, строка waiting_for_postgres на каждую неудачу."""
    cfg = two_stations()
    r = run_dispatcher([("assign",), ("msgs", kafka_msgs(example(cfg), 0))], pg_down=3, watermarks=(0, 5))
    assert r.code == dmain.EXIT_OK and len(r.opened) == 4
    waits = [d for d in r.lines if d["event"] == "waiting_for_postgres"]
    assert [(d["level"], d["attempt"]) for d in waits] == [("WARNING", 1), ("WARNING", 2), ("WARNING", 3)]
    assert waits[0]["error"] == "OperationalError: connection to server failed: Connection refused (1)"
    assert r.events()[3:5] == ["start", "claim"] and r.store.checkpoints[-1][1] == 5


def test_run_gives_up_waiting_for_postgres_and_exits_1(run_dispatcher):
    r = run_dispatcher([("assign",)], pg_down=10**6, pg_wait_s=0.0)
    assert r.code == dmain.EXIT_FAILURE and len(r.opened) == 1
    assert r.events() == ["exit_postgres"] and r.lines[-1]["error"].startswith("OperationalError: ")
    assert r.consumer.conf == {} and not r.store.migrated  # дальше подключения старт не пошёл


def test_sigterm_while_waiting_for_postgres_exits_0(run_dispatcher):
    r = run_dispatcher([("assign",)], pg_down=10**6, sigterm_after=2)
    assert r.code == dmain.EXIT_OK and len(r.opened) == 2
    assert r.events() == ["waiting_for_postgres", "waiting_for_postgres"] and r.consumer.conf == {}


def test_stalled_migrate_exits_1(run_dispatcher):
    """Сторож и на миграции: advisory lock и DDL на замёрзшем Postgres висели бы до разморозки."""
    release = threading.Event()
    exits: list[int] = []

    def fake_exit(code: int) -> None:
        exits.append(code)
        release.set()  # в проде процесса уже нет

    r = run_dispatcher([], migrate_stuck=release, deadline=Deadline(0.05, exit_fn=fake_exit))
    assert exits == [dmain.EXIT_FAILURE]
    line = r.line("exit_postgres_stalled")
    assert (line["op"], line["level"], line["site"], line["deadline_s"]) == ("migrate", "ERROR", "test", 0.05)


def test_schema_mismatch_refuses_start_with_exit_2(run_dispatcher):
    """Схема в Postgres не та, что в коде (том до колонки radius_station): отказ с подсказкой make reset,
    а не UndefinedColumn на claim и выход 1 в цикле рестартов."""
    refused = StartRefused("schema_changed", "vehicles: нет колонок radius_station")
    r = run_dispatcher([("assign",)], migrate_error=refused)
    assert r.code == dmain.EXIT_REFUSED
    last = r.lines[-1]
    assert (last["event"], last["reason"], last["hint"]) == ("start_refused", "schema_changed", "make reset")
    assert r.store.closed and r.consumer.conf == {}


def test_close_failures_are_logged_with_the_error(run_dispatcher):
    r = run_dispatcher(
        [("assign",)],
        partitions=2,
        consumer_close_error=KafkaException(KafkaError(KafkaError._TRANSPORT, "Disconnected")),
        store_close_error=psycopg.OperationalError("server closed the connection unexpectedly"),
    )
    assert r.code == dmain.EXIT_REFUSED
    consumer, store = r.line("consumer_close_failed"), r.line("store_close_failed")
    assert consumer["level"] == store["level"] == "WARNING"
    assert consumer["error"].startswith("KafkaException: ") and "Disconnected" in consumer["error"]
    assert store["error"] == "OperationalError: server closed the connection unexpectedly"


def test_exit_before_the_config_is_read_still_has_site_epoch_offset(run_dispatcher):
    """§2.4: site, epoch и offset — в каждой строке; пока site.toml не прочитан, они null, исключение — в error."""
    r = run_dispatcher([], config_error=ConfigError("assignment: машина T9 без станции"))
    assert r.code == dmain.EXIT_FAILURE and r.events() == ["exit_crash"] and r.opened == []
    line = r.line("exit_crash")
    assert (line["level"], line["site"], line["epoch"], line["offset"]) == ("ERROR", None, None, None)
    assert line["error"].endswith("ConfigError: assignment: машина T9 без станции")


def test_migrations_dir_comes_from_env(run_dispatcher):
    """Путь к SQL — из VQ_MIGRATIONS (в compose /app/migrations); без неё — каталог дерева исходников."""
    r = run_dispatcher([], env={"VQ_MIGRATIONS": "/app/migrations", "VQ_PG_DSN": "postgresql://h/vq"})
    assert r.opened == [("postgresql://h/vq", Path("/app/migrations"))]
    assert run_dispatcher([]).opened == [(dmain.DEFAULT_PG_DSN, MIGRATIONS_DIR)]


def test_sigterm_sets_stop_and_main_exits_with_run_code(monkeypatch):
    """SIGTERM только выставляет stop: цикл доделывает пачку, run закрывает consumer и возвращает 0 (см. тесты run)."""
    seen: dict[str, object] = {}

    installed = JsonFormatter()

    def fake_run(env, formatter, stop: threading.Event) -> int:
        seen["formatter"] = formatter
        os.kill(os.getpid(), signal.SIGTERM)
        seen["stopped"] = stop.wait(5)
        return dmain.EXIT_OK

    monkeypatch.setattr(dmain, "run", fake_run)
    monkeypatch.setattr(dmain, "setup_logging", lambda: installed)  # корневой логгер pytest не трогаем
    monkeypatch.setattr(dmain.logging, "shutdown", lambda: None)
    saved = {s: signal.getsignal(s) for s in (signal.SIGTERM, signal.SIGINT)}
    try:
        with pytest.raises(SystemExit) as exited:
            dmain.main()
    finally:
        for s, handler in saved.items():
            signal.signal(s, handler)
    assert seen["stopped"] is True and exited.value.code == dmain.EXIT_OK
    assert seen["formatter"] is installed  # run() задаёт defaults форматтеру, который пишет в stdout
