"""vq-dispatcher (§2.4): Kafka-клиенты и группа, цикл пачек, метрики, JSON-логи; crash-only."""

import logging
import os
import signal
import sys
import threading
import time
from collections.abc import Mapping
from dataclasses import dataclass
from pathlib import Path
from types import FrameType
from typing import Any

import psycopg
from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition
from prometheus_client import start_http_server

from vq.config import config_hash, load_config
from vq.core.codec import src_headers
from vq.dispatcher.loop import (
    EXIT_FAILURE,
    EXIT_OK,
    EXIT_REFUSED,
    Deadline,
    DeliveryFailed,
    Fenced,
    InMsg,
    Processor,
    chaos_hook,
    log,
)
from vq.dispatcher.metrics import DispatcherMetrics
from vq.dispatcher.store import MIGRATIONS_DIR, PgStore, StartRefused
from vq.logs import JsonFormatter, describe, setup_logging

TELEMETRY_TOPIC = "telemetry.v1"
DEFAULT_KAFKA = "localhost:19092"
DEFAULT_PG_DSN = "postgresql://vq:vq@localhost:5432/vq"
DEFAULT_CONFIG = "config/site.toml"
DEFAULT_METRICS_PORT = 9100
BATCH_SIZE = 500
POLL_TIMEOUT_S = 0.25
WATERMARK_TIMEOUT_S = 5.0
METADATA_TIMEOUT_S = 10.0
SHUTDOWN_FLUSH_S = 10.0
PG_WAIT_S = 60.0  # сколько старт ждёт недоступный Postgres, прежде чем выйти с кодом 1
PG_RETRY_S = 1.0

INFO_ERRORS = frozenset({KafkaError._TRANSPORT, KafkaError._ALL_BROKERS_DOWN, KafkaError._PARTITION_EOF})


class KafkaFatal(Exception):
    """Фатальная ошибка клиента Kafka: consumer — ошибка consume() с кодом _FATAL, error_cb с fatal() — страховка.

    Фатальную ошибку producer confluent-kafka бросает исключением KafkaException: её находит fatal_kafka_exception.
    """


# Сбой → событие строки выхода с кодом 1 (§2.4), первое совпадение. StartRefused — код 2, прочее — exit_crash,
# кроме исключения с фатальной KafkaException в цепочке причин (fatal_kafka_exception): оно — exit_kafka_fatal.
FAILURES: tuple[tuple[type[Exception], str], ...] = (
    (Fenced, "exit_fenced"),
    (DeliveryFailed, "exit_delivery_failed"),
    (psycopg.Error, "exit_postgres"),
    (KafkaFatal, "exit_kafka_fatal"),
)


# --- настройки и чистые проверки ---------------------------------------------


def consumer_config(bootstrap: str, site_id: str, instance_id: str | None = None) -> dict[str, Any]:
    """Настройки consumer строго по §2.4. instance_id — только для standby (dispatcher-b), иначе dispatcher-<site>."""
    return {
        "bootstrap.servers": bootstrap,
        "group.id": "dispatcher",
        "group.instance.id": instance_id or f"dispatcher-{site_id}",
        "session.timeout.ms": 10000,
        "heartbeat.interval.ms": 3000,
        "enable.auto.commit": False,
        "auto.offset.reset": "error",
        "group.protocol": "classic",
        "partition.assignment.strategy": "range",
    }


def producer_config(bootstrap: str) -> dict[str, Any]:
    """Настройки producer строго по §2.4."""
    return {
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 5,
        "compression.type": "zstd",
        "message.timeout.ms": 30000,
    }


def check_single_partition(topic: str, partitions: int | None) -> None:
    """У входного топика ровно одна партиция — иначе нет единого порядка (§2.3, §2.7). None — топика нет."""
    if partitions is None:
        raise StartRefused("partitions", f"топика {topic} нет")
    if partitions != 1:
        raise StartRefused("partitions", f"у {topic} {partitions} партиций, нужна ровно одна")


def classify_error(err: KafkaError) -> str:
    """Что делать с ошибкой из consume(): fatal | offset_reset | info | other.

    Фатальную ошибку consumer (например, FENCED_INSTANCE_ID) librdkafka отдаёт из consume() с кодом _FATAL,
    а confluent-kafka строит такой KafkaError с fatal() == False: проверяется и флаг, и код.
    """
    if err.fatal() or err.code() == KafkaError._FATAL:
        return "fatal"
    if err.code() == KafkaError._AUTO_OFFSET_RESET:
        return "offset_reset"
    if err.code() in INFO_ERRORS:
        return "info"
    return "other"


def fatal_kafka_exception(e: BaseException) -> BaseException | None:
    """KafkaException с фатальной ошибкой клиента (как у classify_error) — сама e или в цепочке её причин; иначе None.

    confluent-kafka 2.15.1 не отдаёт фатальную ошибку producer в error_cb, а бросает из вызова, который обслуживал
    клиента: poll() и flush() — KafkaException с fatal() == True, produce() после этого — с кодом _FATAL, а flush()
    при ожидающем on_delivery — SystemError, у которого эта KafkaException в __cause__ и __context__. Цепочка идёт
    так, как её показывает traceback: __cause__, иначе __context__, если его не скрыл `raise … from None`.
    """
    seen: set[int] = set()
    cur: BaseException | None = e
    while cur is not None and id(cur) not in seen:
        seen.add(id(cur))
        err = cur.args[0] if isinstance(cur, KafkaException) and cur.args else None
        if isinstance(err, KafkaError) and classify_error(err) == "fatal":
            return cur
        if cur.__cause__ is not None:
            cur = cur.__cause__
        else:
            cur = None if cur.__suppress_context__ else cur.__context__
    return None


def offset_reset_reason(expected: int | None, low: int, high: int) -> str:
    """Причина отказа при ERR__AUTO_OFFSET_RESET: offset позади лога или лог создан заново."""
    if expected is not None and expected > high >= 0:
        return "log_recreated"
    return "state_older_than_log"


# --- Kafka -------------------------------------------------------------------


class KafkaSink:
    """Sink поверх confluent_kafka.Producer: заголовок vq-src-offset, первая ошибка доставки — в failed."""

    def __init__(self, producer: Any) -> None:
        self._producer = producer
        self.failed: str | None = None

    def on_delivery(self, err: KafkaError | None, msg: Message) -> None:
        if err is not None and self.failed is None:
            self.failed = f"{msg.topic()}: {err.str()}"

    def send(self, topic: str, key: bytes, value: bytes, src_offset: int) -> None:
        headers = src_headers(src_offset)
        while True:
            try:
                self._producer.produce(topic, value=value, key=key, headers=headers, on_delivery=self.on_delivery)
                break
            except BufferError:  # локальная очередь полна: обслужить отчёты о доставке и повторить
                self._producer.poll(0.1)
        self._producer.poll(0)

    def flush(self, timeout: float) -> int:
        return int(self._producer.flush(timeout))


def partition_count(consumer: Consumer, topic: str) -> int | None:
    md = consumer.list_topics(topic, timeout=METADATA_TIMEOUT_S)
    t = md.topics.get(topic)
    if t is None:
        return None
    if t.error is not None:
        if t.error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
            return None
        raise KafkaException(t.error)
    return len(t.partitions)


# --- логи ----------------------------------------------------------------------


def log_defaults(site: str | None) -> dict[str, object]:
    """Поля §2.4 в каждой строке лога: site площадки; epoch и offset задают строки процессора, у прочих — null."""
    return {"site": site, "epoch": None, "offset": None}


def _event(level: int, event: str, processor: Processor | None = None, **fields: object) -> None:
    if processor is not None:
        processor.log(level, event, **fields)
    else:
        log.log(level, event, extra={"fields": fields})


def on_kafka_error(err: KafkaError, processor: Processor | None) -> None:
    """error_cb обоих клиентов: ошибка — в лог; фатальная — KafkaFatal (выход 1, §2.4).

    Исключение из error_cb confluent-kafka пробрасывает из вызова, который обслуживал колбэк (poll, flush,
    consume). Сюда приходят нефатальные ошибки: фатальную ошибку producer confluent-kafka 2.15.1 в error_cb не
    отдаёт, а бросает KafkaException (её находит fatal_kafka_exception), фатальную ошибку consumer отдаёт consume()
    с кодом _FATAL (classify_error). Ветка fatal — страховка на случай, если клиент начнёт отдавать их сюда.
    """
    fatal = err.fatal()
    _event(
        logging.ERROR if fatal else logging.WARNING,
        "kafka_error",
        processor,
        code=err.name(),
        error=err.str(),
        fatal=fatal,
    )
    if fatal:
        raise KafkaFatal(err.str())


# --- цикл ----------------------------------------------------------------------


def _handle_error(err: KafkaError, consumer: Consumer, processor: Processor) -> None:
    kind = classify_error(err)
    if kind == "fatal":
        raise KafkaFatal(err.str())
    if kind == "offset_reset":
        expected = processor.expected
        try:
            low, high = consumer.get_watermark_offsets(
                TopicPartition(TELEMETRY_TOPIC, 0), timeout=WATERMARK_TIMEOUT_S, cached=False
            )
        except KafkaException:
            low, high = -1, -1
        raise StartRefused(offset_reset_reason(expected, low, high), f"{err.str()}; watermarks [{low}, {high}]")
    level = logging.INFO if kind == "info" else logging.WARNING
    processor.log(level, "kafka_error", code=err.name(), error=err.str())


def consume_loop(consumer: Consumer, processor: Processor, metrics: DispatcherMetrics, stop: threading.Event) -> None:
    tp = TopicPartition(TELEMETRY_TOPIC, 0)
    while not stop.is_set():
        batch: list[InMsg] = []
        for m in consumer.consume(num_messages=BATCH_SIZE, timeout=POLL_TIMEOUT_S):
            err = m.error()
            if err is not None:
                _handle_error(err, consumer, processor)
                continue
            offset = m.offset()
            if offset is not None:
                batch.append(InMsg(offset, m.value()))
        if not batch:
            continue
        seek_to = processor.handle_batch(batch)
        if seek_to is not None:
            consumer.seek(TopicPartition(TELEMETRY_TOPIC, 0, seek_to))
            continue
        nxt = processor.expected
        if not processor.assigned or nxt is None:  # партиции нет
            continue
        try:  # commit — только для лага в Console и метриках; источник правды — Postgres
            consumer.commit(offsets=[TopicPartition(TELEMETRY_TOPIC, 0, nxt)], asynchronous=True)
        except KafkaException as e:
            processor.log(logging.WARNING, "commit_failed", error=str(e))
        try:
            _, high = consumer.get_watermark_offsets(tp, cached=True)
        except KafkaException:
            continue
        if high >= 0:
            metrics.observe_lag(high - nxt)


# --- старт и выход ---------------------------------------------------------------


def open_store(dsn: str, migrations: Path, stop: threading.Event) -> PgStore | None:
    """Подключиться к Postgres, ожидая его до PG_WAIT_S (§2.6); None — SIGTERM во время ожидания.

    Без ожидания старт при недоступном Postgres выходит за доли секунды, Docker удваивает паузу перед следующим
    рестартом (до 60 с), и после возврата Postgres площадка ждала бы очередного рестарта. В памяти ещё ничего нет,
    поэтому ожидание остаётся crash-only.
    """
    give_up = time.monotonic() + PG_WAIT_S
    attempt = 0
    while True:
        try:
            return PgStore(dsn, migrations_dir=migrations)
        except psycopg.OperationalError as e:
            attempt += 1
            if time.monotonic() >= give_up:
                raise
            _event(logging.WARNING, "waiting_for_postgres", attempt=attempt, error=describe(e))
        if stop.wait(PG_RETRY_S):
            return None


def subscribe(consumer: Consumer, proc: Processor) -> None:
    """Подписка с колбэками группы (§2.4): назначение — claim и assign с явным offset, потеря — сброс ядра."""

    def on_assign(c: Consumer, partitions: list[TopicPartition]) -> None:
        if not partitions:  # в группе больше участников, чем партиций
            return
        low, high = c.get_watermark_offsets(
            TopicPartition(TELEMETRY_TOPIC, 0), timeout=WATERMARK_TIMEOUT_S, cached=False
        )
        offset = proc.on_assign(low, high)
        # клиент не берёт offset из изменённого списка partitions: назначаем явно
        c.assign([TopicPartition(TELEMETRY_TOPIC, 0, offset)])

    def on_revoke(c: Consumer, partitions: list[TopicPartition]) -> None:
        proc.on_revoke("revoke")

    def on_lost(c: Consumer, partitions: list[TopicPartition]) -> None:
        proc.on_revoke("lost")

    consumer.subscribe([TELEMETRY_TOPIC], on_assign=on_assign, on_revoke=on_revoke, on_lost=on_lost)


@dataclass
class Opened:
    """Что успел открыть старт: run() закрывает это при любом выходе."""

    store: PgStore | None = None
    consumer: Consumer | None = None
    processor: Processor | None = None

    def close(self) -> None:
        """Перед выходом 1 тоже вызывается consumer.close() (§2.4); ошибка закрытия — только строка лога."""
        if self.consumer is not None:
            try:
                self.consumer.close()
            except Exception as e:
                _event(logging.WARNING, "consumer_close_failed", self.processor, error=describe(e))
        if self.store is not None:
            try:
                self.store.close()
            except Exception as e:
                _event(logging.WARNING, "store_close_failed", self.processor, error=describe(e))


def serve(env: Mapping[str, str], formatter: JsonFormatter, stop: threading.Event, opened: Opened) -> int:
    """Старт по §2.4 и цикл до SIGTERM; всё открытое — в opened."""
    cfg_path = env.get("VQ_CONFIG", DEFAULT_CONFIG)
    cfg = load_config(cfg_path)
    chash = config_hash(cfg_path)
    formatter.defaults = log_defaults(cfg.site_id)
    bootstrap = env.get("VQ_KAFKA", DEFAULT_KAFKA)
    migrations = Path(env.get("VQ_MIGRATIONS") or MIGRATIONS_DIR)
    store = opened.store = open_store(env.get("VQ_PG_DSN", DEFAULT_PG_DSN), migrations, stop)
    if store is None:  # SIGTERM, пока ждали Postgres
        return EXIT_OK
    deadline = Deadline()
    with deadline.guard("migrate", {}):
        store.migrate()
    metrics = DispatcherMetrics(stations=[s.uuid for s in cfg.stations])
    start_http_server(int(env.get("VQ_METRICS_PORT") or DEFAULT_METRICS_PORT))

    def on_error(err: KafkaError) -> None:
        on_kafka_error(err, opened.processor)

    common = {"error_cb": on_error, "logger": logging.getLogger("vq.kafka")}
    sink = KafkaSink(Producer({**producer_config(bootstrap), **common}))
    crash = chaos_hook(env.get("VQ_CRASH_FILE"))
    proc = opened.processor = Processor(cfg, chash, store, sink, metrics, crash, deadline)
    consumer_cfg = consumer_config(bootstrap, cfg.site_id, env.get("VQ_GROUP_INSTANCE_ID"))
    consumer = opened.consumer = Consumer({**consumer_cfg, **common})
    check_single_partition(TELEMETRY_TOPIC, partition_count(consumer, TELEMETRY_TOPIC))
    subscribe(consumer, proc)
    _event(logging.INFO, "start", proc, config=cfg_path, config_hash=chash, kafka=bootstrap)
    consume_loop(consumer, proc, metrics, stop)
    _event(logging.INFO, "stop", proc)
    opened.consumer = None
    consumer.close()
    left = sink.flush(SHUTDOWN_FLUSH_S)
    if left:
        _event(logging.WARNING, "shutdown_unflushed", proc, messages=left)
    return EXIT_OK


def exit_line(e: Exception, processor: Processor | None) -> int:
    """Строка выхода и код (§2.4): отказ старта — 2 с причиной и подсказкой, любой сбой — 1."""
    if isinstance(e, StartRefused):
        _event(logging.ERROR, "start_refused", processor, reason=e.reason, detail=e.detail, hint=e.hint)
        return EXIT_REFUSED
    event = next((ev for kind, ev in FAILURES if isinstance(e, kind)), None)
    cause: BaseException = e
    if event is None and (fatal := fatal_kafka_exception(e)) is not None:
        event, cause = "exit_kafka_fatal", fatal  # в error — сама фатальная ошибка, а не SystemError вокруг неё
    if event is None:  # в том числе исключение ядра: со стеком
        log.error("exit_crash", exc_info=e, extra={"fields": {} if processor is None else processor.context()})
    else:
        _event(logging.ERROR, event, processor, error=describe(cause))
    return EXIT_FAILURE


def run(env: Mapping[str, str], formatter: JsonFormatter, stop: threading.Event) -> int:
    """Код выхода: 0 — SIGTERM, 1 — сбой (crash-only: контейнер перезапустится), 2 — отказ старта (make reset)."""
    formatter.defaults = log_defaults(None)  # site — после чтения site.toml (serve)
    opened = Opened()
    try:
        return serve(env, formatter, stop, opened)
    except Exception as e:
        return exit_line(e, opened.processor)
    finally:
        opened.close()


def main() -> None:
    formatter = setup_logging()
    stop = threading.Event()

    def on_signal(signum: int, frame: FrameType | None) -> None:
        stop.set()

    signal.signal(signal.SIGTERM, on_signal)
    signal.signal(signal.SIGINT, on_signal)
    code = run(os.environ, formatter, stop)
    logging.shutdown()
    sys.exit(code)
