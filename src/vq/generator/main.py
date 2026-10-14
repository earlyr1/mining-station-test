"""Процесс генератора (§4.3, §4.4): симуляция → инжектор сбоев → telemetry.v1; рекомендации из decision.v1.

Чистые части (разбор аргументов, темп, часы старта, итог, основной цикл) проверяются тестами на подделках;
клиенты confluent-kafka создаются в main() и передаются в run() фабриками.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import queue
import signal
import sys
import threading
import time
from collections.abc import Callable, Container, Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from types import FrameType
from typing import Any

from confluent_kafka import Consumer, KafkaError, KafkaException, Message, Producer, TopicPartition
from prometheus_client import start_http_server

from vq.config import SiteConfig, load_config
from vq.core.codec import Telemetry, encode_telemetry, parse_telemetry
from vq.core.decision import DECISION_TOPIC
from vq.generator.faults import FaultInjector
from vq.generator.metrics import FAULT_TYPES, IGNORE_REASONS, GeneratorMetrics
from vq.generator.sim import DecisionStats, Sim
from vq.logs import describe, setup_logging

TELEMETRY_TOPIC = "telemetry.v1"
DIRTY_TAIL = 1000  # сколько последних записей читать при --allow-dirty
KAFKA_TIMEOUT_S = 10.0
FLUSH_TIMEOUT_S = 30.0
PROGRESS_EVERY_S = 30.0
EXIT_OK, EXIT_FAIL = 0, 1

log = logging.getLogger("vq.generator")


# --- аргументы -----------------------------------------------------------


def parse_speed(value: str) -> float | None:
    """'max' → None (без пауз), иначе положительное конечное число."""
    if value.strip().lower() == "max":
        return None
    try:
        speed = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"--speed: ожидается число или max, получено {value!r}") from None
    if not math.isfinite(speed) or speed <= 0:
        raise argparse.ArgumentTypeError(f"--speed: нужно положительное число, получено {value!r}")
    return speed


def _rate(value: str) -> float:
    try:
        rate = float(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"ожидается доля от 0 до 1, получено {value!r}") from None
    if not 0.0 <= rate <= 1.0:
        raise argparse.ArgumentTypeError(f"ожидается доля от 0 до 1, получено {value!r}")
    return rate


def _positive_int(value: str) -> int:
    try:
        n = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"ожидается целое число, получено {value!r}") from None
    if n <= 0:
        raise argparse.ArgumentTypeError(f"ожидается положительное число, получено {value!r}")
    return n


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(prog="vq-generator", description="Симулятор площадки: пишет telemetry.v1")
    p.add_argument("--speed", type=parse_speed, default=1.0, help="темп: 1 — реальный, N — ускорение, max — без пауз")
    p.add_argument("--duration", type=_positive_int, default=None, help="длительность в секундах симуляции")
    p.add_argument("--seed", type=int, default=42)
    p.add_argument("--dup-rate", type=_rate, default=0.005)
    p.add_argument("--late-rate", type=_rate, default=0.002)
    p.add_argument("--outage-rate", type=_rate, default=0.0005)
    p.add_argument("--no-decisions", action="store_true", help="не читать decision.v1 (датасеты для verify и bench)")
    p.add_argument("--allow-dirty", action="store_true", help="стартовать на непустом telemetry.v1 (только демо)")
    p.add_argument("--summary", type=Path, default=Path("/out/summary.json"), help="куда записать итог")
    return p


def check_args(args: argparse.Namespace) -> str | None:
    """Сочетания флагов, которые argparse сам не проверит. Возвращает текст ошибки или None."""
    if args.speed is None and not args.no_decisions:
        return (
            "--speed max требует --no-decisions: без пауз рекомендации не успевают вернуться, "
            "и это уже не замкнутый контур (§4.3)"
        )
    return None


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = build_parser()
    args = parser.parse_args(argv)
    error = check_args(args)
    if error is not None:
        parser.error(error)
    return args


# --- темп, часы старта, итог ---------------------------------------------


@dataclass(frozen=True)
class Pacer:
    """Тик k уходит в момент start_wall + k / speed по часам time.monotonic; speed None — без пауз."""

    start_wall: float
    speed: float | None

    def due(self, k: int) -> float | None:
        # Считается заново для каждого k, без накопления ошибки от суммы пауз.
        return None if self.speed is None else self.start_wall + k / self.speed


def start_clock(wall_now: float, tail: Iterable[bytes | None] | None, units: Container[str]) -> int:
    """Часы симуляции на старте (§4.4).

    Без хвоста (пустой топик) — текущее время. Если хвост есть (--allow-dirty) — не раньше max ts + 1 по последним
    записям: в хвосте лежат дубли и опоздания прежних ts, поэтому берётся максимум, не последняя запись.
    Невалидные записи пропускаются тем же разбором, что и в ядре.
    """
    start = int(wall_now)
    if tail is None:
        return start
    stamps = [m.ts for raw in tail if (m := parse_telemetry(raw, units)) is not None]
    return max(start, max(stamps) + 1) if stamps else start


def build_summary(
    *,
    produced: int,
    faults: Mapping[str, int],
    expected_drops: int,
    stats: DecisionStats,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Итог прогона (§4.4). Известные типы сбоев и причины отказов присутствуют всегда, хотя бы нулём."""
    fault_counts = {kind: 0 for kind in FAULT_TYPES}
    fault_counts.update({kind: int(n) for kind, n in sorted(faults.items())})
    ignored = {reason: 0 for reason in IGNORE_REASONS}
    ignored.update({reason: int(n) for reason, n in sorted(stats.ignored.items())})
    summary: dict[str, Any] = {
        "produced": produced,
        "faults": fault_counts,
        "expected_drops": expected_drops,
        "decisions": {
            "received": stats.received,
            "applied": stats.applied,
            "ignored": ignored,
            "duplicates": stats.duplicates,
        },
    }
    summary.update(extra or {})
    return summary


def prepare_summary_path(path: Path) -> None:
    """Каталог итога создаётся до старта: ошибка прав видна сразу, не через час прогона."""
    path.parent.mkdir(parents=True, exist_ok=True)
    if not os.access(path.parent, os.W_OK):
        raise PermissionError(f"нет прав на запись в {path.parent}")


def write_summary(path: Path, summary: Mapping[str, Any]) -> None:
    """Итог — в файл атомарной заменой (в stdout его пишет строка лога summary)."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    tmp.replace(path)


# --- основной цикл -------------------------------------------------------


class StopFlag:
    """Флаг остановки. Обработчик сигнала только присваивает поле: никаких блокировок внутри обработчика."""

    def __init__(self) -> None:
        self.reason: str | None = None

    def set(self, reason: str) -> None:
        if self.reason is None:
            self.reason = reason

    def __bool__(self) -> bool:
        return self.reason is not None


def wait_until(due: float, stop: StopFlag, clock: Callable[[], float], sleep: Callable[[float], None]) -> bool:
    """Ждёт момента due короткими паузами, чтобы сигнал не ждал конца секунды. False — пришла остановка."""
    while not stop:
        rest = due - clock()
        if rest <= 0:
            return True
        sleep(min(rest, 0.1))
    return False


@dataclass
class LoopResult:
    """Ход цикла. Обновляется на месте: если цикл оборвало исключение, видно, сколько тиков успело уйти."""

    ticks: int = 0
    last_ts: int | None = None


def run_loop(
    sim: Sim,
    injector: FaultInjector,
    sink: KafkaSink,
    pacer: Pacer,
    *,
    duration: int | None,
    stop: StopFlag,
    decisions: Callable[[], list[bytes]] | None = None,
    on_tick: Callable[[int, int], None] | None = None,
    clock: Callable[[], float] = time.monotonic,
    sleep: Callable[[float], None] = time.sleep,
    progress: LoopResult | None = None,
) -> LoopResult:
    """Тики до конца duration или до остановки. Перед каждым тиком в симулятор уходят полученные решения:
    мусор он считает сам (ignored.invalid) и не бросает.

    progress — куда писать ход цикла (по умолчанию новый LoopResult); он же и возвращается.
    """
    res = LoopResult() if progress is None else progress
    while duration is None or res.ticks < duration:
        if stop:
            break
        due = pacer.due(res.ticks)
        if due is not None and not wait_until(due, stop, clock, sleep):
            break
        if decisions is not None:
            for raw in decisions():
                sim.offer_decision(raw)
        t = sim.t
        msgs = sim.tick()
        for msg in injector.push(t, msgs):
            sink.send(msg)
        sink.poll()
        res.last_ts = t
        res.ticks += 1
        if on_tick is not None:
            on_tick(res.ticks, t)
        if sink.fatal is not None:
            stop.set("kafka_fatal")
    return res


def finish(injector: FaultInjector, sink: KafkaSink, flush_timeout: float = FLUSH_TIMEOUT_S) -> int:
    """Досылает отложенное инжектором (дубли, опоздания, обрывы) по порядку и ждёт доставки. Возвращает остаток."""
    for msg in injector.drain():
        sink.send(msg)
    return sink.flush(flush_timeout)


# --- Kafka ---------------------------------------------------------------


class KafkaSink:
    """Продюсер telemetry.v1: ключ unit_uuid, значение encode_telemetry; при переполнении очереди — poll и повтор."""

    def __init__(
        self, make_producer: Callable[[dict[str, Any]], Producer], conf: Mapping[str, Any], topic: str = TELEMETRY_TOPIC
    ) -> None:
        self._topic = topic
        self.produced = 0  # отдано продюсеру
        self.delivered = 0
        self.delivery_failed = 0
        self.last_delivery_error: str | None = None
        self.fatal: str | None = None  # фатальная ошибка продюсера: цикл останавливается
        self._producer = make_producer({**conf, "error_cb": self.on_error})

    def send(self, msg: Telemetry) -> None:
        key, value = msg.unit_uuid.encode(), encode_telemetry(msg)
        while True:
            try:
                self._producer.produce(self._topic, key=key, value=value, on_delivery=self.on_delivery)
                break
            except BufferError:
                # Локальная очередь librdkafka полна (так бывает при --speed max): отдаём время на доставку.
                self._producer.poll(0.1)
            except KafkaException as e:
                # Продюсер в фатальном состоянии: сообщение не уйдёт, цикл остановится по fatal.
                self.delivery_failed += 1
                if self.fatal is None:
                    self.fatal = str(e)
                    log.error("produce_failed", extra={"fields": {"error": str(e)}})
                return
        self.produced += 1

    def poll(self) -> None:
        self._producer.poll(0)

    def flush(self, timeout: float) -> int:
        return int(self._producer.flush(timeout))

    def on_error(self, err: KafkaError) -> None:
        """error_cb продюсера: фатальная ошибка останавливает генератор, остальное librdkafka переживёт сам."""
        if err.fatal():
            self.fatal = str(err)
            log.error("kafka_fatal", extra={"fields": {"error": str(err)}})
        else:
            log.warning("kafka_error", extra={"fields": {"error": str(err)}})

    def on_delivery(self, err: KafkaError | None, _msg: Message | None) -> None:
        """Отчёт о доставке (librdkafka вызывает его из poll и flush)."""
        if err is None:
            self.delivered += 1
            return
        self.delivery_failed += 1
        if self.last_delivery_error is None:
            log.error("delivery_failed", extra={"fields": {"error": str(err)}})
        self.last_delivery_error = str(err)


class DecisionFeed:
    """Фоновый поток чтения decision.v1 (§4.3).

    Партиция 0 назначается вручную (assign, без групповых коммитов) на high watermark, измеренный до первого
    тика: решения прошлых прогонов не исполняются. Сырые значения складываются в queue.Queue; основной цикл
    забирает их через drain() перед каждым тиком.
    """

    def __init__(self, consumer: Consumer, topic: str = DECISION_TOPIC, poll_timeout: float = 0.2) -> None:
        self._consumer = consumer
        self._topic = topic
        self._poll_timeout = poll_timeout
        self._queue: queue.Queue[bytes] = queue.Queue()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self.start_offset: int | None = None

    def start(self, timeout: float = KAFKA_TIMEOUT_S) -> int:
        _, high = self._consumer.get_watermark_offsets(TopicPartition(self._topic, 0), timeout=timeout, cached=False)
        self._consumer.assign([TopicPartition(self._topic, 0, high)])
        self.start_offset = int(high)
        self._thread = threading.Thread(target=self._run, name="vq-decisions", daemon=True)
        self._thread.start()
        return self.start_offset

    def _run(self) -> None:
        errors = 0
        while not self._stop.is_set():
            try:
                # poll отдаёт запись, как только она пришла. consume(num_messages=N, timeout) в librdkafka ждёт
                # N записей или весь таймаут: одиночное решение лежало бы здесь до timeout, а при ускорении xN
                # это ещё N x timeout секунд симуляции сверх задержки самого dispatcher (§4.3).
                m = self._consumer.poll(self._poll_timeout)
            except (KafkaException, RuntimeError) as e:  # поток не должен умирать молча
                log.warning("decisions_consume_failed", extra={"fields": {"error": describe(e)}})
                self._stop.wait(1.0)
                continue
            if m is None:
                continue
            if m.error() is not None:
                errors += 1
                if errors <= 5 or errors % 100 == 0:
                    log.warning("decisions_error", extra={"fields": {"error": str(m.error()), "count": errors}})
                continue
            value = m.value()
            if value is not None:
                self._queue.put(value)

    def drain(self) -> list[bytes]:
        out: list[bytes] = []
        while True:
            try:
                out.append(self._queue.get_nowait())
            except queue.Empty:
                return out

    def stop(self, timeout: float = 2.0) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout)
            if self._thread.is_alive():
                # close() рядом с poll() этого потока — неопределённое поведение librdkafka; клиент закроет выход.
                log.warning("decisions_thread_alive", extra={"fields": {"waited_s": timeout}})
                return
        try:
            self._consumer.close()
        except (KafkaException, RuntimeError) as e:
            log.warning("decisions_close_failed", extra={"fields": {"error": describe(e)}})


class DirtyTopic(Exception):
    """telemetry.v1 не пуст, но --allow-dirty не задан."""


def read_tail(
    consumer: Consumer, topic: str, n: int, timeout: float = KAFKA_TIMEOUT_S
) -> tuple[int, int, list[bytes | None]]:
    """Водяные знаки партиции 0 и значения последних n записей (не больше). Если не дочитали за timeout — ошибка."""
    low, high = consumer.get_watermark_offsets(TopicPartition(topic, 0), timeout=timeout, cached=False)
    low, high = int(low), int(high)
    if high <= low:
        return low, high, []
    consumer.assign([TopicPartition(topic, 0, max(low, high - n))])
    values: list[bytes | None] = []
    deadline = time.monotonic() + timeout
    try:
        while time.monotonic() < deadline:
            for m in consumer.consume(num_messages=n, timeout=0.5):
                if m.error() is not None:
                    continue
                values.append(m.value())
                if (m.offset() or 0) >= high - 1:
                    return low, high, values
    finally:
        consumer.unassign()
    raise TimeoutError(f"не удалось дочитать хвост {topic} до offset {high - 1} за {timeout} сек")


def check_topic(consumer: Consumer, units: Container[str], allow_dirty: bool, wall_now: float) -> tuple[int, int]:
    """Проверка пустого telemetry.v1 (§4.4); возвращает (start_ts, high watermark).

    Пустым считается топик, в который ещё ничего не писали (high watermark = 0): если retention уже удалил
    записи, состояние dispatcher от них всё равно осталось.
    """
    if not allow_dirty:
        low, high = consumer.get_watermark_offsets(
            TopicPartition(TELEMETRY_TOPIC, 0), timeout=KAFKA_TIMEOUT_S, cached=False
        )
        if int(high) > 0:
            raise DirtyTopic(
                f"{TELEMETRY_TOPIC} не пуст (offsets {low}..{high}): новая смена противоречила бы фазам в dispatcher. "
                "Сначала make reset; для демо — --allow-dirty"
            )
        return start_clock(wall_now, None, units), int(high)
    _, high, tail = read_tail(consumer, TELEMETRY_TOPIC, DIRTY_TAIL)
    start_ts = start_clock(wall_now, tail if high > 0 else None, units)
    if high > 0:
        # §4.4: --allow-dirty — только для демо, с предупреждением: модель dispatcher сойдётся примерно за цикл.
        log.warning("dirty_start", extra={"fields": {"high": high, "start_ts": start_ts, "hint": "make reset"}})
    return start_ts, high


def _consumer_conf(bootstrap: str) -> dict[str, Any]:
    return {
        "bootstrap.servers": bootstrap,
        "group.id": "vq-generator",
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "latest",
        "client.id": "vq-generator",
    }


def producer_conf(bootstrap: str) -> dict[str, Any]:
    """Идемпотентный продюсер и acks=all: ретраи не переставят сообщения и не сломают счёт ожидаемых отбросов."""
    return {
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,
        "acks": "all",
        "linger.ms": 5,
        "compression.type": "zstd",
        "client.id": "vq-generator",
    }


# --- точка входа ---------------------------------------------------------


def main(argv: Sequence[str] | None = None) -> int:
    args = parse_args(argv)
    setup_logging()
    bootstrap = os.environ.get("VQ_KAFKA", "localhost:19092")
    cfg_path = os.environ.get("VQ_CONFIG", "config/site.toml")
    metrics_port = int(os.environ.get("VQ_METRICS_PORT", "9101"))
    try:
        cfg = load_config(cfg_path)
        prepare_summary_path(args.summary)
    except (OSError, ValueError) as e:
        log.error("start_failed", extra={"fields": {"error": str(e)}})
        return EXIT_FAIL

    probe = Consumer(_consumer_conf(bootstrap))
    try:
        start_ts, _ = check_topic(probe, cfg.assignment, args.allow_dirty, time.time())
    except DirtyTopic as e:
        log.error("refused_dirty_topic", extra={"fields": {"error": str(e), "hint": "make reset"}})
        return EXIT_FAIL
    except (KafkaException, TimeoutError) as e:
        log.error("kafka_unavailable", extra={"fields": {"error": str(e), "bootstrap": bootstrap}})
        return EXIT_FAIL
    finally:
        probe.close()

    metrics = GeneratorMetrics()
    start_http_server(metrics_port, registry=metrics.registry)
    return run(args, cfg, bootstrap, start_ts, metrics, Producer, Consumer)


def install_signal_handlers(stop: StopFlag) -> dict[signal.Signals, Any]:
    """SIGTERM/SIGINT только выставляют флаг остановки. Возвращает прежние обработчики."""

    def on_signal(signum: int, _frame: FrameType | None) -> None:
        stop.set(signal.Signals(signum).name)

    return {sig: signal.signal(sig, on_signal) for sig in (signal.SIGTERM, signal.SIGINT)}


def restore_signal_handlers(previous: Mapping[signal.Signals, Any]) -> None:
    for sig, handler in previous.items():
        if handler is not None:  # None — обработчик ставился не из Python, вернуть его нельзя
            signal.signal(sig, handler)


def run(
    args: argparse.Namespace,
    cfg: SiteConfig,
    bootstrap: str,
    start_ts: int,
    metrics: GeneratorMetrics,
    make_producer: Callable[[dict[str, Any]], Producer],
    make_consumer: Callable[[dict[str, Any]], Consumer],
    *,
    sim: Sim | None = None,
    injector: FaultInjector | None = None,
) -> int:
    """Собирает симулятор, инжектор и клиентов Kafka, крутит цикл, досылает отложенное и пишет итог.

    sim и injector — подмена для тестов; по умолчанию они строятся из аргументов.
    """
    # Часы берутся как можно позже: в реальном темпе ts ближе к часам, и условие --allow-dirty сохраняется.
    start_ts = max(start_ts, int(time.time()))
    if sim is None:
        sim = Sim(cfg, args.seed, start_ts)
    if injector is None:
        injector = FaultInjector(
            cfg, args.seed, dup_rate=args.dup_rate, late_rate=args.late_rate, outage_rate=args.outage_rate
        )
    sink = KafkaSink(make_producer, producer_conf(bootstrap))
    feed: DecisionFeed | None = None
    if not args.no_decisions:
        feed = DecisionFeed(make_consumer(_consumer_conf(bootstrap)))
        try:
            feed.start()  # assign на high watermark — до первого тика
        except KafkaException as e:  # без decision.v1 замкнутого контура нет
            log.error("decisions_unavailable", extra={"fields": {"error": describe(e)}})
            feed.stop()
            return EXIT_FAIL

    stop = StopFlag()
    previous = install_signal_handlers(stop)
    try:
        progress, remaining, error = _tick_until_stop(args, cfg, start_ts, metrics, sim, injector, sink, feed, stop)
    finally:
        restore_signal_handlers(previous)
    sync_metrics(metrics, sim, injector, sink, start_ts if progress.last_ts is None else progress.last_ts)
    return _report(args, start_ts, sim, injector, sink, progress, remaining, error, stop.reason or "duration")


def sync_metrics(metrics: GeneratorMetrics, sim: Sim, injector: FaultInjector, sink: KafkaSink, sim_time: int) -> None:
    """Переносит счётчики симулятора, инжектора и продюсера в метрики генератора (§4.6)."""
    st = sim.stats
    metrics.sync(
        produced=sink.produced,
        faults=injector.faults,
        expected_drops=injector.expected_drops,
        received=st.received,
        applied=st.applied,
        ignored=st.ignored,
        delays=st.delays,
        sim_time=sim_time,
    )


def _tick_until_stop(
    args: argparse.Namespace,
    cfg: SiteConfig,
    start_ts: int,
    metrics: GeneratorMetrics,
    sim: Sim,
    injector: FaultInjector,
    sink: KafkaSink,
    feed: DecisionFeed | None,
    stop: StopFlag,
) -> tuple[LoopResult, int, str | None]:
    """Тики до конца --duration или до сигнала, затем досылка отложенного и flush.

    Возвращает ход цикла, число недоставленных сообщений и текст ошибки. Исключение в цикле или в досылке не
    обрывает процесс трассировкой: уже отправленное доставляется, итог пишется с error (код выхода 1).
    """
    fields = {
        "site": cfg.site_id,
        "start_ts": start_ts,
        "speed": "max" if args.speed is None else args.speed,
        "duration": args.duration,
        "seed": args.seed,
        "decisions": feed is not None,
        "decisions_from": None if feed is None else feed.start_offset,
    }
    log.info("start", extra={"fields": fields})
    progress = LoopResult()
    last_progress = time.monotonic()

    def on_tick(k: int, t: int) -> None:
        nonlocal last_progress
        sync_metrics(metrics, sim, injector, sink, t)
        now = time.monotonic()
        if now - last_progress >= PROGRESS_EVERY_S:
            last_progress = now
            line = {"ticks": k, "sim_ts": t, "produced": sink.produced, "applied": sim.stats.applied}
            log.info("progress", extra={"fields": line})

    error: str | None = None
    try:
        run_loop(
            sim,
            injector,
            sink,
            Pacer(time.monotonic(), args.speed),
            duration=args.duration,
            stop=stop,
            decisions=None if feed is None else feed.drain,
            on_tick=on_tick,
            progress=progress,
        )
        remaining = finish(injector, sink)
    except Exception as e:  # сбой симулятора, инжектора или продюсера: итог важнее трассировки
        error = describe(e)
        stop.set("error")
        log.exception("run_failed", extra={"fields": {"ticks": progress.ticks}})
        remaining = sink.flush(FLUSH_TIMEOUT_S)
    finally:
        if feed is not None:
            feed.stop()
    return progress, remaining, error


def _report(
    args: argparse.Namespace,
    start_ts: int,
    sim: Sim,
    injector: FaultInjector,
    sink: KafkaSink,
    progress: LoopResult,
    remaining: int,
    error: str | None,
    stopped_by: str,
) -> int:
    """Итог прогона: строкой лога summary в stdout и в файл; код выхода 1, если был сбой или не всё доставлено."""
    summary = build_summary(
        produced=sink.produced,
        faults=injector.faults,
        expected_drops=injector.expected_drops,
        stats=sim.stats,
        extra={
            "ticks": progress.ticks,
            "start_ts": start_ts,
            "last_ts": progress.last_ts,
            "seed": args.seed,
            "speed": "max" if args.speed is None else args.speed,
            "stopped_by": stopped_by,
            "delivery_failed": sink.delivery_failed + remaining,
            "error": error,
        },
    )
    log.info("summary", extra={"fields": {"summary": summary}})  # до файла: итог виден, даже если файл не запишется
    try:
        write_summary(args.summary, summary)
    except OSError as e:
        log.error("summary_write_failed", extra={"fields": {"error": str(e), "path": str(args.summary)}})
        return EXIT_FAIL
    failed = error is not None or sink.fatal is not None or sink.delivery_failed > 0 or remaining > 0
    if failed:
        problems = {
            "error": error,
            "fatal": sink.fatal,
            "delivery_failed": sink.delivery_failed,
            "not_flushed": remaining,
            "last_error": sink.last_delivery_error,
        }
        log.error("run_incomplete", extra={"fields": problems})
    log.info("stop", extra={"fields": {"reason": stopped_by, "ticks": progress.ticks}})
    return EXIT_FAIL if failed else EXIT_OK


if __name__ == "__main__":
    sys.exit(main())
