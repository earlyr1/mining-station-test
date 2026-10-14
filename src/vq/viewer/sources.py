"""Источники витрины (§5.1): Kafka (queue.v1, decision.v1), Postgres только на чтение, /metrics и итог генератора.

Каждый источник читает отдельный фоновый поток, таймауты раздельные. Запрос /api/state берёт только готовые срезы
и никогда не ждёт источник: недоступный источник помечается, остальные продолжают обновляться.
"""

from __future__ import annotations

import functools
import json
import logging
import math
import os
import threading
import time
import urllib.request
from collections import Counter, OrderedDict, deque
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import psycopg
from confluent_kafka import OFFSET_BEGINNING, Consumer, TopicPartition
from prometheus_client.parser import text_string_to_metric_families
from psycopg.rows import dict_row

from vq.core.codec import TS_MAX, TS_MIN, Headers, parse_src_offset
from vq.core.decision import DECISION_TOPIC
from vq.core.engine import QUEUE_TOPIC
from vq.geo import parse_ts
from vq.logs import describe

DECISIONS_KEEP = 200
SOURCE_TIMEOUT_S = 1.0
TOPICS = (QUEUE_TOPIC, DECISION_TOPIC)
QUEUE_ROW_FIELDS = ("unit_uuid", "eta", "service_start", "free_at", "wait_seconds")
RECOMMENDED_FIELDS = ("from_station", "to_station", "gain_seconds")
MAX_RECORD_DEPTH = 32  # уровней вложенности записи; по контракту §3.9 не больше трёх: запись, очередь, строка

log = logging.getLogger("vq.viewer")

PgConnection = psycopg.Connection[dict[str, Any]]


# --- срез источника и фоновый опрос --------------------------------------


@dataclass(frozen=True)
class Snapshot[T]:
    """Срез источника. data — последнее удачное чтение (при ошибке остаётся прежним); ok и error — итог последней
    попытки; checked_at — time.time() последнего удачного чтения."""

    ok: bool
    data: T | None
    error: str | None
    checked_at: float | None


class Poller[T]:
    """Опрос fetch() раз в interval секунд в своём потоке. Исключение помечает источник недоступным.

    Так же помечается опрос, который идёт дольше timeout: вызов не прервать (замёрзший Postgres не исполняет и
    свой statement_timeout), но страница видит пометку через timeout, а не через STALE_S.
    """

    def __init__(
        self,
        name: str,
        fetch: Callable[[], T],
        interval: float,
        clock: Callable[[], float] = time.time,
        timeout: float = SOURCE_TIMEOUT_S,
        monotonic: Callable[[], float] = time.monotonic,
    ) -> None:
        self.name = name
        self.timeout = timeout
        self._fetch = fetch
        self._interval = interval
        self._clock = clock
        self._monotonic = monotonic
        self._snap: Snapshot[T] = Snapshot(False, None, "ещё не опрашивался", None)
        self._busy_since: float | None = None  # monotonic начала текущего опроса
        self._first = True
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def poll_once(self) -> Snapshot[T]:
        prev = self._snap
        self._busy_since = self._monotonic()
        try:
            try:
                data = self._fetch()
            except Exception as e:  # источник вправе падать как угодно: это лишь пометка на странице
                snap: Snapshot[T] = Snapshot(False, prev.data, describe(e), prev.checked_at)
                if prev.ok or self._first:  # только переход в «недоступен», без повтора на каждый опрос
                    log.warning("source_down", extra={"fields": {"source": self.name, "error": snap.error}})
            else:
                snap = Snapshot(True, data, None, self._clock())
                if not prev.ok and prev.checked_at is not None:
                    log.info("source_up", extra={"fields": {"source": self.name}})
            self._snap = snap  # замена ссылки атомарна: читателю блокировка не нужна
        finally:
            self._busy_since = None
        self._first = False
        return snap

    def snapshot(self) -> Snapshot[T]:
        snap, since = self._snap, self._busy_since
        if since is not None:
            waited = self._monotonic() - since
            if waited > self.timeout:
                return Snapshot(False, snap.data, f"нет ответа {waited:.1f} сек", snap.checked_at)
        return snap

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name=f"vq-viewer-{self.name}", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=2.0)

    def _run(self) -> None:
        while not self._stop.is_set():
            started = time.monotonic()
            self.poll_once()
            self._stop.wait(max(0.0, self._interval - (time.monotonic() - started)))


# --- Kafka: свёртка queue.v1 и decision.v1 -------------------------------


@dataclass(frozen=True)
class QueueRecord:
    station: str
    at: str
    src_offset: int
    kafka_offset: int
    rows: tuple[Mapping[str, Any], ...]


@dataclass(frozen=True)
class DecisionRecord:
    unit: str
    at: str
    src_offset: int
    kafka_offset: int
    payload: Mapping[str, Any]


@dataclass(frozen=True)
class StreamData:
    queues: Mapping[str, QueueRecord]  # станция → запись, где vq-src-offset наибольший
    decisions: tuple[DecisionRecord, ...]  # от старых к новым, не больше DECISIONS_KEEP, без повторов
    decision_totals: Mapping[str, int]  # recommended / no_gain / stale_telemetry за весь прочитанный лог
    max_src_offset: Mapping[str, int]  # топик → наибольший vq-src-offset
    next_offset: Mapping[str, int]  # топик → следующий offset чтения
    high: Mapping[str, int]  # топик → high watermark по последней проверке
    repeats: Mapping[str, int]  # топик → отброшено повторов
    malformed: int
    resets: int


def event_ts(text: Any) -> int | None:
    """at записи → секунды epoch, если это 'YYYY-MM-DDTHH:MM:SSZ' ровно в виде fmt_ts и в диапазоне ts ядра
    (2001–2100); иначе None."""
    if not isinstance(text, str):
        return None
    try:
        ts = parse_ts(text)
    except ValueError:
        return None
    return ts if TS_MIN <= ts < TS_MAX else None


def _nested_deeper_than(obj: dict[str, Any] | list[Any], limit: int) -> bool:
    """Глубже ли limit уровней вложенность obj (сам obj — первый уровень). Обход без рекурсии: на глубокой записи
    рекурсия в Python падает уже на 1 000 уровней."""
    stack: list[tuple[Any, int]] = [(obj, 1)]
    while stack:
        node, depth = stack.pop()
        if depth > limit:
            return True
        for child in node.values() if isinstance(node, dict) else node:
            if isinstance(child, (dict, list)):
                stack.append((child, depth + 1))
    return False


def _json_object(value: bytes | None) -> dict[str, Any] | None:
    """JSON-объект записи или None. Не конечные числа (NaN, 1e999) json.loads пропускает: в /api/state они
    становятся null (app.json_safe). Глубокая вложенность — запись не той формы (§5.1): json.loads разбирает около
    10 000 уровней, а json_safe рекурсивен и падает на 1 000, и одна такая запись гасила бы страницу."""
    if value is None:
        return None
    try:
        obj = json.loads(value)
    except (ValueError, RecursionError):  # UnicodeDecodeError и JSONDecodeError — подклассы ValueError
        return None
    return obj if isinstance(obj, dict) and not _nested_deeper_than(obj, MAX_RECORD_DEPTH) else None


def _queue_rows(rows: Any) -> tuple[Mapping[str, Any], ...] | None:
    """Строки очереди формы §3.9 (объекты с пятью полями) или None. Типы полей не проверяются: записи пишет наш
    dispatcher, а страница показывает что пришло."""
    if not isinstance(rows, list) or not all(
        isinstance(r, dict) and all(k in r for k in QUEUE_ROW_FIELDS) for r in rows
    ):
        return None
    return tuple(rows)


def _decision_kind(payload: Mapping[str, Any]) -> str | None:
    """Итог решения для счётчиков: recommended или код причины отказа; None — запись не формы §3.9."""
    result = payload.get("result")
    if result == "recommended":
        return result if all(k in payload for k in RECOMMENDED_FIELDS) else None
    reason = payload.get("reason")
    return reason if result == "rejected" and isinstance(reason, str) else None


class StreamCache:
    """Правила потребителя (§2.6) без Kafka.

    queue.v1: по станции хранится запись, где vq-src-offset наибольший; запись, где vq-src-offset не больше уже
    виденного — повтор после падения dispatcher, она отбрасывается. decision.v1: последние keep записей,
    повторы по (unit_uuid, at) отбрасываются. Если offset самого топика ушёл назад (make reset пересоздал
    топики), кеш этого топика сбрасывается. Запись не той формы (не объект, без заголовка или ключей §3.9)
    считается в malformed и ничего не меняет: станция остаётся с последней хорошей очередью.
    """

    def __init__(self, keep: int = DECISIONS_KEEP, seen_limit: int = 20 * DECISIONS_KEEP) -> None:
        self._keep = keep
        self._seen_limit = seen_limit
        self._queues: dict[str, QueueRecord] = {}
        self._decisions: deque[DecisionRecord] = deque(maxlen=keep)
        self._seen: OrderedDict[tuple[str, str], None] = OrderedDict()
        self._totals: Counter[str] = Counter()
        self._max_src: dict[str, int] = {}
        self._next: dict[str, int] = {}
        self._high: dict[str, int] = {}
        self._repeats: Counter[str] = Counter()
        self.malformed = 0
        self.resets = 0

    def next_offset(self, topic: str) -> int:
        return self._next.get(topic, 0)

    def reset_topic(self, topic: str) -> None:
        if topic == QUEUE_TOPIC:
            self._queues.clear()
        elif topic == DECISION_TOPIC:
            self._decisions.clear()
            self._seen.clear()
            self._totals.clear()
        self._max_src.pop(topic, None)
        self._next.pop(topic, None)
        self._repeats.pop(topic, None)
        self.resets += 1
        log.warning("stream_reset", extra={"fields": {"topic": topic}})

    def note_high(self, topic: str, high: int) -> None:
        """High watermark ниже позиции чтения — лог пересоздан."""
        if high < self._next.get(topic, 0):
            self.reset_topic(topic)
        self._high[topic] = high

    def ingest(self, topic: str, kafka_offset: int, value: bytes | None, headers: Headers) -> None:
        if kafka_offset < self._next.get(topic, 0):
            self.reset_topic(topic)
        self._next[topic] = kafka_offset + 1
        src = parse_src_offset(headers)
        payload = _json_object(value)
        if src is None or payload is None:
            self.malformed += 1
            return
        if topic == QUEUE_TOPIC:
            valid = self._ingest_queue(kafka_offset, src, payload)
        elif topic == DECISION_TOPIC:
            valid = self._ingest_decision(kafka_offset, src, payload)
        else:
            return
        if not valid:
            self.malformed += 1
            return
        self._max_src[topic] = max(self._max_src.get(topic, -1), src)

    def _ingest_queue(self, kafka_offset: int, src: int, payload: dict[str, Any]) -> bool:
        station, at = payload.get("station_uuid"), payload.get("at")
        rows = _queue_rows(payload.get("queue"))
        if not isinstance(station, str) or not isinstance(at, str) or event_ts(at) is None or rows is None:
            return False
        current = self._queues.get(station)
        if current is not None and src <= current.src_offset:
            self._repeats[QUEUE_TOPIC] += 1
            return True
        self._queues[station] = QueueRecord(station, at, src, kafka_offset, rows)
        return True

    def _ingest_decision(self, kafka_offset: int, src: int, payload: dict[str, Any]) -> bool:
        unit, at, kind = payload.get("unit_uuid"), payload.get("at"), _decision_kind(payload)
        if not isinstance(unit, str) or not isinstance(at, str) or event_ts(at) is None or kind is None:
            return False
        key = (unit, at)
        if key in self._seen:
            self._repeats[DECISION_TOPIC] += 1
            return True
        self._seen[key] = None
        while len(self._seen) > self._seen_limit:
            self._seen.popitem(last=False)
        self._decisions.append(DecisionRecord(unit, at, src, kafka_offset, payload))
        self._totals[kind] += 1
        return True

    def data(self) -> StreamData:
        return StreamData(
            queues=dict(self._queues),
            decisions=tuple(self._decisions),
            decision_totals=dict(self._totals),
            max_src_offset=dict(self._max_src),
            next_offset=dict(self._next),
            high=dict(self._high),
            repeats=dict(self._repeats),
            malformed=self.malformed,
            resets=self.resets,
        )


def kafka_conf(bootstrap: str) -> dict[str, Any]:
    """Consumer витрины: assign без группы и коммитов, установка соединения не дольше таймаута источника."""
    return {
        "bootstrap.servers": bootstrap,
        "group.id": "vq-viewer",  # librdkafka требует group.id; группой не пользуемся
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
        "client.id": "vq-viewer",
        "socket.connection.setup.timeout.ms": int(SOURCE_TIMEOUT_S * 1000),
    }


class KafkaView:
    """Фоновый поток: queue.v1 и decision.v1 от начала, assign без группы и коммитов, свёртка в StreamCache.

    Раз в секунду читается high watermark обоих топиков (таймаут 1 сек): это и признак доступности брокера,
    и отставание чтения, и способ заметить пересозданный топик. Сломанный клиент пересоздаётся через retry_after
    секунд, чтение продолжается с позиции кеша.
    """

    def __init__(
        self,
        bootstrap: str,
        make_consumer: Callable[[dict[str, Any]], Consumer] = Consumer,
        clock: Callable[[], float] = time.time,
        check_every: float = 1.0,
        reconnect_after: int = 10,
        retry_after: float = 1.0,
    ) -> None:
        self._bootstrap = bootstrap
        self._make_consumer = make_consumer
        self._clock = clock
        self._check_every = check_every
        self._reconnect_after = reconnect_after
        self._retry_after = retry_after
        self._cache = StreamCache()
        self._lock = threading.Lock()
        self._ok = False
        self._error: str | None = "ещё не подключались"
        self._checked_at: float | None = None
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None

    def snapshot(self) -> Snapshot[StreamData]:
        with self._lock:
            return Snapshot(self._ok, self._cache.data(), self._error, self._checked_at)

    def start(self) -> None:
        self._thread = threading.Thread(target=self._run, name="vq-viewer-kafka", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join(timeout=3.0)

    def _set_down(self, error: str) -> None:
        with self._lock:
            if self._ok:
                log.warning("source_down", extra={"fields": {"source": "kafka", "error": error}})
            self._ok, self._error = False, error

    def _run(self) -> None:
        while not self._stop.is_set():
            consumer = None
            try:
                consumer = self._make_consumer(kafka_conf(self._bootstrap))
                self._session(consumer)
            except Exception as e:  # брокер недоступен или клиент сломан: пометить и пересоздать
                self._set_down(describe(e))
                self._stop.wait(self._retry_after)
            finally:
                if consumer is not None:
                    try:
                        consumer.close()
                    except Exception as e:
                        log.warning("kafka_close_failed", extra={"fields": {"error": describe(e)}})

    def _session(self, consumer: Consumer) -> None:
        with self._lock:
            # После пересоздания клиента чтение продолжается дальше, не сначала.
            parts = [TopicPartition(t, 0, self._cache.next_offset(t) or OFFSET_BEGINNING) for t in TOPICS]
        consumer.assign(parts)
        failures = 0
        next_check = 0.0
        while not self._stop.is_set():
            msgs = consumer.consume(num_messages=500, timeout=0.25)
            error = None
            with self._lock:
                for m in msgs:
                    err, topic, offset = m.error(), m.topic(), m.offset()
                    if err is not None:
                        error = str(err)
                    elif topic is not None and offset is not None:  # без ошибки они всегда есть
                        self._cache.ingest(topic, offset, m.value(), m.headers())
            if error is not None:
                self._set_down(error)
            if time.monotonic() < next_check:
                continue
            next_check = time.monotonic() + self._check_every
            try:
                highs = {
                    t: consumer.get_watermark_offsets(TopicPartition(t, 0), SOURCE_TIMEOUT_S, False)[1] for t in TOPICS
                }
            except Exception as e:
                failures += 1
                self._set_down(describe(e))
                if failures >= self._reconnect_after:
                    raise
                continue
            failures = 0
            with self._lock:
                for topic, high in highs.items():
                    self._cache.note_high(topic, int(high))
                if not self._ok and self._checked_at is not None:
                    log.info("source_up", extra={"fields": {"source": "kafka"}})
                self._ok, self._error, self._checked_at = True, None, self._clock()


# --- Postgres -------------------------------------------------------------


@dataclass(frozen=True)
class PgData:
    processor: Mapping[str, Any] | None  # строка processor этой площадки; None — dispatcher ещё не стартовал
    stations: tuple[Mapping[str, Any], ...]
    vehicles: tuple[Mapping[str, Any], ...]


PROCESSOR_SQL = (
    "SELECT site_id, epoch, next_offset, now_ts, logic_version, config_hash,"
    " msgs_accepted, msgs_not_newer, msgs_invalid, updated_at FROM processor WHERE site_id = %s"
)
STATIONS_SQL = (
    "SELECT station_uuid, occupant_uuid, occupied_at, last_release_at, updated_at FROM stations ORDER BY station_uuid"
)
VEHICLES_SQL = (
    "SELECT unit_uuid, home_station, last_ts, lat, lon, speed_kmh, phase, target_station, radius_entered_at,"
    " arrived_at, dump_visited, decided, predicted_start, updated_at FROM vehicles ORDER BY unit_uuid"
)


def _plain(row: Mapping[str, Any]) -> dict[str, Any]:
    """Строка Postgres → JSON-совместимый словарь; timestamptz → ISO и секунды epoch."""
    out: dict[str, Any] = {}
    for key, value in row.items():
        if isinstance(value, datetime):
            value = value.astimezone(UTC)
            out[key] = value.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
            out[f"{key}_epoch"] = value.timestamp()
        else:
            out[key] = value
    return out


def pg_connect(dsn: str) -> PgConnection:
    # connect_timeout=1: libpq всё равно ждёт минимум 2 секунды, это нижняя граница самой libpq.
    return psycopg.connect(
        dsn,
        connect_timeout=1,
        autocommit=True,
        row_factory=dict_row,
        options="-c statement_timeout=1000 -c idle_in_transaction_session_timeout=5000",
        application_name="vq-viewer",
    )


class PgReader:
    """Чтение processor, stations и vehicles одной транзакцией REPEATABLE READ READ ONLY: согласованный срез
    на next_offset. После любой ошибки соединение закрывается, следующий опрос подключится заново."""

    def __init__(self, dsn: str, site_id: str, connect: Callable[[str], PgConnection] = pg_connect) -> None:
        self._dsn = dsn
        self._site_id = site_id
        self._connect = connect
        self._conn: PgConnection | None = None

    def read(self) -> PgData:
        if self._conn is None or self._conn.closed:
            self._conn = self._connect(self._dsn)
        conn = self._conn
        try:
            conn.execute("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY")
            processor = conn.execute(PROCESSOR_SQL, (self._site_id,)).fetchone()
            stations = conn.execute(STATIONS_SQL).fetchall()
            vehicles = conn.execute(VEHICLES_SQL).fetchall()
            conn.execute("COMMIT")
        except Exception:
            self.close()
            raise
        return PgData(
            processor=None if processor is None else _plain(processor),
            stations=tuple(_plain(r) for r in stations),
            vehicles=tuple(_plain(r) for r in vehicles),
        )

    def close(self) -> None:
        conn, self._conn = self._conn, None
        if conn is not None:
            try:
                conn.close()
            except Exception as e:
                log.warning("pg_close_failed", extra={"fields": {"error": describe(e)}})


# --- /metrics и итог генератора ------------------------------------------


@dataclass(frozen=True)
class Sample:
    name: str
    labels: Mapping[str, str]
    value: float


def parse_metrics(text: str) -> tuple[Sample, ...]:
    return tuple(
        Sample(s.name, dict(s.labels), float(s.value))
        for family in text_string_to_metric_families(text)
        for s in family.samples
    )


def scrape(url: str, timeout: float = SOURCE_TIMEOUT_S) -> tuple[Sample, ...]:
    """GET /metrics без прокси из окружения, таймаут на соединение и на чтение."""
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    with opener.open(url, timeout=timeout) as resp:
        body = resp.read(8 * 1024 * 1024)
    return parse_metrics(body.decode("utf-8", errors="replace"))


def read_summary(path: Path) -> Mapping[str, Any]:
    with open(path, encoding="utf-8") as f:
        obj = json.load(f)
    if not isinstance(obj, dict):
        raise ValueError(f"{path}: ожидается JSON-объект")
    return obj


def metric_value(samples: Sequence[Sample], name: str, **labels: str) -> float | None:
    """Сумма отсчётов метрики name при совпадении меток; None — таких отсчётов нет."""
    values = [s.value for s in samples if s.name == name and all(s.labels.get(k) == v for k, v in labels.items())]
    return sum(values) if values else None


def bucket_value(samples: Sequence[Sample], histogram: str, le: float) -> float | None:
    """Накопленный счётчик корзины le гистограммы (сумма по прочим меткам); None — такой корзины нет."""
    values = [s.value for s in samples if s.name == f"{histogram}_bucket" and _le(s.labels.get("le")) == le]
    return sum(values) if values else None


def _le(text: str | None) -> float | None:
    try:
        return float(text) if text is not None else None  # '5.0', '5', '+Inf'
    except ValueError:
        return None


def pick_dispatcher(snaps: Sequence[Snapshot[tuple[Sample, ...]]]) -> Snapshot[tuple[Sample, ...]] | None:
    """Срез dispatcher из нескольких экземпляров (основной и standby, §2.2). Из ответивших — владелец площадки:
    наибольший vq_epoch, как в панелях Grafana; при равенстве — первый в списке. Если не ответил никто — срез
    с самым свежим удачным чтением (его данные и ошибка)."""
    up = [s for s in snaps if s.ok and s.data is not None]
    if up:
        return max(up, key=lambda s: _epoch(s.data or ()))
    return max(snaps, key=lambda s: -math.inf if s.checked_at is None else s.checked_at, default=None)


def _epoch(samples: Sequence[Sample]) -> float:
    value = metric_value(samples, "vq_epoch")
    return value if value is not None and math.isfinite(value) else -1.0


# --- набор источников -----------------------------------------------------


@dataclass(frozen=True)
class SourceSnapshots:
    kafka: Snapshot[StreamData] | None = None
    pg: Snapshot[PgData] | None = None
    dispatcher: Snapshot[tuple[Sample, ...]] | None = None
    generator: Snapshot[tuple[Sample, ...]] | None = None
    summary: Snapshot[Mapping[str, Any]] | None = None


class Sources:
    """Источники витрины; каждый — отдельный поток. Экземпляров dispatcher может быть несколько (standby)."""

    def __init__(
        self,
        kafka: KafkaView | None,
        pg: Poller[PgData] | None,
        dispatchers: Sequence[Poller[tuple[Sample, ...]]],
        generator: Poller[tuple[Sample, ...]] | None,
        summary: Poller[Mapping[str, Any]] | None,
    ) -> None:
        self.kafka, self.pg, self.generator, self.summary = kafka, pg, generator, summary
        self.dispatchers = tuple(dispatchers)

    @classmethod
    def from_env(cls, site_id: str, env: Mapping[str, str] | None = None) -> Sources:
        """VQ_DISPATCHER_METRICS — адрес /metrics dispatcher или несколько через запятую (основной и standby)."""
        env = os.environ if env is None else env
        reader = PgReader(env.get("VQ_PG_DSN", "postgresql://vq:vq@localhost:5432/vq"), site_id)
        dispatcher_urls = env.get("VQ_DISPATCHER_METRICS", "http://localhost:9100/metrics").split(",")
        generator_url = env.get("VQ_GENERATOR_METRICS", "http://localhost:9101/metrics")
        summary_path = Path(env.get("VQ_SUMMARY", "/out/summary.json"))
        dispatchers = [
            Poller(f"dispatcher {urlsplit(url).netloc or url}", functools.partial(scrape, url), 1.0)
            for url in (u.strip() for u in dispatcher_urls)
            if url
        ]
        return cls(
            kafka=KafkaView(env.get("VQ_KAFKA", "localhost:19092")),
            pg=Poller("postgres", reader.read, 0.5),
            dispatchers=dispatchers,
            generator=Poller("generator", functools.partial(scrape, generator_url), 1.0),
            summary=Poller("summary", functools.partial(read_summary, summary_path), 2.0),
        )

    def _all(self) -> list[KafkaView | Poller[Any]]:
        sources = (self.kafka, self.pg, *self.dispatchers, self.generator, self.summary)
        return [s for s in sources if s is not None]

    def start(self) -> None:
        for s in self._all():
            s.start()

    def stop(self) -> None:
        for s in self._all():
            s.stop()

    def snapshots(self) -> SourceSnapshots:
        return SourceSnapshots(
            kafka=None if self.kafka is None else self.kafka.snapshot(),
            pg=None if self.pg is None else self.pg.snapshot(),
            dispatcher=pick_dispatcher([p.snapshot() for p in self.dispatchers]),
            generator=None if self.generator is None else self.generator.snapshot(),
            summary=None if self.summary is None else self.summary.snapshot(),
        )
