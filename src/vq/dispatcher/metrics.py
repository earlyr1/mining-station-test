"""Метрики dispatcher (§5.2): из событий ядра, времени пачек и лага. Обновляются после успешного чекпоинта."""

import json
import math
import time
from collections.abc import Callable, Iterable

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram

from vq.core.model import Event, SiteState

MESSAGE_RESULTS = ("accepted", "invalid", "not_newer")
DECISION_LABELS = (("recommended", ""), ("rejected", "no_gain"), ("rejected", "stale_telemetry"))
RELEASE_REASONS = ("timer", "exit")

PREDICTION_BUCKETS = (-300, -120, -60, -30, -10, -5, -1, 0, 1, 5, 10, 30, 60, 120, 300)
ABS_ERROR_BUCKETS = (0, 1, 2, 3, 5, 10, 20, 30, 60, 120, 300)
GAIN_BUCKETS = (60, 90, 120, 180, 240, 300, 450, 600, 900, 1200, 1800)
BATCH_BUCKETS = (0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0, 30.0)
CHECKPOINT_BUCKETS = (0.0005, 0.001, 0.0025, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0)


class DispatcherMetrics:
    """Все метрики §5.2. registry — для тестов (свежий CollectorRegistry); stations — заранее создать серии с нулём."""

    def __init__(
        self,
        registry: CollectorRegistry = REGISTRY,
        stations: Iterable[str] = (),
        clock: Callable[[], float] = time.time,
    ) -> None:
        r = registry
        self._clock = clock
        self._now: int | None = None
        self.messages = Counter(
            "vq_messages_total",
            "Входные сообщения по исходу; счётчик процесса, точные — в processor",
            ["result"],
            registry=r,
        )
        self.event_time = Gauge("vq_event_time_seconds", "«Сейчас»: наибольший учтённый ts", registry=r)
        self.event_time_lag = Gauge(
            "vq_event_time_lag_seconds", "Часы минус «сейчас»; осмысленна только в реальном темпе", registry=r
        )
        self.event_time_lag.set_function(self._event_time_lag)
        self.queue_publications = Counter(
            "vq_queue_publications_total", "Записи queue.v1 по станциям", ["station"], registry=r
        )
        self.queue_length = Gauge("vq_queue_length", "Длина последней опубликованной очереди", ["station"], registry=r)
        self.decisions = Counter("vq_decisions_total", "Решения decision.v1", ["result", "reason"], registry=r)
        self.decision_gain = Histogram(
            "vq_decision_gain_seconds", "Выигрыш рекомендаций (result=recommended)", buckets=GAIN_BUCKETS, registry=r
        )
        self.prediction_error = Histogram(
            "vq_prediction_error_seconds",
            "Фактическое занятие минус предсказанное начало обслуживания",
            buckets=PREDICTION_BUCKETS,
            registry=r,
        )
        self.prediction_abs_error = Histogram(
            "vq_prediction_abs_error_seconds",
            "Модуль ошибки прогноза начала обслуживания",
            buckets=ABS_ERROR_BUCKETS,
            registry=r,
        )
        self.fsm_resync = Counter(
            "vq_fsm_resync_total",
            "Самолечение (остановка у нецелевой станции) и конец заезда «к станции» в радиусе разгрузки",
            ["station"],
            registry=r,
        )
        self.last_checkpoint = Gauge("vq_last_checkpoint_unixtime", "Время последнего чекпоинта, unixtime", registry=r)
        self.station_releases = Counter(
            "vq_station_releases_total", "Освобождения станций", ["station", "reason"], registry=r
        )
        self.batch_seconds = Histogram(
            "vq_batch_seconds", "Обработка пачки: ядро, отправка, flush, чекпоинт", buckets=BATCH_BUCKETS, registry=r
        )
        self.checkpoint_seconds = Histogram(
            "vq_checkpoint_seconds", "Чекпоинт в Postgres", buckets=CHECKPOINT_BUCKETS, registry=r
        )
        self.consumer_lag = Gauge("vq_consumer_lag_messages", "High watermark минус следующий offset", registry=r)
        self.epoch = Gauge("vq_epoch", "Epoch текущего владения площадкой", registry=r)
        for result in MESSAGE_RESULTS:
            self.messages.labels(result)
        for result, reason in DECISION_LABELS:
            self.decisions.labels(result, reason)
        for st in stations:
            self.queue_publications.labels(st)
            self.fsm_resync.labels(st)
            for why in RELEASE_REASONS:
                self.station_releases.labels(st, why)

    def _event_time_lag(self) -> float:
        return math.nan if self._now is None else self._clock() - self._now

    def _set_now(self, now: int | None) -> None:
        if now is not None:
            self._now = now
            self.event_time.set(now)

    def on_claim(self, epoch: int, state: SiteState) -> None:
        """Новое владение: epoch, «сейчас» и длины очередей из прочитанного состояния."""
        self.epoch.set(epoch)
        self._set_now(state.now)
        for st in state.stations.values():
            if st.last_published_queue is not None:
                self.queue_length.labels(st.station_uuid).set(len(json.loads(st.last_published_queue)))

    def observe_events(self, events: Iterable[Event]) -> None:
        for e in events:
            kind = e.kind
            if kind in MESSAGE_RESULTS:
                self.messages.labels(kind).inc()
            elif kind == "queue_published":
                station = str(e.get("station"))
                self.queue_publications.labels(station).inc()
                length = e.get("length")
                if isinstance(length, int):
                    self.queue_length.labels(station).set(length)
            elif kind == "decision":
                result, reason = str(e.get("result")), str(e.get("reason") or "")
                self.decisions.labels(result, reason).inc()
                gain = e.get("gain")
                if result == "recommended" and isinstance(gain, int):
                    self.decision_gain.observe(gain)
            elif kind == "prediction_error":
                seconds = e.get("seconds")
                if isinstance(seconds, int):
                    self.prediction_error.observe(seconds)
                    self.prediction_abs_error.observe(abs(seconds))
            elif kind == "fsm_resync":
                self.fsm_resync.labels(str(e.get("station"))).inc()
            elif kind == "released":
                self.station_releases.labels(str(e.get("station")), str(e.get("reason"))).inc()

    def observe_batch(
        self, events: Iterable[Event], batch_seconds: float, checkpoint_seconds: float, now: int | None
    ) -> None:
        """Пачка записана в Postgres: события ядра, время пачки и чекпоинта, «сейчас»."""
        self.observe_events(events)
        self.batch_seconds.observe(batch_seconds)
        self.checkpoint_seconds.observe(checkpoint_seconds)
        self.last_checkpoint.set(self._clock())
        self._set_now(now)

    def observe_lag(self, messages: int) -> None:
        self.consumer_lag.set(max(0, messages))
