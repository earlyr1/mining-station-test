"""Метрики генератора (§4.6). Значения берутся из симулятора и инжектора и переносятся приращениями."""

from collections.abc import Mapping, Sequence

from prometheus_client import REGISTRY, CollectorRegistry, Counter, Gauge, Histogram

FAULT_TYPES = ("dup", "late", "outage")
IGNORE_REASONS = ("already_serving", "stale_trip")
DELAY_BUCKETS = (0.0, 1.0, 2.0, 3.0, 5.0, 10.0, 20.0, 30.0, 60.0, 120.0, 300.0, 600.0)


class GeneratorMetrics:
    """Счётчики Prometheus генератора. sync() вызывается после каждого тика."""

    def __init__(self, registry: CollectorRegistry = REGISTRY) -> None:
        self.registry = registry
        self.produced = Counter(
            "vq_gen_telemetry_produced_total", "Сообщений telemetry.v1 отдано продюсеру", registry=registry
        )
        self.faults = Counter("vq_gen_faults_injected_total", "Внесённые сбои по типу", ["type"], registry=registry)
        self.expected_drops = Counter(
            "vq_gen_expected_drops_total", "Ожидаемые отбросы по собственному порядку отправки", registry=registry
        )
        self.decisions_received = Counter(
            "vq_gen_decisions_received_total", "Получено записей decision.v1", registry=registry
        )
        self.decisions_applied = Counter(
            "vq_gen_decisions_applied_total", "Рекомендаций исполнено симулятором", registry=registry
        )
        self.decisions_ignored = Counter(
            "vq_gen_decisions_ignored_total", "Рекомендаций не исполнено, по причине", ["reason"], registry=registry
        )
        self.decision_delay = Histogram(
            "vq_gen_decision_delay_sim_seconds",
            "Задержка поворота в секундах симуляции: тик применения рекомендации минус тик входа в зону решения",
            buckets=DELAY_BUCKETS,
            registry=registry,
        )
        self.sim_time = Gauge(
            "vq_gen_sim_time_seconds", "Время симуляции последнего тика, секунды epoch", registry=registry
        )
        # Метки известных значений заводятся сразу: в /metrics видны нули вместо пустоты.
        for kind in FAULT_TYPES:
            self.faults.labels(type=kind)
        for reason in IGNORE_REASONS:
            self.decisions_ignored.labels(reason=reason)
        self._produced = 0
        self._faults: dict[str, int] = {}
        self._drops = 0
        self._received = 0
        self._applied = 0
        self._ignored: dict[str, int] = {}
        self._delays_seen = 0

    def sync(
        self,
        *,
        produced: int,
        faults: Mapping[str, int],
        expected_drops: int,
        received: int,
        applied: int,
        ignored: Mapping[str, int],
        delays: Sequence[int],
        sim_time: int,
    ) -> None:
        """Переносит накопленные значения: счётчики растут на разницу, новые задержки идут в гистограмму."""
        self._produced = _bump(self.produced, self._produced, produced)
        self._drops = _bump(self.expected_drops, self._drops, expected_drops)
        self._received = _bump(self.decisions_received, self._received, received)
        self._applied = _bump(self.decisions_applied, self._applied, applied)
        for kind, value in faults.items():
            self._faults[kind] = _bump(self.faults.labels(type=kind), self._faults.get(kind, 0), value)
        for reason, value in ignored.items():
            self._ignored[reason] = _bump(
                self.decisions_ignored.labels(reason=reason), self._ignored.get(reason, 0), value
            )
        for delay in delays[self._delays_seen :]:
            self.decision_delay.observe(delay)
        self._delays_seen = max(self._delays_seen, len(delays))
        self.sim_time.set(sim_time)


def _bump(counter: Counter, seen: int, value: int) -> int:
    """Увеличивает счётчик до value; уменьшение источника игнорируется (счётчик Prometheus не убывает)."""
    if value > seen:
        counter.inc(value - seen)
        return value
    return seen
