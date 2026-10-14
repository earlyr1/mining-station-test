"""Инжектор сбоев телеметрии (§4.5): дубли, опоздания, обрывы связи. Детерминирован по seed."""

import heapq
import random
from collections import Counter
from collections.abc import Callable

from vq.config import SiteConfig
from vq.core.codec import Telemetry
from vq.geo import haversine

LATE_CLEARANCE_M = 100.0  # опоздание — только дальше 100 м от станций и разгрузки
OUTAGE_CLEARANCE_M = 700.0  # обрыв начинается только дальше 700 м: кончится до подъезда
LATE_S = (2, 60)
DUP_S = (0, 60)
OUTAGE_S = (5, 60)


class FaultInjector:
    """push(t, сообщения тика) → что отправить на тике t; drain() → отложенное по окончании прогона.

    Порядок на тике: текущие сообщения (по unit_uuid), затем назначенные на этот тик дубли и опоздания
    (в порядке назначения), затем досылка обрывов. Сообщение тика, на котором обрыв кончается, — последнее
    в досылке. expected_drops — сколько сообщений ядро отбросит как not_newer при этом порядке отправки.
    """

    def __init__(
        self,
        cfg: SiteConfig,
        seed: int,
        dup_rate: float = 0.005,
        late_rate: float = 0.002,
        outage_rate: float = 0.0005,
        on_fault: Callable[[str, int, Telemetry], None] | None = None,
    ):
        self.on_fault = on_fault  # (dup | late | outage, тик, сообщение) — для логов и тестов
        self.rng = random.Random(f"faults:{seed}")
        self.dup_rate = dup_rate
        self.late_rate = late_rate
        self.outage_rate = outage_rate
        self.faults: Counter[str] = Counter()  # dup | late | outage
        self.expected_drops = 0
        self._points = [(s.lat, s.lon) for s in cfg.stations] + [(cfg.dump.lat, cfg.dump.lon)]
        self._last_sent: dict[str, int] = {}
        self._scheduled: list[tuple[int, int, Telemetry]] = []  # куча (тик отправки, порядок назначения, msg)
        self._seq = 0
        self._outage_end: dict[str, int] = {}
        self._outage_buf: dict[str, list[Telemetry]] = {}

    def _far(self, m: Telemetry, dist_m: float) -> bool:
        return all(haversine(m.lat, m.lon, lat, lon) > dist_m for lat, lon in self._points)

    def _fault(self, kind: str, t: int, m: Telemetry) -> None:
        self.faults[kind] += 1
        if self.on_fault is not None:
            self.on_fault(kind, t, m)

    def _schedule(self, at: int, m: Telemetry) -> None:
        heapq.heappush(self._scheduled, (at, self._seq, m))
        self._seq += 1

    def push(self, t: int, msgs: list[Telemetry]) -> list[Telemetry]:
        current: list[Telemetry] = []
        flush: list[Telemetry] = []
        for m in msgs:
            unit = m.unit_uuid
            end = self._outage_end.get(unit)
            if end is not None:
                buf = self._outage_buf[unit]
                buf.append(m)
                if t >= end:
                    flush.extend(buf)
                    del self._outage_end[unit], self._outage_buf[unit]
                continue
            # сначала жребий, потом расстояние: гаверсинусы считаются только для редких выпавших
            if self.rng.random() < self.outage_rate and self._far(m, OUTAGE_CLEARANCE_M):
                self._outage_end[unit] = t + self.rng.randint(*OUTAGE_S)
                self._outage_buf[unit] = [m]
                self._fault("outage", t, m)
                continue
            if self.rng.random() < self.late_rate and self._far(m, LATE_CLEARANCE_M):
                self._schedule(t + self.rng.randint(*LATE_S), m)
                self._fault("late", t, m)
                continue
            current.append(m)
            if self.rng.random() < self.dup_rate:
                self._schedule(t + self.rng.randint(*DUP_S), m)
                self._fault("dup", t, m)
        due: list[Telemetry] = []
        while self._scheduled and self._scheduled[0][0] <= t:
            due.append(heapq.heappop(self._scheduled)[2])
        out = current + due + flush
        self._count(out)
        return out

    def drain(self) -> list[Telemetry]:
        """Всё отложенное: назначенные дубли и опоздания по времени, затем буферы обрывов по unit_uuid."""
        out = [m for _, _, m in sorted(self._scheduled)]
        self._scheduled.clear()
        for unit in sorted(self._outage_buf):
            out.extend(self._outage_buf[unit])
        self._outage_buf.clear()
        self._outage_end.clear()
        self._count(out)
        return out

    def _count(self, out: list[Telemetry]) -> None:
        """Правило ядра §3.4 п. 2 на собственном порядке отправки."""
        for m in out:
            last = self._last_sent.get(m.unit_uuid)
            if last is not None and m.ts <= last:
                self.expected_drops += 1
            else:
                self._last_sent[m.unit_uuid] = m.ts
