"""Общее для тестов dispatcher: поток со всеми путями ядра (и для test_store_pg), подделки Sink и Store,
пример из ТЗ, пачки и офлайн-прогон."""

import copy
from collections.abc import Callable, Iterator

from tests.helpers import BASE, NOON, at, msg, site, two_stations
from vq.config import SiteConfig
from vq.core.engine import Engine
from vq.core.model import SiteState
from vq.dispatcher.loop import Deadline, InMsg, Processor
from vq.dispatcher.metrics import DispatcherMetrics
from vq.dispatcher.store import Claim, Counters
from vq.geo import destination

# --- сценарий потока: общий для test_dispatcher_loop и test_store_pg ----------------------------


def line_site() -> SiteConfig:
    """S1 в 2 км к северу от разгрузки, S2 в 1800 м к востоку от S1; T1..T6 за S1, U1..U2 за S2."""
    s1 = destination(*BASE, 0.0, 2_000.0)
    s2 = destination(*s1, 90.0, 1_800.0)
    units = {**{f"T{i}": "S1" for i in range(1, 7)}, "U1": "S2", "U2": "S2"}
    return site({"S1": s1, "S2": s2}, units, dump=BASE)


# машина: (станция, азимут линии от неё, старт м, остановка м, стоянка с, первый тик)
PLAN = {
    "T1": ("S1", 180.0, 14.0, 14.0, 240, 0),
    "T2": ("S1", 180.0, 700.0, 22.0, 240, 0),
    "T3": ("S1", 180.0, 1_640.0, 30.0, 240, 0),
    "T4": ("S1", 180.0, 1_180.0, 18.0, 120, 0),  # уезжает раньше таймера: освобождение по выходу
    "T5": ("S1", 180.0, 1_990.0, 26.0, 240, 0),
    "T6": ("S1", 180.0, 900.0, 34.0, 240, 40),
    "U1": ("S2", 90.0, 16.0, 16.0, 240, 0),
    "U2": ("S2", 90.0, 460.0, 24.0, 240, 0),
}


def trajectory(start: float, stop: float, stand: int, first: int, horizon: int) -> Iterator[tuple[int, float, float]]:
    """(тик, расстояние от станции по линии, скорость): к станции 10 м/с, стоянка, до 2000 м (разгрузка) и назад."""
    d, mode, left = start, "in", 0
    for t in range(first, horizon):
        if mode == "in":
            d = max(stop, d - 10.0)
            if d == stop:
                mode, left = "stand", stand
                yield t, d, 0.0
                continue
            yield t, d, 36.0
        elif mode == "stand":
            left -= 1
            if left <= 0:
                mode = "out"
            yield t, d, 0.0
        else:
            d = min(2_000.0, d + 10.0)
            if d == 2_000.0:
                mode = "in"
            yield t, d, 36.0


def scenario(cfg: SiteConfig, horizon: int = 900) -> list[bytes | None]:
    """Детерминированный поток с дублями, опозданиями, обрывом (опоздавшие, но новые) и невалидными сообщениями.

    Покрывает recommended и no_gain, освобождение по таймеру и по выходу, передачу, самолечение, разгрузку.
    """
    entries: list[tuple[tuple[int, int, int], bytes | None]] = []
    for i, unit in enumerate(sorted(PLAN)):
        st_uuid, brg, start, stop, stand, first = PLAN[unit]
        st = cfg.station(st_uuid)
        for t, d, speed in trajectory(start, stop, stand, first, horizon):
            raw = msg(unit, NOON + t, destination(st.lat, st.lon, brg, d), speed)
            n = len(entries)
            send_at = t
            if unit == "T2" and 300 <= t < 312:  # обрыв: копит и досылает по порядку
                send_at = 312
            elif n % 89 == 5:  # опоздание: старее уже учтённого
                send_at = t + 3
            entries.append(((send_at, i, n), raw))
            if n % 61 == 7:  # дубль
                entries.append(((t + 4, i, n), raw))
    for t in range(17, horizon, 113):
        entries.append(((t, 99, t), b'{"unit_uuid":"T1","ts":"bad","lat":0,"lon":0,"speed_kmh":0}'))
    for t in range(29, horizon, 157):
        entries.append(((t, 98, t), None))
    return [raw for _, raw in sorted(entries, key=lambda e: e[0])]


# --- подделки -------------------------------------------------------------------


class FakeSink:
    def __init__(self, journal: list[str], remaining: int = 0, failed: str | None = None):
        self.journal = journal
        self.sent: list[tuple[str, bytes, bytes, int]] = []
        self.remaining = remaining
        self.failed = failed

    def send(self, topic: str, key: bytes, value: bytes, src_offset: int) -> None:
        self.journal.append("send")
        self.sent.append((topic, key, value, src_offset))

    def flush(self, timeout: float) -> int:
        assert timeout == 10.0
        self.journal.append("flush")
        return self.remaining


class FakeStore:
    def __init__(self, journal: list[str], claim: Claim, accept: bool = True):
        self.journal = journal
        self.claim_result = claim
        self.accept = accept
        self.claims: list[tuple[str, str, str]] = []
        self.checkpoints: list[tuple[int, int, Counters, SiteState]] = []

    def claim(self, site_id: str, logic_version: str, config_hash: str) -> Claim:
        self.journal.append("claim")
        self.claims.append((site_id, logic_version, config_hash))
        return copy.deepcopy(self.claim_result)

    def checkpoint(self, site_id: str, epoch: int, next_offset: int, counters: Counters, state: SiteState) -> bool:
        self.journal.append("checkpoint")
        self.checkpoints.append((epoch, next_offset, copy.deepcopy(counters), copy.deepcopy(state)))
        return self.accept


def empty_claim(epoch: int = 7, counters: Counters | None = None) -> Claim:
    return Claim(epoch, None, None, counters or Counters(), None)


def make(
    cfg: SiteConfig | None = None,
    claim: Claim | None = None,
    accept: bool = True,
    remaining: int = 0,
    failed: str | None = None,
    metrics: DispatcherMetrics | None = None,
    crash_hook: Callable[..., None] | None = None,
    journal: list[str] | None = None,
    store: FakeStore | None = None,
    deadline: Deadline | None = None,
) -> tuple[Processor, FakeStore, FakeSink, list[str]]:
    journal = [] if journal is None else journal
    store = store or FakeStore(journal, claim or empty_claim(), accept)
    sink = FakeSink(journal, remaining, failed)
    p = Processor(cfg or two_stations(), "cfg-hash", store, sink, metrics, crash_hook, deadline)
    return p, store, sink, journal


def example(cfg: SiteConfig) -> list[bytes | None]:
    """Пример из ТЗ: T1 занимает S1, T2 и T3 едут к S1, T4 получает рекомендацию на S2."""
    return [
        msg("T1", NOON - 60, at(cfg, "S1", 0.0, 15.0), speed=0.0),
        msg("T1", NOON, at(cfg, "S1", 0.0, 15.0), speed=0.0),
        msg("T2", NOON, at(cfg, "S1", 270.0, 1_200.0)),
        msg("T3", NOON, at(cfg, "S1", 180.0, 3_000.0)),
        msg("T4", NOON, at(cfg, "S1", 90.0, 900.0)),
    ]


def batch(raws: list[bytes | None], start: int) -> list[InMsg]:
    return [InMsg(start + i, r) for i, r in enumerate(raws)]


def offline(cfg: SiteConfig, raws: list[bytes | None], start: int) -> list[tuple[str, bytes, bytes, int]]:
    e = Engine(cfg)
    return [(o.topic, o.key, o.value, start + i) for i, r in enumerate(raws) for o in e.apply(r).outputs]
