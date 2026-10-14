"""Общие построители для тестов ядра: площадка, сообщения, разбор выходов, детерминированный поток;
цифры прогона в итог pytest."""

import json
import random
from dataclasses import dataclass
from typing import TYPE_CHECKING

import pytest

from vq.config import Constants, Point, SiteConfig, StationCfg, validate
from vq.core.codec import Telemetry, encode_telemetry
from vq.core.model import StepResult
from vq.geo import bearing, destination, haversine

if TYPE_CHECKING:  # Engine нужен только аннотации feed(): импорт helpers не тянет ядро расчёта
    from vq.core.engine import Engine

NOON = 1_789_473_600  # 2026-09-15T12:00:00Z
BASE = (49.1288, 142.6601)  # точка разгрузки из образца в задании


def site(
    stations: dict[str, tuple[float, float]],
    assignment: dict[str, str],
    dump: tuple[float, float] | None = None,
    **constants: float,
) -> SiteConfig:
    """Площадка для теста. По умолчанию разгрузка в 10 км к югу от первой станции."""
    first = stations[min(stations)]
    d = dump if dump is not None else destination(first[0], first[1], 180.0, 10_000.0)
    cfg = SiteConfig(
        site_id="test",
        dump=Point(*d),
        stations=tuple(StationCfg(u, *stations[u]) for u in sorted(stations)),
        assignment=dict(sorted(assignment.items())),
        constants=Constants(**constants),  # type: ignore[arg-type]
    )
    validate(cfg)
    return cfg


def two_stations(units: dict[str, str] | None = None) -> SiteConfig:
    """S1 в BASE+10 км на север, S2 в 3300 м к востоку от S1; машины T1..T9 за S1, U1..U3 за S2."""
    s1 = destination(*BASE, 0.0, 10_000.0)
    s2 = destination(*s1, 90.0, 3_300.0)
    assignment = units or {**{f"T{i}": "S1" for i in range(1, 10)}, **{f"U{i}": "S2" for i in range(1, 4)}}
    return site({"S1": s1, "S2": s2}, assignment, dump=BASE)


def one_station() -> SiteConfig:
    """Одна станция S1: рекомендация всегда no_gain и не уводит машину — для тестов занятия и очереди."""
    s1 = destination(*BASE, 0.0, 10_000.0)
    return site({"S1": s1}, {f"T{i}": "S1" for i in range(1, 10)}, dump=BASE)


def four_stations() -> SiteConfig:
    """Площадка §4.1: S1..S4 в 10 км от разгрузки по азимутам 0/10/20/30°, машины 13/11/9/7."""
    stations = {f"S{i + 1}": destination(*BASE, 10.0 * i, 10_000.0) for i in range(4)}
    counts = {"S1": 13, "S2": 11, "S3": 9, "S4": 7}
    assignment: dict[str, str] = {}
    n = 1
    for st, k in counts.items():
        for _ in range(k):
            assignment[f"T-{n:02d}"] = st
            n += 1
    return site(stations, assignment, dump=BASE)


def at(cfg: SiteConfig, station: str, bearing: float, dist: float) -> tuple[float, float]:
    s = cfg.station(station)
    return destination(s.lat, s.lon, bearing, dist)


def msg(unit: str, ts: int, pos: tuple[float, float], speed: float = 36.0) -> bytes:
    return encode_telemetry(Telemetry(unit, ts, pos[0], pos[1], speed))


def feed(engine: "Engine", *raws: bytes) -> list[StepResult]:
    return [engine.apply(r) for r in raws]


def outputs(results: list[StepResult], topic: str) -> list[dict]:
    return [json.loads(o.value) for r in results for o in r.outputs if o.topic == topic]


def last_queue(results: list[StepResult], station: str) -> dict | None:
    qs = [q for q in outputs(results, "queue.v1") if q["station_uuid"] == station]
    return qs[-1] if qs else None


def kinds(results: list[StepResult]) -> list[str]:
    return [e.kind for r in results for e in r.events]


# --- детерминированный поток телеметрии -------------------------------------------------


@dataclass
class _Car:
    unit: str
    station: str
    dist: float  # от точки станции вдоль линии к разгрузке, м
    mode: str = "in"  # in | arrived | queued | serve | out | back
    start: int = 0  # начало обслуживания
    outage_until: int | None = None
    buffered: list[bytes] | None = None


def fleet_stream(
    cfg: SiteConfig,
    starts: dict[str, float],
    ticks: int,
    t0: int = NOON,
    *,
    seed: int = 0,
    dup_rate: float = 0.0,
    late_rate: float = 0.0,
    outage_rate: float = 0.0,
    leg_m: float = 10.0,
    approach_m: float = 1_703.0,
    service_s: dict[str, int] | None = None,
) -> list[bytes]:
    """Поток telemetry.v1 маленькой симуляции, не зависящей от vq.generator.

    Машина из starts едет по прямой от разгрузки к своей станции (10 м/с, старт в starts[unit] м
    от станции), ждёт в FIFO, стоит 200 с, выезжает манёвром 1,7 м/с (51 м на +230), едет к
    разгрузке и обратно шагом leg_m. service_s — своё время стоянки для отдельных машин (короткое даёт выезд
    раньше таймера). Рекомендаций не исполняет. Сбои по seed: дубль через 0–20 с, опоздание на 2–40 с,
    обрыв 5–60 с с досылкой по порядку (только дальше 700 м от станции).
    """
    c = cfg.constants
    rng = random.Random(seed)
    lines = {
        s.uuid: (
            s.lat,
            s.lon,
            bearing(s.lat, s.lon, cfg.dump.lat, cfg.dump.lon),
            haversine(s.lat, s.lon, cfg.dump.lat, cfg.dump.lon),
        )
        for s in cfg.stations
    }
    cars = [_Car(u, cfg.assignment[u], float(d)) for u, d in sorted(starts.items())]
    occupant: dict[str, _Car | None] = {s.uuid: None for s in cfg.stations}
    fifo: dict[str, list[_Car]] = {s.uuid: [] for s in cfg.stations}
    pending: dict[int, list[bytes]] = {}
    stream: list[bytes] = []
    for t in range(t0, t0 + ticks):
        for st in sorted(occupant):
            o = occupant[st]
            if o is not None and o.start + c.occupy_s <= t:
                occupant[st] = None
            if occupant[st] is None and fifo[st]:
                head = fifo[st].pop(0)
                occupant[st], head.start, head.mode = head, t, "serve"
        for car in cars:
            lat, lon, b, dump_m = lines[car.station]
            speed = 36.0
            if car.mode == "in":
                step = min(10.0, car.dist)
                car.dist -= step
                speed = max(step * 3.6, 1.0)
                if car.dist <= 0.0:
                    car.dist, car.mode = 0.0, "arrived"
            elif car.mode == "arrived":
                speed = 0.0
                if occupant[car.station] is None and not fifo[car.station]:
                    occupant[car.station], car.start, car.mode = car, t, "serve"
                else:
                    fifo[car.station].append(car)
                    car.mode = "queued"
            elif car.mode == "queued":
                speed = 0.0
            elif car.mode == "serve":
                k = t - car.start - (service_s or {}).get(car.unit, c.service_s)
                if k < 0:
                    speed = 0.0
                else:
                    car.dist, speed = 1.7 * k, 6.12
                    if k >= c.maneuver_s:
                        car.mode = "out"
            elif car.mode == "out":
                car.dist = min(car.dist + leg_m, dump_m)
                if car.dist >= dump_m:
                    car.mode = "back"
            else:
                car.dist = max(car.dist - leg_m, approach_m)
                if car.dist <= approach_m:
                    car.mode = "in"
            pos = destination(lat, lon, b, car.dist) if car.dist > 0.0 else (lat, lon)
            m = msg(car.unit, t, pos, speed)
            if car.outage_until is None and car.dist > 700.0 and rng.random() < outage_rate:
                car.outage_until, car.buffered = t + rng.randint(5, 60), []
            if car.outage_until is not None and car.buffered is not None:
                car.buffered.append(m)
                if t >= car.outage_until:
                    pending.setdefault(t, []).extend(car.buffered)
                    car.outage_until, car.buffered = None, None
                continue
            r = rng.random()
            if r < late_rate and car.dist > 100.0:
                pending.setdefault(t + rng.randint(2, 40), []).append(m)
                continue
            stream.append(m)
            if r > 1.0 - dup_rate:
                pending.setdefault(t + rng.randint(0, 20), []).append(m)
        stream.extend(pending.pop(t, []))
    for t in sorted(pending):
        stream.extend(pending[t])
    for car in cars:
        stream.extend(car.buffered or [])
    return stream


# --- цифры прогона в итоге pytest -------------------------------------------------------


class _Summary:
    """Плагин pytest: строки, которые тесты добавили в summary_line, — отдельной секцией итога прогона."""

    def __init__(self) -> None:
        self.lines: list[str] = []

    def pytest_terminal_summary(self, terminalreporter: pytest.TerminalReporter) -> None:
        terminalreporter.section("цифры прогона", sep="-")
        for line in self.lines:
            terminalreporter.write_line(line)


def summary_line(config: pytest.Config, line: str) -> None:
    """Строка в итог pytest: print в прошедшем тесте перехватывается, а итог печатается всегда."""
    plugin = config.pluginmanager.get_plugin(_Summary.__name__)
    if plugin is None:
        plugin = _Summary()
        config.pluginmanager.register(plugin, _Summary.__name__)
    plugin.lines.append(line)
