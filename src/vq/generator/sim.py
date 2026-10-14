"""Симулятор площадки (§4.2–4.4): чистый код без Kafka, часов и случайности, кроме random.Random(seed).

Тик — секунда симуляции; на каждом тике каждая машина даёт одно сообщение с ts = t. Все пороги считаются
vq.geo.haversine от тех же float, что уходят в сообщение.
"""

import bisect
import json
import random
from collections import Counter
from dataclasses import dataclass, field

from vq.config import SiteConfig
from vq.core.codec import Telemetry
from vq.geo import haversine, parse_ts, step_toward

DRIVE = "drive"  # отрезок к точке станции или разгрузки
ARRIVED = "arrived"  # в точке станции; на следующем тике — обслуживание или очередь
QUEUED = "queued"  # стоит в точке станции в FIFO
SERVING = "serving"  # занимает станцию, стоит
MANEUVER = "maneuver"  # отъезд от станции после обслуживания

DUMP = "dump"  # назначение отрезка «к разгрузке»

START_MIN_M = 1603.0  # старт: 1603 + 10·k м от своей станции, k ∈ 0…839 (§4.4)
START_STEP_M = 10.0
START_SLOTS = 840


@dataclass
class DecisionStats:
    received: int = 0  # принятые записи decision.v1 (без повторов и мусора)
    applied: int = 0  # исполненные рекомендации
    ignored: Counter[str] = field(default_factory=Counter)  # already_serving | stale_trip | invalid
    delays: list[int] = field(default_factory=list)  # тик исполнения − тик входа в зону решения, с (§4.2 п. 4)
    duplicates: int = 0  # отброшенные повторы по (unit_uuid, at)


@dataclass
class SimVehicle:
    unit_uuid: str
    home: str
    lat: float
    lon: float
    state: str = DRIVE
    dest: str = ""  # станция (или DUMP для drive к разгрузке)
    speed_kmh: float = 0.0
    radius_entered_at: int | None = None  # первый отсчёт ближе радиуса к dest на этом подъезде
    start: int | None = None  # начало обслуживания
    redirected: bool = False  # рекомендация в этом заезде уже исполнена
    trip_started_at: int = 0  # тик разгрузки; для первого заезда — тик старта
    zone_entered_at: int | None = None  # первый отсчёт ближе радиуса решения к своей станции в этом заезде
    cycles: int = 0  # завершённые циклы: обслуживание и разгрузка


@dataclass
class SimStation:
    uuid: str
    lat: float
    lon: float
    occupant: str | None = None
    start: int | None = None
    fifo: list[tuple[int, str]] = field(default_factory=list)  # (radius_entered_at, unit_uuid) по возрастанию


@dataclass(frozen=True)
class _Pending:
    unit: str
    at: int
    to_station: str


class Sim:
    """Машины ездят по прямой разгрузка ↔ станция, стоят 200 с, соблюдают FIFO и исполняют рекомендации."""

    def __init__(self, cfg: SiteConfig, seed: int, start_ts: int):
        self.cfg = cfg
        self.c = cfg.constants
        self.t = start_ts  # ts следующего тика
        self.stats = DecisionStats()
        self.stations: dict[str, SimStation] = {s.uuid: SimStation(s.uuid, s.lat, s.lon) for s in cfg.stations}
        self.dump = (cfg.dump.lat, cfg.dump.lon)
        # манёвр: на +maneuver_s машина в radius + 1 м от точки станции, на шаг раньше — ещё в радиусе
        self.maneuver_mps = (self.c.radius_m + 1.0) / self.c.maneuver_s
        self.maneuver_kmh = round(self.maneuver_mps * 3.6, 6)
        self._pending: list[_Pending] = []
        self._seen: set[tuple[str, int]] = set()
        rng = random.Random(seed)
        self.vehicles: dict[str, SimVehicle] = {}
        for unit in sorted(cfg.assignment):
            home = cfg.assignment[unit]
            st = self.stations[home]
            dist = START_MIN_M + START_STEP_M * rng.randrange(START_SLOTS)
            lat, lon = step_toward(st.lat, st.lon, self.dump[0], self.dump[1], dist)
            self.vehicles[unit] = SimVehicle(unit, home, lat, lon, DRIVE, home, trip_started_at=start_ts)

    # --- рекомендации ------------------------------------------------------

    def offer_decision(self, raw: bytes) -> None:
        """Запись decision.v1; рекомендация применяется в начале следующего tick(). Не бросает на мусоре."""
        try:
            obj = json.loads(raw)
            unit, at_s, result = obj["unit_uuid"], obj["at"], obj["result"]
            at = parse_ts(at_s)
            to_station = obj["to_station"] if result == "recommended" else ""
        except (ValueError, TypeError, KeyError, AttributeError, RecursionError):  # глубокая вложенность JSON
            self.stats.ignored["invalid"] += 1
            return
        valid = (
            isinstance(unit, str)
            and unit in self.vehicles
            and result in ("recommended", "rejected")
            and isinstance(to_station, str)
            and (result == "rejected" or (to_station in self.stations and to_station != self.vehicles[unit].home))
        )
        if not valid:
            self.stats.ignored["invalid"] += 1
            return
        if (unit, at) in self._seen:
            self.stats.duplicates += 1
            return
        self._seen.add((unit, at))
        self.stats.received += 1
        if result == "recommended":
            self._pending.append(_Pending(unit, at, to_station))

    def _refusal(self, v: SimVehicle, at: int) -> str | None:
        if at < v.trip_started_at:
            return "stale_trip"  # решение прошлого заезда
        if v.state in (SERVING, MANEUVER) or (v.state == DRIVE and v.dest == DUMP):
            return "already_serving"
        if v.redirected:
            return "stale_trip"  # в этом заезде уже перенаправлена
        return None  # drive к своей станции, arrived или queued у неё

    def _apply_decisions(self, t: int) -> None:
        pending, self._pending = self._pending, []
        for p in pending:
            v = self.vehicles[p.unit]
            reason = self._refusal(v, p.at)
            if reason is not None:
                self.stats.ignored[reason] += 1
                continue
            if v.state == QUEUED:
                assert v.radius_entered_at is not None
                self.stations[v.dest].fifo.remove((v.radius_entered_at, v.unit_uuid))
            v.state = DRIVE
            v.dest = p.to_station
            v.redirected = True
            v.radius_entered_at = None
            self.stats.applied += 1
            # задержка поворота — от входа в зону решения (§4.2 п. 4), с опозданием сообщения-триггера. Ядро решает
            # только по отсчёту ближе 1500 м, и зона к этому моменту уже пройдена; at — запасной вариант
            zone = v.zone_entered_at if v.zone_entered_at is not None else p.at
            self.stats.delays.append(t - zone)

    # --- тик ---------------------------------------------------------------

    def tick(self) -> list[Telemetry]:
        """Один тик: рекомендации, станции, машины по unit_uuid; затем t += 1."""
        t = self.t
        self._apply_decisions(t)
        for st in self.stations.values():
            if st.occupant is not None and st.start is not None and st.start + self.c.occupy_s <= t:
                st.occupant = None
                st.start = None
            if st.occupant is None and st.fifo:
                _, unit = st.fifo.pop(0)
                self._start_service(st, self.vehicles[unit], t)
        out: list[Telemetry] = []
        for v in self.vehicles.values():  # ключи вставлены по unit_uuid
            self._step(v, t)
            out.append(Telemetry(v.unit_uuid, t, v.lat, v.lon, v.speed_kmh))
        self.t = t + 1
        return out

    def occupants(self) -> dict[str, str | None]:
        return {uuid: st.occupant for uuid, st in self.stations.items()}

    def _start_service(self, st: SimStation, v: SimVehicle, t: int) -> None:
        st.occupant = v.unit_uuid
        st.start = t
        v.state = SERVING
        v.start = t

    def _step(self, v: SimVehicle, t: int) -> None:
        if v.state == DRIVE:
            self._drive(v, t)
        elif v.state == ARRIVED:
            v.speed_kmh = 0.0
            st = self.stations[v.dest]
            if st.occupant is None and not st.fifo:
                self._start_service(st, v, t)
            else:
                v.state = QUEUED
                assert v.radius_entered_at is not None
                bisect.insort(st.fifo, (v.radius_entered_at, v.unit_uuid))
        elif v.state == QUEUED:
            v.speed_kmh = 0.0
        else:  # SERVING / MANEUVER
            assert v.start is not None
            k = t - v.start - self.c.service_s
            if k < 0:
                v.speed_kmh = 0.0
                return
            st = self.stations[v.dest]
            v.state = MANEUVER
            v.lat, v.lon = step_toward(st.lat, st.lon, self.dump[0], self.dump[1], self.maneuver_mps * k)
            v.speed_kmh = self.maneuver_kmh
            if k >= self.c.maneuver_s:  # вне радиуса; со следующего тика — к разгрузке
                v.state = DRIVE
                v.dest = DUMP
                v.radius_entered_at = None
                v.start = None

    def _drive(self, v: SimVehicle, t: int) -> None:
        step = self.c.speed_mps  # м за тик
        if v.dest == DUMP:
            tlat, tlon = self.dump
        else:
            st = self.stations[v.dest]
            tlat, tlon = st.lat, st.lon
        remaining = haversine(v.lat, v.lon, tlat, tlon)
        if remaining > step:
            v.lat, v.lon = step_toward(v.lat, v.lon, tlat, tlon, step)
            moved = step
        elif v.dest == DUMP:
            # мгновенная разгрузка; остаток шага этого тика — уже к своей станции (иначе отсчёты
            # обратного плеча легли бы ровно на пороги 50 и 1500 м)
            self._unload(v, t)
            home = self.stations[v.home]
            v.lat, v.lon = step_toward(tlat, tlon, home.lat, home.lon, step - remaining)
            moved = step
        else:
            v.lat, v.lon = tlat, tlon
            v.state = ARRIVED
            moved = remaining
        # на коротком последнем шаге машина не должна выглядеть стоящей раньше прибытия
        v.speed_kmh = max(moved * 3.6, self.c.stop_kmh)
        if v.dest == DUMP:
            return
        zone_pending = v.dest == v.home and not v.redirected and v.zone_entered_at is None
        if v.radius_entered_at is None or zone_pending:
            st = self.stations[v.dest]
            d = haversine(v.lat, v.lon, st.lat, st.lon)  # тот же порядок аргументов, что у ядра
            if v.radius_entered_at is None and d < self.c.radius_m:
                v.radius_entered_at = t
            if zone_pending and d < self.c.decision_radius_m:
                v.zone_entered_at = t  # только для метрики задержки (§4.2 п. 4)

    def _unload(self, v: SimVehicle, t: int) -> None:
        v.cycles += 1
        v.dest = v.home
        v.redirected = False
        v.trip_started_at = t
        v.zone_entered_at = None
        v.radius_entered_at = None
