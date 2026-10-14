"""Переходы машины (§3.5) и занятие / освобождение / передача станции (§3.6).

Таблица §3.5 — по методу на фазу. В фазе to_station до её строк — вход в радиус любой станции (R1) и радиус
разгрузки (R2).
"""

from vq.config import SiteConfig
from vq.core.model import SERVED, SERVICE, TO_DUMP, TO_STATION, WAITING, Event, SiteState, Station, Vehicle, event
from vq.core.queue import dist_to_station
from vq.geo import haversine


def occupy_time(ts: int, st: Station) -> int:
    """Момент занятия по сообщению: не раньше последнего освобождения (§3.6, §3.11 п. 19)."""
    return ts if st.last_release_at is None else max(ts, st.last_release_at)


class Fsm:
    """Мутирует SiteState; события складывает в self.events."""

    def __init__(self, state: SiteState, cfg: SiteConfig, events: list[Event]):
        self.state = state
        self.cfg = cfg
        self.const = cfg.constants
        self.events = events

    # --- станции (§3.6) ----------------------------------------------------

    def occupy(self, st: Station, v: Vehicle, t: int, how: str) -> None:
        st.occupant_uuid = v.unit_uuid
        st.occupied_at = t
        v.phase = SERVICE
        self.events.append(event("occupied", station=st.station_uuid, how=how, unit=v.unit_uuid))
        if v.predicted_start is not None:
            self.events.append(event("prediction_error", seconds=t - v.predicted_start))
            v.predicted_start = None

    def release(self, st: Station, r: int, reason: str) -> None:
        if reason == "timer" and st.occupant_uuid is not None:
            self.state.vehicles[st.occupant_uuid].phase = SERVED
        st.occupant_uuid = None
        st.occupied_at = None
        st.last_release_at = r
        self.events.append(event("released", station=st.station_uuid, reason=reason))
        self.handover(st, r)

    def handover(self, st: Station, r: int) -> None:
        """Станция переходит к первой по (arrived_at, unit_uuid) свежей стоящей ждущей; занятие в max(R, приезд)."""
        now = self.state.now
        assert now is not None
        best: tuple[int, str] | None = None
        for v in self.state.vehicles.values():
            if (
                v.phase == WAITING
                and v.target_station == st.station_uuid
                and now - v.last_ts <= self.const.fresh_s
                and self.stopped(v)
            ):
                assert v.arrived_at is not None
                key = (v.arrived_at, v.unit_uuid)
                if best is None or key < best:
                    best = key
        if best is not None:
            self.occupy(st, self.state.vehicles[best[1]], max(r, best[0]), "handover")

    def fire_timers(self) -> None:
        """Шаг 4: освободить по таймеру все станции с occupied_at + 230 ≤ now, по порядку (R, station_uuid)."""
        now = self.state.now
        assert now is not None
        occupy_s = self.const.occupy_s
        while True:
            due = [
                (st.occupied_at + occupy_s, st.station_uuid)
                for st in self.state.stations.values()
                if st.occupant_uuid is not None and st.occupied_at is not None and st.occupied_at + occupy_s <= now
            ]
            if not due:
                return
            r, uuid = min(due)
            self.release(self.state.stations[uuid], r, "timer")

    # --- геометрия ---------------------------------------------------------

    def stopped(self, v: Vehicle) -> bool:
        return v.speed_kmh < self.const.stop_kmh

    def in_radius(self, v: Vehicle, station_uuid: str) -> bool:
        return dist_to_station(self.cfg, v.lat, v.lon, station_uuid) < self.const.radius_m

    def station_in_radius(self, v: Vehicle) -> str | None:
        """Станция, в радиусе которой машина. Радиусы не пересекаются (§3.1), поэтому такая одна или её нет."""
        if self.in_radius(v, v.target_station):
            return v.target_station
        for s in self.cfg.stations:
            if s.uuid != v.target_station and self.in_radius(v, s.uuid):
                return s.uuid
        return None

    def in_dump(self, v: Vehicle) -> bool:
        d = self.cfg.dump
        return haversine(v.lat, v.lon, d.lat, d.lon) < self.const.radius_m

    # --- машина (§3.5) -----------------------------------------------------

    def transition(self, v: Vehicle, ts: int) -> None:
        """Шаг 5: строки §3.5 для фазы машины по сообщению с ts; позиция v уже обновлена."""
        if v.phase == TO_STATION:
            self._to_station(v, ts)
        elif v.phase == WAITING:
            self._waiting(v, ts)
        elif v.phase == SERVICE:
            self._service(v, ts)
        elif v.phase == SERVED:
            self._served(v)
        elif v.phase == TO_DUMP:
            self._to_dump(v)

    def _to_station(self, v: Vehicle, ts: int) -> None:
        """to_station: вход в радиус (R1); в радиусе разгрузки — конец заезда (R2); стоит в радиусе станции — приезд."""
        self._track_radius(v, ts)
        if self.in_dump(v):
            self._end_trip_at_dump(v)
        elif v.radius_station is not None and self.stopped(v):
            self._arrive(v, ts, v.radius_station)

    def _track_radius(self, v: Vehicle, ts: int) -> None:
        """R1: станция, в радиусе которой машина, — любая, не только цель — и момент входа; выход забывает вход."""
        z = self.station_in_radius(v)
        if z != v.radius_station:
            v.radius_station = z
            v.radius_entered_at = None if z is None else ts

    def _end_trip_at_dump(self, v: Vehicle) -> None:
        """R2: «к станции» в радиусе разгрузки — конец заезда: в радиусах станции и разгрузки сразу быть нельзя
        (≥ 1550 м, §3.1). fsm_resync с прежней целью — если заезд был (решение или чужая цель); первое сообщение
        машины в радиусе разгрузки — пустой заезд, без события."""
        had_trip = v.decided or v.target_station != v.home_station
        old = v.target_station
        self.end_trip(v)
        v.dump_visited = True
        if had_trip:
            self.events.append(event("fsm_resync", station=old))

    def _arrive(self, v: Vehicle, ts: int, z: str) -> None:
        """Стоит в радиусе станции z. Не цель — самолечение: цель = z и fsm_resync. Свободна — занятие в ts, занята —
        ожидание с входа в радиус (§3.11 пп. 3, 4)."""
        if z != v.target_station:
            v.target_station = z
            self.events.append(event("fsm_resync", station=z))
        st = self.state.stations[z]
        if st.occupant_uuid is None:
            v.arrived_at = ts
            self.occupy(st, v, occupy_time(ts, st), "arrival")
        else:
            v.phase = WAITING
            v.arrived_at = v.radius_entered_at

    def _waiting(self, v: Vehicle, ts: int) -> None:
        """waiting: стоит в радиусе свободной цели — занятие, приезд прежний; вне радиуса — уехала необслуженной."""
        in_target = self.in_radius(v, v.target_station)
        st = self.state.stations[v.target_station]
        if in_target and self.stopped(v) and st.occupant_uuid is None:
            self.occupy(st, v, occupy_time(ts, st), "arrival")
        elif not in_target:
            v.phase = TO_STATION
            v.radius_station = None
            v.radius_entered_at = None
            v.arrived_at = None

    def _service(self, v: Vehicle, ts: int) -> None:
        """service: вне радиуса цели — освобождение по выходу, затем конец заезда. R = max(ts, occupied_at):
        опоздавшее занятие могло сдвинуть occupied_at вперёд, и R = ts дал бы занятие отрицательной длины."""
        if not self.in_radius(v, v.target_station):
            st = self.state.stations[v.target_station]
            assert st.occupied_at is not None
            self.release(st, max(ts, st.occupied_at), "exit")
            self.end_trip(v)

    def _served(self, v: Vehicle) -> None:
        """served: вне радиуса цели — конец заезда; станцию уже освободил таймер."""
        if not self.in_radius(v, v.target_station):
            self.end_trip(v)

    def _to_dump(self, v: Vehicle) -> None:
        """to_dump: в радиусе разгрузки — разгрузилась; вышла из него — снова «к станции»."""
        if self.in_dump(v):
            v.dump_visited = True
        elif v.dump_visited:
            v.phase = TO_STATION
            v.dump_visited = False
            v.radius_station = None
            v.radius_entered_at = None

    def end_trip(self, v: Vehicle) -> None:
        """«Конец заезда» (§3.5): к разгрузке, цель — своя, решение снова возможно. Прогноз — заезда, и без занятия
        (правило разгрузки) он уходит вместе с заездом, а не достаётся следующему."""
        v.phase = TO_DUMP
        v.target_station = v.home_station
        v.decided = False
        v.dump_visited = False
        v.radius_station = None
        v.radius_entered_at = None
        v.arrived_at = None
        v.predicted_start = None
