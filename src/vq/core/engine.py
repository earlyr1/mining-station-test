"""Engine.apply: одно сообщение telemetry.v1 → выходы и события (§3.4). Чистое ядро, без I/O."""

from vq.config import SiteConfig
from vq.core.codec import parse_telemetry, queue_message
from vq.core.decision import decide
from vq.core.fsm import Fsm
from vq.core.model import TO_STATION, Event, Output, SiteState, Station, StepResult, Vehicle, event
from vq.core.queue import Row, all_station_rows, dist_to_station, rows_json

QUEUE_TOPIC = "queue.v1"
LOGIC_VERSION = "1"  # меняется вместе с правилами ядра; несовпадение с Postgres требует make reset


class Engine:
    def __init__(self, cfg: SiteConfig, state: SiteState | None = None):
        self.cfg = cfg
        self.state = state if state is not None else SiteState()
        for s in cfg.stations:
            self.state.stations.setdefault(s.uuid, Station(s.uuid))
        # кеш сериализации: станция → (строки, их JSON). rows_json — чистая функция строк, поэтому кеш не
        # влияет на выходы; он только избавляет от форматирования времени, когда очередь не изменилась
        self._json_cache: dict[str, tuple[list[Row], str]] = {}

    def apply(self, raw: bytes | None) -> StepResult:
        msg = parse_telemetry(raw, self.cfg.assignment)
        if msg is None:
            return StepResult((), (event("invalid"),))
        s = self.state
        v = s.vehicles.get(msg.unit_uuid)
        if v is not None and msg.ts <= v.last_ts:
            return StepResult((), (event("not_newer"),))
        events: list[Event] = [event("accepted")]
        outputs: list[Output] = []
        s.now = msg.ts if s.now is None else max(s.now, msg.ts)
        fsm = Fsm(s, self.cfg, events)
        fsm.fire_timers()
        if v is None:
            home = self.cfg.assignment[msg.unit_uuid]
            v = Vehicle(msg.unit_uuid, home, msg.ts, msg.lat, msg.lon, msg.speed_kmh, TO_STATION, home)
            s.vehicles[msg.unit_uuid] = v
        else:
            v.last_ts, v.lat, v.lon, v.speed_kmh = msg.ts, msg.lat, msg.lon, msg.speed_kmh
        fsm.transition(v, msg.ts)
        if (
            v.phase == TO_STATION
            and not v.decided
            and dist_to_station(self.cfg, v.lat, v.lon, v.home_station) < self.cfg.constants.decision_radius_m
        ):
            out, evs = decide(s, self.cfg, v)
            outputs.append(out)
            events.extend(evs)
        for station_uuid, rows in all_station_rows(s, self.cfg).items():
            rj = self._rows_json(station_uuid, rows)
            st = s.stations[station_uuid]
            if rj != st.last_published_queue:
                st.last_published_queue = rj
                outputs.append(Output(QUEUE_TOPIC, station_uuid.encode(), queue_message(station_uuid, s.now, rj)))
                events.append(event("queue_published", station=station_uuid, length=len(rows)))
        return StepResult(tuple(outputs), tuple(events))

    def _rows_json(self, station_uuid: str, rows: list[Row]) -> str:
        cached = self._json_cache.get(station_uuid)
        if cached is not None and cached[0] == rows:
            return cached[1]
        rj = rows_json(rows)
        self._json_cache[station_uuid] = (rows, rj)
        return rj
