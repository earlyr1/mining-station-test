"""Рекомендация станции (§3.8)."""

from typing import NamedTuple

from vq.config import SiteConfig
from vq.core.codec import decision_recommended, decision_rejected
from vq.core.model import Event, Output, SiteState, Vehicle, event
from vq.core.queue import COMING_KIND, Row, all_station_rows, chain_start, dist_to_station, travel_s

DECISION_TOPIC = "decision.v1"


class Option(NamedTuple):
    """Станция для машины. Поля — в порядке выбора §3.8 п. 3: min() по ним и есть выбор."""

    wait: int
    foreign: bool  # не своя: при равном ожидании своя раньше
    dist: float
    station: str
    eta: int


def _wait_after(rows: list[Row], start: int | None, unit: str, eta: int) -> int:
    """Ожидание машины unit с приездом в eta: free_at предшественника (иначе начало цепочки start) минус eta, ≥ 0."""
    me = (eta, COMING_KIND, unit)
    prev = start
    for r in rows:
        if r.key is None or r.key < me:  # занявшая (key None) всегда раньше
            prev = r.free_at
        else:
            break
    return 0 if prev is None else max(0, prev - eta)


def options(state: SiteState, cfg: SiteConfig, v: Vehicle) -> list[Option]:
    """§3.8 п. 2: приезд и ожидание машины v у каждой станции по её последней позиции и очередям без неё."""
    rows = all_station_rows(state, cfg, exclude=v.unit_uuid)
    out = []
    for s in cfg.stations:
        dist = dist_to_station(cfg, v.lat, v.lon, s.uuid)
        eta = v.last_ts + travel_s(cfg, dist)
        wait = _wait_after(rows[s.uuid], chain_start(state, cfg, s.uuid), v.unit_uuid, eta)
        out.append(Option(wait, s.uuid != v.home_station, dist, s.uuid, eta))
    return out


def decide(state: SiteState, cfg: SiteConfig, v: Vehicle) -> tuple[Output, list[Event]]:
    """Расчёт рекомендации для машины v по её последнему сообщению. Меняет v."""
    now = state.now
    assert now is not None
    v.decided = True
    key = v.unit_uuid.encode()
    if now - v.last_ts > cfg.constants.fresh_s:
        value = decision_rejected(v.unit_uuid, now, "stale_telemetry")
        return Output(DECISION_TOPIC, key, value), [event("decision", result="rejected", reason="stale_telemetry")]
    opts = options(state, cfg, v)
    best = min(opts)
    home = next(o for o in opts if o.station == v.home_station)
    gain = home.wait - best.wait
    if best.station == v.home_station or gain < cfg.constants.min_gain_s:
        v.predicted_start = home.eta + home.wait
        value = decision_rejected(v.unit_uuid, now, "no_gain")
        return Output(DECISION_TOPIC, key, value), [event("decision", result="rejected", reason="no_gain", gain=gain)]
    v.target_station = best.station  # только цель: вход в радиус — геометрия, а не намерение (§3.5)
    v.predicted_start = best.eta + best.wait
    value = decision_recommended(v.unit_uuid, now, v.home_station, best.station, gain)
    return Output(DECISION_TOPIC, key, value), [event("decision", result="recommended", reason="", gain=gain)]
