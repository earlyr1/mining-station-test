"""Очередь станции на момент «сейчас» (§3.7) и время пути."""

from dataclasses import dataclass

from vq.config import SiteConfig
from vq.core.codec import dumps
from vq.core.model import TO_DUMP, TO_STATION, WAITING, SiteState, Vehicle
from vq.geo import fmt_ts, haversine, rhu

ARRIVED_KIND = 0  # уже на станции (ждёт в радиусе)
COMING_KIND = 1  # едет

# кандидат очереди: (приезд, вид, unit_uuid, eta); сортировка по первым трём полям, unit_uuid уникален
Candidate = tuple[int, int, str, int | None]


@dataclass(frozen=True)
class Row:
    unit_uuid: str
    eta: int | None
    service_start: int
    free_at: int
    wait_seconds: int
    key: tuple[int, int, str] | None  # (приезд, вид, unit_uuid); None у занявшей — она всегда первая


def travel_s(cfg: SiteConfig, dist_m: float) -> int:
    return rhu(dist_m / cfg.constants.speed_mps)


def dist_to_station(cfg: SiteConfig, lat: float, lon: float, station_uuid: str) -> float:
    s = cfg.station(station_uuid)
    return haversine(lat, lon, s.lat, s.lon)


def eta_to(cfg: SiteConfig, v: Vehicle, station_uuid: str) -> int:
    """ETA едущей к станции: от ts последней позиции, по прямой."""
    return v.last_ts + travel_s(cfg, dist_to_station(cfg, v.lat, v.lon, station_uuid))


def eta_via_dump(cfg: SiteConfig, v: Vehicle) -> int:
    """ETA машины «к разгрузке» до своей станции. Плечи складываются до округления."""
    if v.dump_visited:
        return eta_to(cfg, v, v.home_station)
    d = cfg.dump
    path = haversine(v.lat, v.lon, d.lat, d.lon) + cfg.dump_leg_m(v.home_station)
    return v.last_ts + travel_s(cfg, path)


def chain_start(state: SiteState, cfg: SiteConfig, station_uuid: str) -> int | None:
    """Начало цепочки: free_at занявшей, иначе последнее освобождение, иначе None (−∞)."""
    st = state.stations[station_uuid]
    if st.occupant_uuid is not None and st.occupied_at is not None:
        return st.occupied_at + cfg.constants.occupy_s
    return st.last_release_at


def candidate(cfg: SiteConfig, v: Vehicle, now: int) -> tuple[str, Candidate] | None:
    """Станция, в очередь которой попадает свежая машина v, и её ключ; None — ни в какую (§3.7 п. 2).

    Занявшая (фаза service) кандидатом не бывает: она стоит отдельной строкой.
    """
    c = cfg.constants
    if now - v.last_ts > c.fresh_s:
        return None
    if v.phase == WAITING:
        assert v.arrived_at is not None
        return v.target_station, (v.arrived_at, ARRIVED_KIND, v.unit_uuid, None)
    if v.phase == TO_STATION:
        station, eta = v.target_station, eta_to(cfg, v, v.target_station)
    elif v.phase == TO_DUMP:
        station, eta = v.home_station, eta_via_dump(cfg, v)
    else:
        return None
    if eta > now + c.horizon_s:
        return None
    return station, (eta, COMING_KIND, v.unit_uuid, eta)


def build_rows(state: SiteState, cfg: SiteConfig, station_uuid: str, cands: list[Candidate]) -> list[Row]:
    """Занявшая первой, затем кандидаты по (приезд, вид, unit_uuid) цепочкой от chain_start (§3.7 пп. 1, 3, 4)."""
    occupy_s = cfg.constants.occupy_s
    st = state.stations[station_uuid]
    rows: list[Row] = []
    if st.occupant_uuid is not None:
        o = state.vehicles[st.occupant_uuid]
        assert st.occupied_at is not None and o.arrived_at is not None
        rows.append(
            Row(o.unit_uuid, None, st.occupied_at, st.occupied_at + occupy_s, st.occupied_at - o.arrived_at, None)
        )
    cands.sort()
    prev = chain_start(state, cfg, station_uuid)
    for a, kind, unit, eta in cands:
        start = a if prev is None else max(a, prev)
        rows.append(Row(unit, eta, start, start + occupy_s, start - a, (a, kind, unit)))
        prev = start + occupy_s
    return rows


def all_station_rows(state: SiteState, cfg: SiteConfig, exclude: str | None = None) -> dict[str, list[Row]]:
    """Строки очередей всех станций за один проход по машинам; exclude — кандидат, которого не учитывать
    (сама машина при расчёте рекомендации)."""
    now = state.now
    assert now is not None
    occupants = {st.station_uuid: st.occupant_uuid for st in state.stations.values()}
    cands: dict[str, list[Candidate]] = {s.uuid: [] for s in cfg.stations}
    for v in state.vehicles.values():
        if v.unit_uuid == exclude:
            continue
        found = candidate(cfg, v, now)
        if found is None:
            continue
        bucket = cands.get(found[0])
        if bucket is not None and occupants.get(found[0]) != v.unit_uuid:
            bucket.append(found[1])
    return {s.uuid: build_rows(state, cfg, s.uuid, cands[s.uuid]) for s in cfg.stations}


def station_rows(state: SiteState, cfg: SiteConfig, station_uuid: str, exclude: str | None = None) -> list[Row]:
    """Строки очереди одной станции; exclude — как у all_station_rows."""
    return all_station_rows(state, cfg, exclude)[station_uuid]


def rows_json(rows: list[Row]) -> str:
    """Каноническая сериализация строк: так они уходят в queue.v1 и хранятся в last_published_queue."""
    return dumps(
        [
            {
                "unit_uuid": r.unit_uuid,
                "eta": None if r.eta is None else fmt_ts(r.eta),
                "service_start": fmt_ts(r.service_start),
                "free_at": fmt_ts(r.free_at),
                "wait_seconds": r.wait_seconds,
            }
            for r in rows
        ]
    )
