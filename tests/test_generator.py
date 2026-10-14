"""Симулятор и инжектор сбоев отдельно от ядра (§4.2–4.5)."""

import json
from collections import defaultdict
from pathlib import Path

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from tests.helpers import NOON, two_stations
from vq.config import load_config
from vq.core.codec import Telemetry, decision_recommended, decision_rejected, encode_telemetry
from vq.core.engine import Engine
from vq.generator.faults import FaultInjector
from vq.generator.sim import ARRIVED, DRIVE, DUMP, MANEUVER, QUEUED, SERVING, Sim
from vq.geo import haversine, step_toward

SITE = Path(__file__).resolve().parent.parent / "config" / "site.toml"


def small_sim(units: dict[str, str], dist: dict[str, float]) -> Sim:
    """S1 и S2 из two_stations; машина u — на отрезке своя станция → разгрузка в dist[u] м от станции."""
    sim = Sim(two_stations(units), seed=1, start_ts=NOON)
    for unit, d in dist.items():
        v = sim.vehicles[unit]
        st = sim.stations[v.home]
        v.lat, v.lon = step_toward(st.lat, st.lon, sim.dump[0], sim.dump[1], d)
    return sim


def d_station(sim: Sim, m: Telemetry, station: str) -> float:
    st = sim.stations[station]
    return haversine(m.lat, m.lon, st.lat, st.lon)


def tick_by_unit(sim: Sim) -> dict[str, Telemetry]:
    return {m.unit_uuid: m for m in sim.tick()}


def run_until(sim: Sim, cond, limit: int = 3000) -> None:
    for _ in range(limit):
        if cond():
            return
        sim.tick()
    raise AssertionError("условие не наступило")


def rec(unit: str, at: int, to: str = "S2", frm: str = "S1") -> bytes:
    return decision_recommended(unit, at, frm, to, 100)


# --- движение, манёвр, FIFO --------------------------------------------------


def test_maneuver_inside_radius_on_229_outside_on_230():
    sim = small_sim({"T1": "S1"}, {"T1": 23.0})
    v = sim.vehicles["T1"]
    log: dict[int, Telemetry] = {}
    start = None
    while start is None or sim.t <= start + 231:
        t = sim.t
        log[t] = tick_by_unit(sim)["T1"]
        if start is None and v.state == SERVING:
            start = v.start
    assert start == NOON + 3  # 13 м, 3 м, точка станции (arrived), затем обслуживание
    seen = {t - start: m for t, m in log.items() if t >= start}
    assert all(seen[k].speed_kmh == 0.0 and d_station(sim, seen[k], "S1") == 0.0 for k in range(0, 200))
    assert seen[200].speed_kmh == pytest.approx(6.12) and d_station(sim, seen[200], "S1") < 1e-6
    assert d_station(sim, seen[229], "S1") == pytest.approx(49.3, abs=1e-6)
    assert d_station(sim, seen[230], "S1") == pytest.approx(51.0, abs=1e-6)
    assert d_station(sim, seen[231], "S1") == pytest.approx(61.0, abs=1e-6)
    assert seen[231].speed_kmh == 36.0
    v = sim.vehicles["T1"]
    assert (v.state, v.dest) == (DRIVE, DUMP)
    assert sim.occupants()["S1"] is None  # станция освобождена таймером на +230


def test_arrival_last_short_step_is_not_a_stop_and_service_starts_next_tick():
    sim = small_sim({"T1": "S1"}, {"T1": 23.0})
    m0, m1, m2 = (tick_by_unit(sim)["T1"] for _ in range(3))
    assert [round(d_station(sim, m, "S1"), 6) for m in (m0, m1, m2)] == [13.0, 3.0, 0.0]
    assert m2.speed_kmh == pytest.approx(10.8)  # короткий шаг 3 м: не «стоит»
    assert sim.vehicles["T1"].state == ARRIVED and sim.occupants()["S1"] is None
    m3 = tick_by_unit(sim)["T1"]
    assert m3.speed_kmh == 0.0 and sim.occupants()["S1"] == "T1" and sim.vehicles["T1"].start == m3.ts


def test_fifo_by_radius_entry_then_unit_and_handover_at_release():
    # T1 приезжает первой; T3 входит в радиус раньше T2, хотя T2 < T3 по имени; T4 входит вместе с T3
    units = {u: "S1" for u in ("T1", "T2", "T3", "T4")}
    sim = small_sim(units, {"T1": 63.0, "T2": 83.0, "T3": 73.0, "T4": 73.0})
    run_until(sim, lambda: sim.vehicles["T2"].state == QUEUED)
    v = sim.vehicles
    assert (v["T3"].radius_entered_at, v["T4"].radius_entered_at, v["T2"].radius_entered_at) == (
        NOON + 2,
        NOON + 2,
        NOON + 3,
    )
    assert sim.stations["S1"].fifo == [(NOON + 2, "T3"), (NOON + 2, "T4"), (NOON + 3, "T2")]
    start = v["T1"].start
    assert start == NOON + 7
    order = [(sim.occupants()["S1"], start)]
    while sim.t <= start + 3 * 230:
        t = sim.t
        sim.tick()
        occ = sim.occupants()["S1"]
        if occ != order[-1][0]:
            order.append((occ, t))
    assert order == [("T1", start), ("T3", start + 230), ("T4", start + 460), ("T2", start + 690)]


# --- рекомендации ------------------------------------------------------------


def test_recommendation_applied_in_drive():
    sim = small_sim({"T1": "S1"}, {"T1": 1493.0})
    before = tick_by_unit(sim)["T1"]
    at = before.ts
    sim.offer_decision(rec("T1", at))
    after = tick_by_unit(sim)["T1"]
    v = sim.vehicles["T1"]
    assert (v.state, v.dest, v.redirected) == (DRIVE, "S2", True)
    assert d_station(sim, after, "S2") == pytest.approx(d_station(sim, before, "S2") - 10.0, abs=1e-6)
    assert (sim.stats.received, sim.stats.applied, sim.stats.delays) == (1, 1, [after.ts - at])


def test_decision_delay_counts_from_decision_zone_entry():
    """§4.2 п. 4: задержка поворота — от первого отсчёта ближе 1500 м к своей станции, а не от at решения."""
    sim = small_sim({"T1": "S1"}, {"T1": 1523.0})
    v = sim.vehicles["T1"]
    ms = [tick_by_unit(sim)["T1"] for _ in range(3)]
    assert [round(d_station(sim, m, "S1"), 6) for m in ms] == [1513.0, 1503.0, 1493.0]
    assert v.zone_entered_at == NOON + 2
    run_until(sim, lambda: sim.t == NOON + 8)
    sim.offer_decision(rec("T1", NOON + 7))  # ядро решило позже входа в зону: триггер был в обрыве
    sim.tick()
    assert (sim.stats.applied, sim.stats.delays) == (1, [6])  # NOON + 8 − (NOON + 2), а не NOON + 8 − at
    assert v.zone_entered_at == NOON + 2  # перенаправленная машина зону не переписывает


def test_decision_zone_resets_after_unload():
    sim = small_sim({"T1": "S1"}, {"T1": 13.0})
    v = sim.vehicles["T1"]
    sim.tick()
    assert v.zone_entered_at == NOON  # стартовала в зоне: первый же отсчёт ближе 1500 м
    run_until(sim, lambda: v.cycles == 1)
    assert v.zone_entered_at is None and v.trip_started_at == sim.t - 1
    prev = last = None
    for _ in range(2000):  # обратный путь от разгрузки до зоны — 8500 м
        prev, last = last, tick_by_unit(sim)["T1"]
        if v.zone_entered_at is not None:
            break
    assert prev is not None and last is not None and v.zone_entered_at == last.ts
    assert d_station(sim, prev, "S1") >= 1500.0 > d_station(sim, last, "S1")


def test_recommendation_applied_when_arrived():
    sim = small_sim({"T1": "S1"}, {"T1": 13.0})
    run_until(sim, lambda: sim.vehicles["T1"].state == ARRIVED)
    sim.offer_decision(rec("T1", sim.t - 1))
    m = tick_by_unit(sim)["T1"]
    v = sim.vehicles["T1"]
    assert (v.state, v.dest, v.radius_entered_at) == (DRIVE, "S2", None)
    assert d_station(sim, m, "S1") == pytest.approx(10.0, abs=1e-6) and m.speed_kmh == 36.0
    assert sim.occupants()["S1"] is None and sim.stats.applied == 1


def test_recommendation_removes_queued_vehicle_from_fifo():
    sim = small_sim({"T1": "S1", "T2": "S1"}, {"T1": 13.0, "T2": 23.0})
    run_until(sim, lambda: sim.vehicles["T2"].state == QUEUED)
    assert sim.stations["S1"].fifo == [(NOON, "T2")]
    sim.offer_decision(rec("T2", sim.t - 1))
    sim.tick()
    assert sim.stations["S1"].fifo == [] and sim.vehicles["T2"].dest == "S2"
    start = sim.vehicles["T1"].start
    run_until(sim, lambda: sim.t > start + 230)
    assert sim.occupants()["S1"] is None  # передавать некому
    run_until(sim, lambda: sim.vehicles["T2"].state == SERVING)
    assert sim.occupants()["S2"] == "T2"


def test_recommendation_refused_after_service_start_with_reasons():
    sim = small_sim({"T1": "S1"}, {"T1": 13.0})
    v = sim.vehicles["T1"]
    trip_at = NOON  # первый заезд начался на старте
    run_until(sim, lambda: v.state == SERVING)
    sim.offer_decision(rec("T1", trip_at + 1))
    sim.tick()
    run_until(sim, lambda: v.state == MANEUVER)
    sim.offer_decision(rec("T1", trip_at + 2))
    sim.tick()
    run_until(sim, lambda: v.state == DRIVE and v.dest == DUMP)
    sim.offer_decision(rec("T1", trip_at + 3))
    sim.tick()
    assert sim.stats.ignored == {"already_serving": 3}
    # разгрузка открывает новый заезд: решение прошлого заезда устарело
    run_until(sim, lambda: v.cycles == 1)
    assert v.trip_started_at == sim.t - 1 and (v.dest, v.redirected) == ("S1", False)
    sim.offer_decision(rec("T1", trip_at + 4))
    sim.offer_decision(rec("T1", v.trip_started_at))  # решение этого заезда — применяется
    sim.tick()
    sim.offer_decision(rec("T1", v.trip_started_at + 1, to="S2"))  # второе в том же заезде
    sim.tick()
    assert sim.stats.ignored == {"already_serving": 3, "stale_trip": 2}
    assert (sim.stats.received, sim.stats.applied) == (6, 1)
    assert v.dest == "S2"


def test_decision_duplicates_and_rejections():
    sim = small_sim({"T1": "S1"}, {"T1": 1493.0})
    raw = rec("T1", NOON)
    sim.offer_decision(raw)
    sim.offer_decision(raw)  # повтор по (unit_uuid, at)
    sim.offer_decision(decision_rejected("T1", NOON + 5, "no_gain"))
    sim.tick()
    s = sim.stats
    assert (s.received, s.applied, s.duplicates, s.ignored) == (2, 1, 1, {})


@pytest.mark.parametrize(
    "raw",
    [
        b"not json",
        b"[]",
        b"{}",
        b'{"unit_uuid": ["T1"], "at": "2026-09-15T12:00:00Z", "result": "rejected"}',
        b'{"unit_uuid": "T1", "at": 1789473600, "result": "rejected"}',
        b'{"unit_uuid": "T1", "at": "2026-09-15 12:00", "result": "rejected"}',
        b'{"unit_uuid": "T1", "at": "2026-09-15T12:00:00Z", "result": "maybe"}',
        b'{"unit_uuid": "T1", "at": "2026-09-15T12:00:00Z", "result": "recommended", "to_station": ["S2"]}',
        b'{"unit_uuid": "T1", "at": "2026-09-15T12:00:00Z", "result": "recommended", "to_station": "S9"}',
        b'{"unit_uuid": "T1", "at": "2026-09-15T12:00:00Z", "result": "recommended", "to_station": "S1"}',
        b'{"unit_uuid": "T-99", "at": "2026-09-15T12:00:00Z", "result": "rejected"}',
        b'{"unit_uuid": "T1", "at": "99999-01-01T00:00:00Z", "result": "rejected"}',
        b"\xff\xfe",
        # json.loads бросает RecursionError на глубокой вложенности
        pytest.param(b"[" * 100_000 + b"]" * 100_000, id="nested_array_100k"),
        pytest.param(
            b'{"unit_uuid": "T1", "at": "2026-09-15T12:00:00Z", "result": "rejected", "x": '
            + b"[" * 100_000
            + b"]" * 100_000
            + b"}",
            id="nested_field_100k",
        ),
    ],
)
def test_garbage_decision_is_counted_not_raised(raw):
    sim = small_sim({"T1": "S1"}, {"T1": 1493.0})
    sim.offer_decision(raw)
    sim.tick()
    assert (sim.stats.received, sim.stats.applied, sim.stats.ignored) == (0, 0, {"invalid": 1})
    assert sim.vehicles["T1"].dest == "S1"


JUNK = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(),
    st.floats(allow_nan=False),
    st.text(max_size=8),
    st.lists(st.integers(), max_size=2),
    st.dictionaries(st.text(max_size=3), st.integers(), max_size=2),
)


@st.composite
def near_valid_decision(draw) -> bytes:
    """Почти валидная запись decision.v1: поля иногда другого типа, неизвестные, пропущены."""

    def pick(valid):
        return draw(valid if draw(st.integers(0, 3)) else JUNK)

    fields = {
        "unit_uuid": pick(st.sampled_from(["T1", "T-99", ""])),
        "at": pick(st.sampled_from(["2026-09-15T12:00:00Z", "2026-09-15T12:00:01Z", "2026-09-15 12:00", "Z"])),
        "result": pick(st.sampled_from(["recommended", "rejected", "maybe"])),
        "to_station": pick(st.sampled_from(["S1", "S2", "S9"])),
        "reason": pick(st.sampled_from(["no_gain", "stale_telemetry"])),
    }
    keep = draw(st.lists(st.sampled_from(sorted(fields)), unique=True))
    return json.dumps({k: fields[k] for k in keep}).encode()


@settings(max_examples=300, deadline=None)
@given(st.lists(st.one_of(st.binary(max_size=64), near_valid_decision()), max_size=5))
def test_offer_decision_never_raises_and_counts_every_record(raws):
    """Любая запись decision.v1 — принята, повтор или invalid; генератор не падает и тикает дальше."""
    sim = small_sim({"T1": "S1"}, {"T1": 1493.0})
    for raw in raws:
        sim.offer_decision(raw)
    sim.tick()
    s = sim.stats
    assert s.received + s.duplicates + s.ignored["invalid"] == len(raws)
    assert s.applied + s.ignored["already_serving"] + s.ignored["stale_trip"] <= s.received


# --- старт -------------------------------------------------------------------


@pytest.mark.parametrize("seed", [1, 42, 2026])
def test_start_spread(seed):
    cfg = load_config(SITE)
    sim = Sim(cfg, seed, NOON)
    dists = {}
    for u, v in sim.vehicles.items():
        st = sim.stations[v.home]
        dists[u] = haversine(v.lat, v.lon, st.lat, st.lon)
        assert (v.state, v.dest) == (DRIVE, v.home)
    for d in dists.values():
        k = (d - 1603.0) / 10.0
        assert d >= 1603.0 - 1e-6 and d <= 9993.0 + 1e-6
        assert abs(k - round(k)) < 1e-6  # расстояние оканчивается на 3
    assert len({round(d) for d in dists.values()}) >= 30  # разброс, а не одна точка
    first = {m.unit_uuid: m for m in sim.tick()}
    for u, m in first.items():
        d = d_station(sim, m, sim.vehicles[u].home)
        assert d == pytest.approx(dists[u] - 10.0, abs=1e-6) and d > 1500.0
    assert [(v.lat, v.lon) for v in Sim(cfg, seed, NOON).vehicles.values()] != [
        (v.lat, v.lon) for v in Sim(cfg, seed + 1, NOON).vehicles.values()
    ]


def test_sim_is_deterministic():
    cfg = load_config(SITE)
    a, b = Sim(cfg, 7, NOON), Sim(cfg, 7, NOON)
    for _ in range(1500):
        assert a.tick() == b.tick()


# --- сбои --------------------------------------------------------------------


def sim_stream(ticks: int, seed: int = 3, cfg=None):
    sim = Sim(cfg or load_config(SITE), seed, NOON)
    for _ in range(ticks):
        t = sim.t
        yield t, sim.tick()


def far_from_all(cfg, m: Telemetry) -> float:
    points = [(s.lat, s.lon) for s in cfg.stations] + [(cfg.dump.lat, cfg.dump.lon)]
    return min(haversine(m.lat, m.lon, lat, lon) for lat, lon in points)


def test_fault_placement_late_and_outage_keep_clear_of_stations_and_dump():
    cfg = load_config(SITE)
    log = []
    inj = FaultInjector(cfg, 5, dup_rate=0.05, late_rate=0.05, outage_rate=0.01, on_fault=lambda *e: log.append(e))
    for t, msgs in sim_stream(3000):
        inj.push(t, msgs)
    inj.drain()
    lates = [far_from_all(cfg, m) for kind, _, m in log if kind == "late"]
    outages = [far_from_all(cfg, m) for kind, _, m in log if kind == "outage"]
    dups = [far_from_all(cfg, m) for kind, _, m in log if kind == "dup"]
    assert len(lates) > 1000 and len(outages) > 100
    assert min(lates) > 100.0 and min(outages) > 700.0
    assert min(lates) < 150.0 and min(outages) < 750.0  # проверка не пустая: сбои подходят к границам
    assert min(dups) < 50.0  # дубли — где угодно, в том числе у станций
    assert inj.faults == {"late": len(lates), "outage": len(outages), "dup": len(dups)}


def test_fault_delays_stay_within_spec_ranges():
    """§4.5: дубль — ещё раз через 0–60 с, опоздание — на 2–60 с, обрыв — 5–60 с; крайние значения достигаются."""
    cfg = load_config(SITE)
    log = []
    inj = FaultInjector(cfg, 9, dup_rate=0.05, late_rate=0.05, outage_rate=0.01, on_fault=lambda *e: log.append(e))
    sent_at: defaultdict[tuple[str, int], list[int]] = defaultdict(list)  # (unit, ts) → тики отправки
    end = None
    for t, msgs in sim_stream(4000):
        for m in inj.push(t, msgs):
            sent_at[(m.unit_uuid, m.ts)].append(t)
        end = t
    assert end is not None
    lags: defaultdict[str, list[int]] = defaultdict(list)
    for kind, t, m in log:
        if t > end - 61:  # отложенное могло не успеть уйти до конца потока
            continue
        ticks = sent_at[(m.unit_uuid, m.ts)]
        assert m.ts == t
        if kind == "dup":
            assert len(ticks) == 2 and ticks[0] == t  # оригинал — в свой тик, копия — позже
            lags[kind].append(ticks[1] - t)
        else:
            assert len(ticks) == 1  # опоздавшее и накопленное в обрыве уходят один раз
            lags[kind].append(ticks[0] - t)
    assert {k: (min(v), max(v)) for k, v in lags.items()} == {"dup": (0, 60), "late": (2, 60), "outage": (5, 60)}
    assert min(len(v) for v in lags.values()) > 300


def test_expected_drops_equal_kernel_not_newer():
    cfg = two_stations()  # 12 машин: ядро прогоняет поток быстро
    inj = FaultInjector(cfg, 11, dup_rate=0.02, late_rate=0.02, outage_rate=0.005)
    engine = Engine(cfg)
    sent = []
    for t, msgs in sim_stream(1500, seed=4, cfg=cfg):
        sent += inj.push(t, msgs)
    sent += inj.drain()
    not_newer = sum(1 for m in sent for e in engine.apply(encode_telemetry(m)).events if e.kind == "not_newer")
    assert inj.faults["dup"] > 100 and inj.faults["late"] > 100 and inj.faults["outage"] > 10
    assert len(sent) == 1500 * 12 + inj.faults["dup"]  # обрывы и опоздания досланы, дубли — сверху
    assert not_newer == inj.expected_drops > inj.faults["dup"]  # дубли отброшены все, плюс часть опозданий


def test_expected_drops_rule_on_hand_made_order():
    cfg = load_config(SITE)
    inj = FaultInjector(cfg, 1, dup_rate=0.0, late_rate=0.0, outage_rate=0.0)  # без сбоев — отправка как есть
    far = step_toward(cfg.dump.lat, cfg.dump.lon, cfg.stations[0].lat, cfg.stations[0].lon, 5000.0)
    order = [("A", 10), ("A", 10), ("B", 9), ("A", 9), ("A", 11), ("B", 9), ("B", 12)]
    out = inj.push(NOON, [Telemetry(u, ts, *far, 36.0) for u, ts in order])
    assert [(m.unit_uuid, m.ts) for m in out] == order
    assert inj.expected_drops == 3  # A@10 повтор, A@9 старое, B@9 повтор


def test_outage_flush_order():
    cfg = load_config(SITE)
    s1 = cfg.station("S1")
    far = step_toward(cfg.dump.lat, cfg.dump.lon, s1.lat, s1.lon, 5000.0)  # дальше 700 м от всего
    near = step_toward(s1.lat, s1.lon, cfg.dump.lat, cfg.dump.lon, 300.0)  # ближе 700 м к S1
    inj = FaultInjector(cfg, 2, dup_rate=1.0, late_rate=0.0, outage_rate=1.0)
    t0 = NOON
    flushed_at = None
    for t in range(t0, t0 + 70):
        out = inj.push(t, [Telemetry("A", t, *far, 36.0), Telemetry("B", t, *near, 0.0)])
        assert out[0] == Telemetry("B", t, *near, 0.0)  # сначала текущие сообщения тика
        a = [x for x in out if x.unit_uuid == "A"]
        if a:
            flushed_at = t
            assert [x.ts for x in a] == list(range(t0, t + 1))  # всё накопленное по порядку ts
            assert out[-1] == Telemetry("A", t, *far, 36.0)  # сообщение тика конца обрыва — последнее
            assert all(x.unit_uuid == "B" and x.ts <= t for x in out[1 : -len(a)])  # между ними — дубли B
            break
    assert flushed_at is not None and 5 <= flushed_at - t0 <= 60
    assert inj.faults["outage"] == 1


def test_drain_sends_scheduled_then_outage_buffers():
    cfg = load_config(SITE)
    s1 = cfg.station("S1")
    far = step_toward(cfg.dump.lat, cfg.dump.lon, s1.lat, s1.lon, 5000.0)
    near = step_toward(s1.lat, s1.lon, cfg.dump.lat, cfg.dump.lon, 300.0)
    inj = FaultInjector(cfg, 4, dup_rate=1.0, late_rate=0.0, outage_rate=1.0)
    sent = []
    for t in range(NOON, NOON + 3):  # A уходит в обрыв (≥ 5 с), у B каждое сообщение дублируется
        sent += inj.push(t, [Telemetry("A", t, *far, 36.0), Telemetry("B", t, *near, 0.0)])
    rest = inj.drain()
    tail = [m for m in rest if m.unit_uuid == "A"]
    assert [m.ts for m in tail] == [NOON, NOON + 1, NOON + 2] and rest[-3:] == tail  # буфер обрыва — последним
    assert sorted(m.ts for m in sent + rest if m.unit_uuid == "B") == sorted(2 * [NOON, NOON + 1, NOON + 2])
    assert inj.expected_drops == 3 and len(sent + rest) == 9  # три дубля B отброшены
    assert inj.drain() == []
