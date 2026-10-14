"""Очередь станции (§3.7).

Строки §3.11: п. 3 (очередь — по целевой станции: и едущая, и ждущая у рекомендованной станции стоят в её
очереди, а не в очереди своей), п. 4 (ждущая: eta = null, приезд = вход в радиус, ожидание сохраняется после
занятия), п. 5 (ETA от ts позиции), п. 6 (плечи «к разгрузке» складываются до округления), п. 7 (одна строка на
машину), п. 8 (ничьи: ждущая раньше едущей, затем unit_uuid), п. 9 (цепочка свободной станции от последнего
освобождения), п. 20 (ETA после разгрузки — прямо до станции).
"""

from hypothesis import given, settings
from hypothesis import strategies as st

from tests.helpers import BASE, NOON, at, feed, msg, one_station, site, two_stations
from vq.core.engine import Engine
from vq.core.model import SERVED, TO_DUMP
from vq.core.queue import all_station_rows, chain_start, station_rows
from vq.geo import destination


def rows(e, station="S1"):
    return [
        (r.unit_uuid, r.eta, r.service_start, r.free_at, r.wait_seconds) for r in station_rows(e.state, e.cfg, station)
    ]


def stand(cfg, unit, ts, dist=10.0, station="S1"):
    return msg(unit, ts, at(cfg, station, 0.0, dist), speed=0.0)


def test_composition_by_phase_and_one_row_per_vehicle():
    cfg = two_stations()
    e = Engine(cfg)
    feed(
        e,
        stand(cfg, "T5", NOON - 600),  # T5 обслужена и уехала к разгрузке
        msg("T5", NOON - 500, at(cfg, "S1", 180.0, 60.0)),
        *(stand(cfg, "T6", t) for t in range(NOON - 400, NOON - 99, 100)),  # T6 освобождена таймером, стоит: served
        stand(cfg, "T1", NOON),  # занявшая
        msg("T3", NOON, at(cfg, "S1", 180.0, 2_000.0)),  # к станции
        msg("U1", NOON, at(cfg, "S2", 180.0, 2_000.0)),  # к другой станции
        stand(cfg, "T2", NOON + 1, dist=20.0),  # ждущая
        msg("T5", NOON + 1, at(cfg, "S1", 180.0, 9_000.0)),  # к разгрузке, в 1000 м от неё
        stand(cfg, "T6", NOON + 1),
    )
    assert e.state.vehicles["T6"].phase == SERVED
    assert rows(e) == [
        ("T1", None, NOON, NOON + 230, 0),
        ("T2", None, NOON + 230, NOON + 460, 229),
        ("T3", NOON + 200, NOON + 460, NOON + 690, 260),
        ("T5", NOON + 1 + 1_100, NOON + 1_101, NOON + 1_331, 0),  # через разгрузку: 1000 + 10 000 м
    ]
    assert [r[0] for r in rows(e, "S2")] == ["U1"]


def test_chain_and_waiting_has_null_eta():
    cfg = one_station()
    e = Engine(cfg)
    feed(
        e,
        stand(cfg, "T1", NOON),
        stand(cfg, "T2", NOON + 10, dist=20.0),
        msg("T3", NOON + 10, at(cfg, "S1", 180.0, 2_000.0)),
    )
    assert rows(e) == [
        ("T1", None, NOON, NOON + 230, 0),
        ("T2", None, NOON + 230, NOON + 460, 220),
        ("T3", NOON + 210, NOON + 460, NOON + 690, 250),
    ]


def test_occupant_after_handover_keeps_accumulated_wait():
    cfg = one_station()
    e = Engine(cfg)
    feed(e, stand(cfg, "T1", NOON), *(stand(cfg, "T2", t, dist=20.0) for t in range(NOON + 10, NOON + 231)))
    assert rows(e) == [("T2", None, NOON + 230, NOON + 460, 220)]


def test_tie_arrived_before_coming_then_unit_uuid():
    cfg = one_station()
    e = Engine(cfg)
    feed(
        e,
        stand(cfg, "T1", NOON),
        msg("T2", NOON + 100, at(cfg, "S1", 180.0, 100.0)),  # ETA NOON+110
        msg("T4", NOON + 102, at(cfg, "S1", 200.0, 80.0)),  # ETA NOON+110
        stand(cfg, "T3", NOON + 110, dist=20.0),  # приехала (ждёт) в NOON+110
    )
    assert [(r[0], r[1]) for r in rows(e)] == [("T1", None), ("T3", None), ("T2", NOON + 110), ("T4", NOON + 110)]


def test_uuid_breaks_tie_between_coming_vehicles():
    cfg = one_station()
    e = Engine(cfg)
    feed(
        e,
        msg("T4", NOON + 10, at(cfg, "S1", 200.0, 1_000.0)),  # ETA NOON+110
        msg("T3", NOON + 10, at(cfg, "S1", 180.0, 1_000.0)),  # ETA NOON+110
    )
    assert [r[0] for r in rows(e)] == ["T3", "T4"]


def test_horizon_1800_inclusive_1801_excluded():
    cfg = one_station()
    e = Engine(cfg)
    feed(e, msg("T1", NOON, at(cfg, "S1", 180.0, 18_000.0)), msg("T2", NOON, at(cfg, "S1", 180.0, 18_010.0)))
    assert [r[0] for r in rows(e)] == ["T1"]


def test_freshness_30_kept_31_dropped_occupant_always_kept():
    cfg = one_station()
    e = Engine(cfg)
    feed(
        e,
        stand(cfg, "T1", NOON),
        msg("T2", NOON, at(cfg, "S1", 180.0, 3_000.0)),
        msg("T3", NOON + 30, at(cfg, "S1", 180.0, 9_000.0)),
    )
    assert [r[0] for r in rows(e)] == ["T1", "T2", "T3"]
    feed(e, msg("T3", NOON + 31, at(cfg, "S1", 180.0, 8_990.0)))
    assert [r[0] for r in rows(e)] == ["T1", "T3"]  # T2 старше 30 с, T1 занявшая — остаётся


def test_eta_counts_from_position_ts_not_now():
    cfg = one_station()
    e = Engine(cfg)
    feed(e, msg("T2", NOON, at(cfg, "S1", 180.0, 3_000.0)), msg("T3", NOON + 20, at(cfg, "S1", 180.0, 9_000.0)))
    assert rows(e)[0][:2] == ("T2", NOON + 300)


def test_to_dump_eta_via_dump_legs_summed_before_rounding():
    # разгрузка в 2000,6 м от S1; машина в 96 м от S1 к разгрузке: плечи 1904,6 и 2000,6 м.
    # Раздельное округление дало бы 190 + 200 = 390 с, сумма до округления — rhu(390,52) = 391 с.
    s1 = destination(*BASE, 0.0, 10_000.0)
    cfg = site({"S1": s1}, {"T1": "S1"}, dump=destination(*s1, 180.0, 2_000.6))
    e = Engine(cfg)
    feed(e, stand(cfg, "T1", NOON), msg("T1", NOON + 100, at(cfg, "S1", 180.0, 96.0)))
    assert rows(e)[0][1] == NOON + 100 + 391


def test_to_dump_after_dump_radius_counts_direct_to_station():
    cfg = one_station()
    e = Engine(cfg)
    d = cfg.dump
    feed(e, stand(cfg, "T1", NOON), msg("T1", NOON + 100, at(cfg, "S1", 180.0, 100.0)))
    # в 60 м сбоку от разгрузки, ещё не заехав: через разгрузку 60 + 10 000 м → 1006 с
    feed(e, msg("T1", NOON + 101, destination(d.lat, d.lon, 90.0, 60.0)))
    assert rows(e)[0][1] == NOON + 101 + 1_006
    side = destination(d.lat, d.lon, 90.0, 40.0)
    feed(e, msg("T1", NOON + 102, side))  # заехала в радиус разгрузки: прямо до станции ≈ 10 000,08 м → 1000 с
    assert e.state.vehicles["T1"].dump_visited
    assert rows(e)[0][1] == NOON + 102 + 1_000
    feed(e, msg("T1", NOON + 110, at(cfg, "S1", 180.0, 9_900.0)))  # выехала из радиуса: уже «к станции»
    assert rows(e)[0][1] == NOON + 110 + 990


def test_to_dump_that_missed_dump_radius_keeps_via_dump_eta_and_does_not_occupy():
    """Фиксирует известное ограничение (§10): пропущенный радиус разгрузки. Это буква ТЗ: «к станции» — вышла из
    радиуса 50 м разгрузки; машина, не побывавшая в нём, остаётся «к разгрузке» и на обратном пути, и у станции."""
    cfg = one_station()
    e = Engine(cfg)
    d = cfg.dump
    feed(e, stand(cfg, "T1", NOON), msg("T1", NOON + 100, at(cfg, "S1", 180.0, 60.0)))  # обслужена: к разгрузке
    feed(
        e,
        msg("T1", NOON + 1_100, destination(d.lat, d.lon, 90.0, 60.0)),  # прошла в 60 м от разгрузки
        msg("T1", NOON + 1_200, at(cfg, "S1", 180.0, 9_000.0)),  # развернулась: в 1000 м от разгрузки
    )
    v = e.state.vehicles["T1"]
    assert (v.phase, v.dump_visited) == (TO_DUMP, False)
    # ETA по-прежнему через разгрузку: 1000 + 10 000 м → 1100 с, а не прямо 9000 м → 900 с
    assert rows(e) == [("T1", NOON + 2_300, NOON + 2_300, NOON + 2_530, 0)]
    feed(e, stand(cfg, "T1", NOON + 2_100))  # встала в радиусе своей S1
    assert (v.phase, e.state.stations["S1"].occupant_uuid) == (TO_DUMP, None)
    assert rows(e) == []  # через разгрузку 10 010 + 10 000 м → 2001 с: за горизонтом


def test_free_station_chain_starts_at_last_release_not_now():
    cfg = one_station()
    e = Engine(cfg)
    feed(
        e,
        stand(cfg, "T1", NOON),
        msg("T2", NOON + 90, at(cfg, "S1", 180.0, 60.0)),  # ETA NOON+96
        msg("T1", NOON + 100, at(cfg, "S1", 0.0, 60.0)),  # выезд: освобождение в NOON+100
        msg("T3", NOON + 110, at(cfg, "S1", 180.0, 5_000.0)),  # «сейчас» = NOON+110
    )
    # от последнего освобождения: начало NOON+100, ожидание 4; подтягивание к «сейчас» дало бы NOON+110,
    # отсчёт от −∞ — NOON+96
    assert rows(e) == [
        ("T2", NOON + 96, NOON + 100, NOON + 330, 4),
        ("T3", NOON + 610, NOON + 610, NOON + 840, 0),
    ]


def test_never_used_station_chain_starts_at_arrival():
    cfg = one_station()
    e = Engine(cfg)
    feed(e, msg("T2", NOON, at(cfg, "S1", 180.0, 500.0)))
    assert rows(e) == [("T2", NOON + 50, NOON + 50, NOON + 280, 0)]


def test_served_vehicle_not_in_any_queue():
    cfg = one_station()
    e = Engine(cfg)
    feed(e, *(stand(cfg, "T1", t) for t in range(NOON, NOON + 235)))
    assert rows(e) == []


def test_redirected_vehicle_counted_at_target_only():
    cfg = two_stations()
    e = Engine(cfg)
    feed(e, stand(cfg, "T1", NOON - 60), msg("T4", NOON, at(cfg, "S1", 90.0, 900.0)))
    assert "T4" not in [r[0] for r in rows(e, "S1")]
    assert [r[0] for r in rows(e, "S2")] == ["T4"]


def test_vehicle_waiting_at_recommended_station_is_in_its_queue_not_home():
    # §3.8 п. 4: до конца обслуживания машина учитывается в очереди рекомендованной станции — и ждущей тоже
    cfg = two_stations()
    e = Engine(cfg)
    feed(
        e,
        stand(cfg, "T1", NOON - 60),
        stand(cfg, "U1", NOON, station="S2"),  # S2 занята до NOON+230
        msg("T4", NOON, at(cfg, "S1", 90.0, 900.0)),  # своя S1, рекомендована S2
        stand(cfg, "T4", NOON + 200, dist=20.0, station="S2"),  # приехала к занятой S2: ждёт
    )
    assert (e.state.vehicles["T4"].phase, e.state.vehicles["T4"].home_station) == ("waiting", "S1")
    assert rows(e, "S2") == [("U1", None, NOON, NOON + 230, 0), ("T4", None, NOON + 230, NOON + 460, 30)]
    assert rows(e, "S1") == []  # T1 уже освобождена таймером, T4 здесь не стоит


# --- свойство: инварианты очереди на случайном валидном потоке ------------------------------

CFG2 = two_stations()
UNITS = ["T1", "T2", "T3", "T4", "T5", "U1", "U2"]
STEP = st.tuples(
    st.sampled_from(UNITS),
    st.sampled_from([0, 0, 1, 1, 2, 7]),  # шаг общих часов
    st.sampled_from([0, 0, 0, 3, 31, 60]),  # опоздание сообщения
    st.sampled_from(["S1", "S2", "dump"]),
    st.floats(0.0, 360.0),
    st.sampled_from([0.0, 10.0, 30.0, 49.0, 51.0, 300.0, 1_400.0, 2_000.0, 9_000.0]),
    st.sampled_from([0.0, 0.0, 0.5, 6.0, 36.0]),
)


def check_queues(e: Engine, unit: str) -> None:
    s, c = e.state, CFG2.constants
    assert s.now is not None
    seen: list[str] = []
    for cfg_st in CFG2.stations:
        st_ = s.stations[cfg_st.uuid]
        rs = station_rows(s, CFG2, cfg_st.uuid)
        cands = rs
        if st_.occupant_uuid is not None:
            assert rs[0].unit_uuid == st_.occupant_uuid and rs[0].eta is None and rs[0].key is None
            assert rs[0].free_at == rs[0].service_start + c.occupy_s and rs[0].wait_seconds >= 0
            cands = rs[1:]
        keys = [r.key for r in cands]
        assert all(k is not None for k in keys) and keys == sorted(keys)  # type: ignore[type-var]
        prev = chain_start(s, CFG2, cfg_st.uuid)
        for r in cands:
            assert r.key is not None
            a = r.key[0]
            assert r.service_start == (a if prev is None else max(a, prev))
            assert (r.free_at, r.wait_seconds) == (r.service_start + c.occupy_s, r.service_start - a)
            assert r.eta is None or r.eta <= s.now + c.horizon_s
            v = s.vehicles[r.unit_uuid]
            assert s.now - v.last_ts <= c.fresh_s and v.phase != SERVED
            prev = r.free_at
        seen += [r.unit_uuid for r in rs]
    assert len(seen) == len(set(seen))
    # очереди без машины unit (так их видит её рекомендация): пропадает только её строка кандидата, занявшая
    # остаётся; очереди остальных станций не меняются
    rows_all, without = all_station_rows(s, CFG2), all_station_rows(s, CFG2, exclude=unit)
    for station, rs in rows_all.items():
        kept = [r.unit_uuid for r in rs if r.unit_uuid != unit or r.key is None]
        assert [r.unit_uuid for r in without[station]] == kept
        if len(kept) == len(rs):
            assert without[station] == rs


def check_predictions(e: Engine) -> None:
    """Прогноз (§3.8 п. 5) есть только у заезда с решением: конец заезда и занятие его снимают."""
    assert [u for u, v in e.state.vehicles.items() if not v.decided and v.predicted_start is not None] == []


@settings(max_examples=100, deadline=None)
@given(st.lists(STEP, min_size=20, max_size=120))
def test_queue_invariants_on_random_valid_stream(steps):
    e = Engine(CFG2)
    clock = NOON
    for unit, dt, late, where, brg, dist, speed in steps:
        clock += dt
        if where == "dump":
            pos = destination(CFG2.dump.lat, CFG2.dump.lon, brg, dist / 10.0)
        else:
            pos = at(CFG2, where, brg, dist)
        e.apply(msg(unit, clock - late, pos, speed))
        if e.state.now is not None:
            check_queues(e, unit)
            check_predictions(e)
