"""Занятие, освобождение, передача станции, самолечение, конец заезда в радиусе разгрузки (§3.5, §3.6).

Строки §3.11: п. 1 (передача FIFO по приезду, а не по unit_uuid и не по порядку лога, в момент освобождения),
п. 2 (served не занимает повторно), п. 3 (целевая станция и самолечение — только в фазе to_station; приезд при
самолечении — вход в радиус), п. 8 (ничья в передаче — по unit_uuid), п. 13 (уехала необслуженной; «к станции»
в радиусе разгрузки — конец заезда), п. 14 (переходы не пересматриваются), п. 16 (границы радиусов станции
и разгрузки, порог «стоит», свежесть кандидата передачи), п. 19 (опоздавшие сообщения у станции: приезд, ждущая, выход).
"""

import copy

from tests.helpers import BASE, NOON, at, feed, kinds, last_queue, msg, one_station, outputs, site, two_stations
from vq.core.engine import Engine
from vq.core.model import SERVED, SERVICE, TO_DUMP, TO_STATION, WAITING
from vq.geo import bearing, destination, fmt_ts, haversine, rhu

IN = 0.0  # азимут точки внутри радиуса


def stand(cfg, unit, ts, station="S1", dist=10.0):
    return msg(unit, ts, at(cfg, station, IN, dist), speed=0.0)


def away(cfg, unit, ts, station="S1", dist=60.0, speed=6.0):
    return msg(unit, ts, at(cfg, station, IN, dist), speed=speed)


def setup():
    cfg = one_station()
    return cfg, Engine(cfg)


def setup_two():
    cfg = two_stations()
    e = Engine(cfg)
    feed(e, stand(cfg, "T1", NOON - 60))  # S1 занята: T4 с 900 м получит рекомендацию на S2
    return cfg, e


def released(results):
    return [(ev.get("station"), ev.get("reason")) for r in results for ev in r.events if ev.kind == "released"]


def occupied(results):
    return [(ev.get("unit"), ev.get("how")) for r in results for ev in r.events if ev.kind == "occupied"]


def resyncs(results):
    return [ev.get("station") for r in results for ev in r.events if ev.kind == "fsm_resync"]


def units_in(results, station):
    q = last_queue(results, station)
    assert q is not None, f"очередь {station} в этих шагах не публиковалась"
    return [row["unit_uuid"] for row in q["queue"]]


def toward(src, dst, dist):
    """Точка в dist м от src по направлению к dst."""
    return destination(*src, bearing(*src, *dst), dist)


def test_free_station_occupied_on_first_stopped_message():
    cfg, e = setup()
    feed(e, stand(cfg, "T1", NOON))
    st = e.state.stations["S1"]
    assert (st.occupant_uuid, st.occupied_at) == ("T1", NOON)
    assert e.state.vehicles["T1"].phase == SERVICE


def test_free_station_occupied_at_stop_time_with_arrival_equal_to_stop():
    cfg, e = setup()
    feed(e, msg("T1", NOON, at(cfg, "S1", 90.0, 40.0), speed=20.0), stand(cfg, "T1", NOON + 5))
    st, v = e.state.stations["S1"], e.state.vehicles["T1"]
    assert (st.occupied_at, v.arrived_at, v.radius_entered_at) == (NOON + 5, NOON + 5, NOON)


def test_busy_station_makes_vehicle_wait_from_radius_entry():
    cfg, e = setup()
    feed(
        e,
        stand(cfg, "T1", NOON),
        msg("T2", NOON + 5, at(cfg, "S1", 90.0, 40.0), speed=20.0),  # вошла в радиус на ходу
        stand(cfg, "T2", NOON + 8, dist=20.0),
    )
    v = e.state.vehicles["T2"]
    assert (v.phase, v.arrived_at) == (WAITING, NOON + 5)


def test_first_message_stopped_in_radius_of_busy_station_waits_from_that_message():
    cfg, e = setup()
    feed(e, stand(cfg, "T1", NOON), stand(cfg, "T2", NOON + 3, dist=20.0))
    v = e.state.vehicles["T2"]
    assert (v.phase, v.target_station, v.radius_entered_at, v.arrived_at) == (WAITING, "S1", NOON + 3, NOON + 3)


def test_release_by_timer_and_fifo_handover_at_release_moment():
    # FIFO по приезду расходится здесь и с порядком unit_uuid, и с порядком лога: T3 приехала раньше T2, но uuid у неё
    # больше, её первое сообщение пришло позже, а после освобождения первым приходит сообщение T2
    cfg, e = setup()
    feed(
        e,
        stand(cfg, "T1", NOON),
        stand(cfg, "T2", NOON + 20, dist=20.0),
        stand(cfg, "T3", NOON + 10, dist=30.0),  # опоздавшее, но новое для T3: приехала раньше T2
    )
    for t in range(NOON + 21, NOON + 230):
        feed(e, stand(cfg, "T2", t, dist=20.0), stand(cfg, "T3", t, dist=30.0), stand(cfg, "T1", t))
    first, *_ = feed(e, stand(cfg, "T2", NOON + 230, dist=20.0), stand(cfg, "T3", NOON + 230, dist=30.0))
    assert released([first]) == [("S1", "timer")]
    assert occupied([first]) == [("T3", "handover")]  # в шаге сообщения T2 станция уходит T3
    st = e.state.stations["S1"]
    assert (st.occupant_uuid, st.occupied_at, st.last_release_at) == ("T3", NOON + 230, NOON + 230)
    assert e.state.vehicles["T1"].phase == SERVED
    assert (e.state.vehicles["T2"].phase, e.state.vehicles["T2"].arrived_at) == (WAITING, NOON + 20)


def test_handover_tie_on_arrival_goes_to_smaller_unit_uuid():
    cfg, e = setup()
    feed(e, stand(cfg, "T1", NOON), stand(cfg, "T3", NOON + 10, dist=30.0), stand(cfg, "T2", NOON + 10, dist=20.0))
    for t in range(NOON + 11, NOON + 231):
        feed(e, stand(cfg, "T3", t, dist=30.0), stand(cfg, "T2", t, dist=20.0))
    st = e.state.stations["S1"]
    assert (st.occupant_uuid, st.occupied_at) == ("T2", NOON + 230)


def test_release_by_exit_before_timer():
    cfg, e = setup()
    feed(e, stand(cfg, "T1", NOON), away(cfg, "T1", NOON + 100))
    st = e.state.stations["S1"]
    assert (st.occupant_uuid, st.last_release_at) == (None, NOON + 100)
    assert e.state.vehicles["T1"].phase == TO_DUMP


def test_exit_exactly_at_230_is_timer_first():
    cfg, e = setup()
    results = feed(e, stand(cfg, "T1", NOON), away(cfg, "T1", NOON + 230))
    assert [reason for _, reason in released(results)] == ["timer"]
    assert e.state.vehicles["T1"].phase == TO_DUMP


def test_handover_candidate_thresholds_fresh_30_and_stopped_below_1_kmh():
    # на освобождении «сейчас» = NOON+230: T2 приехала первой, но молчит 31 с — несвежая; T4 приехала второй, но в
    # последнем сообщении едет ровно 1 км/ч — не стоит; T3 молчит ровно 30 с — свежая и стоящая, станция её
    cfg, e = setup()
    feed(
        e,
        stand(cfg, "T1", NOON),
        stand(cfg, "T2", NOON + 5, dist=20.0),
        stand(cfg, "T4", NOON + 7, dist=25.0),
        stand(cfg, "T3", NOON + 10, dist=30.0),
        stand(cfg, "T2", NOON + 199, dist=20.0),
        stand(cfg, "T3", NOON + 200, dist=30.0),
        msg("T4", NOON + 229, at(cfg, "S1", IN, 25.0), speed=1.0),
    )
    assert [e.state.vehicles[u].phase for u in ("T2", "T3", "T4")] == [WAITING] * 3
    r = feed(e, msg("T9", NOON + 230, at(cfg, "S1", 180.0, 9_000.0)))  # таймер T1 от сообщения чужой машины
    assert occupied(r) == [("T3", "handover")]
    st = e.state.stations["S1"]
    assert (st.occupant_uuid, st.occupied_at) == ("T3", NOON + 230)


def test_handover_goes_only_to_vehicles_waiting_for_that_station():
    cfg = two_stations()
    e = Engine(cfg)
    feed(e, stand(cfg, "T1", NOON), stand(cfg, "U1", NOON + 5, station="S2"))
    feed(e, *(stand(cfg, "U2", t, dist=20.0, station="S2") for t in range(NOON + 10, NOON + 230)))  # ждёт у S2
    r = feed(e, stand(cfg, "U2", NOON + 230, dist=20.0, station="S2"))  # таймер T1 освобождает S1
    assert (released(r), occupied(r)) == ([("S1", "timer")], [])  # ждущая у S2 станцию S1 не получает
    assert e.state.stations["S1"].occupant_uuid is None
    assert (e.state.vehicles["U2"].phase, e.state.stations["S2"].occupant_uuid) == (WAITING, "U1")


def test_stale_and_moving_candidates_are_skipped_in_handover():
    cfg, e = setup()
    feed(
        e,
        stand(cfg, "T1", NOON),
        stand(cfg, "T2", NOON + 1, dist=20.0),  # затем молчит: несвежая к моменту освобождения
        stand(cfg, "T3", NOON + 2, dist=30.0),
        msg("T3", NOON + 3, at(cfg, "S1", IN, 30.0), speed=3.0),  # в радиусе, но едет
        stand(cfg, "T4", NOON + 4, dist=25.0),
    )
    for t in range(NOON + 5, NOON + 231):
        feed(e, stand(cfg, "T1", t), stand(cfg, "T4", t, dist=25.0), msg("T3", t, at(cfg, "S1", IN, 30.0), speed=3.0))
    assert e.state.stations["S1"].occupant_uuid == "T4"


def test_waiting_vehicle_does_not_take_free_station_while_moving():
    cfg, e = setup()
    feed(e, stand(cfg, "T1", NOON), *(stand(cfg, "T2", t, dist=20.0) for t in range(NOON + 10, NOON + 229)))
    feed(e, msg("T2", NOON + 229, at(cfg, "S1", IN, 25.0), speed=3.0))  # в радиусе, но едет
    r = feed(e, msg("T9", NOON + 230, at(cfg, "S1", 180.0, 9_000.0)))  # таймер T1: передачи нет, T2 едет
    assert (released(r), occupied(r)) == ([("S1", "timer")], [])
    feed(e, msg("T2", NOON + 231, at(cfg, "S1", IN, 25.0), speed=3.0))  # станция свободна, T2 всё ещё едет
    st, v = e.state.stations["S1"], e.state.vehicles["T2"]
    assert (st.occupant_uuid, v.phase) == (None, WAITING)  # строка waiting требует stopped
    r = feed(e, stand(cfg, "T2", NOON + 232, dist=20.0))
    assert occupied(r) == [("T2", "arrival")]
    assert (st.occupied_at, v.arrived_at) == (NOON + 232, NOON + 10)


def test_late_stop_of_waiting_vehicle_occupies_not_before_last_release():
    # строка waiting: занятие в max(ts, последнее освобождение), как и при приезде (§3.6, §3.11 п. 19)
    cfg, e = setup()
    feed(e, stand(cfg, "T1", NOON), stand(cfg, "T2", NOON + 10, dist=20.0))  # T2 ждёт и замолкает
    r = feed(e, away(cfg, "T1", NOON + 100))  # выезд T1 в NOON+100: передачи нет — T2 несвежая
    assert (occupied(r), e.state.vehicles["T2"].phase) == ([], WAITING)
    r = feed(e, stand(cfg, "T2", NOON + 50, dist=20.0))  # опоздавшее, но новое для T2: стоит, станция свободна
    assert occupied(r) == [("T2", "arrival")]
    st, v = e.state.stations["S1"], e.state.vehicles["T2"]
    # не NOON+50: станцию до NOON+100 занимала T1; приезд не меняется
    assert (st.occupant_uuid, st.occupied_at, v.arrived_at) == ("T2", NOON + 100, NOON + 10)


def test_radius_entry_resets_when_vehicle_leaves_radius_before_stopping():
    cfg, e = setup()
    feed(
        e,
        stand(cfg, "T1", NOON),
        msg("T2", NOON + 10, at(cfg, "S1", 90.0, 40.0), speed=20.0),  # проезжает радиус
        msg("T2", NOON + 12, at(cfg, "S1", 90.0, 70.0), speed=20.0),  # вышла, не остановившись
    )
    assert e.state.vehicles["T2"].radius_entered_at is None
    feed(
        e,
        stand(cfg, "T3", NOON + 30, dist=30.0),
        msg("T2", NOON + 50, at(cfg, "S1", 90.0, 40.0), speed=20.0),  # вернулась в радиус
        stand(cfg, "T2", NOON + 52, dist=20.0),
    )
    assert (e.state.vehicles["T2"].phase, e.state.vehicles["T2"].arrived_at) == (WAITING, NOON + 50)
    for t in range(NOON + 53, NOON + 231):
        feed(e, stand(cfg, "T2", t, dist=20.0), stand(cfg, "T3", t, dist=30.0))
    assert e.state.stations["S1"].occupant_uuid == "T3"  # FIFO: T3 (NOON+30) раньше T2 (NOON+50)


def test_waiting_vehicle_moving_at_release_takes_free_station_on_next_stop_keeping_arrival():
    cfg, e = setup()
    feed(e, stand(cfg, "T1", NOON), stand(cfg, "T2", NOON + 10, dist=20.0))
    feed(e, *(stand(cfg, "T2", t, dist=20.0) for t in range(NOON + 11, NOON + 229)))
    feed(e, msg("T2", NOON + 229, at(cfg, "S1", IN, 25.0), speed=3.0))  # в радиусе, но едет
    results = feed(e, stand(cfg, "T2", NOON + 230, dist=20.0))  # таймер T1 без передачи, затем T2 встала
    assert occupied(results) == [("T2", "arrival")]
    st, v = e.state.stations["S1"], e.state.vehicles["T2"]
    assert (st.occupied_at, v.arrived_at) == (NOON + 230, NOON + 10)


def test_several_timers_in_one_step_fire_in_r_then_uuid_order_with_handover():
    cfg = two_stations()
    e = Engine(cfg)
    feed(e, stand(cfg, "U1", NOON, station="S2"), stand(cfg, "T1", NOON))  # одинаковый R = NOON+230
    for t in range(NOON + 1, NOON + 230):
        feed(e, stand(cfg, "T2", t, dist=20.0))  # ждёт у S1
    results = feed(e, msg("T5", NOON + 240, at(cfg, "S1", 180.0, 5_000.0)))  # пауза: «сейчас» прыгает на +240
    assert released(results) == [("S1", "timer"), ("S2", "timer")]
    st = e.state.stations["S1"]
    assert (st.occupant_uuid, st.occupied_at) == ("T2", NOON + 230)  # передача в момент освобождения


def test_several_timers_in_one_step_fire_by_r_before_station_uuid():
    cfg = two_stations()
    e = Engine(cfg)
    feed(e, stand(cfg, "U1", NOON, station="S2"), stand(cfg, "T1", NOON + 5))  # R: S2 = +230, S1 = +235
    results = feed(e, msg("T5", NOON + 240, at(cfg, "S1", 180.0, 5_000.0)))
    assert released(results) == [("S2", "timer"), ("S1", "timer")]


def test_handover_after_pause_happens_at_release_moment():
    cfg, e = setup()
    feed(e, stand(cfg, "T1", NOON), stand(cfg, "T2", NOON + 1, dist=20.0))
    # T2 шлёт до NOON+229, затем тишина 20 с и сообщение T2 на NOON+249 — она ещё свежая
    for t in range(NOON + 2, NOON + 230):
        feed(e, stand(cfg, "T2", t, dist=20.0))
    feed(e, stand(cfg, "T2", NOON + 249, dist=20.0))
    st = e.state.stations["S1"]
    assert (st.occupant_uuid, st.occupied_at) == ("T2", NOON + 230)


def test_handover_chain_in_one_step_when_now_jumps_over_several_timers():
    # При fresh_s = 30 цепочка в одном шаге невозможна: свежий кандидат значит, что «сейчас» с его сообщения
    # сдвинулось не больше чем на 30 с, а два таймера подряд — это ≥ 230 с. Поэтому площадка с fresh_s = 1000:
    # проверяется сам цикл шага 4 («пока есть станция с occupied_at + 230 ≤ now»).
    s1 = destination(*BASE, 0.0, 10_000.0)
    cfg = site({"S1": s1}, {f"T{i}": "S1" for i in range(1, 10)}, dump=BASE, fresh_s=1000)
    e = Engine(cfg)
    # T3 приехала раньше T2 (опоздавшее сообщение): цепочка идёт по приезду, а не по unit_uuid и не по порядку лога
    feed(e, stand(cfg, "T1", NOON), stand(cfg, "T2", NOON + 2, dist=20.0), stand(cfg, "T3", NOON + 1, dist=30.0))
    results = feed(e, msg("T9", NOON + 500, at(cfg, "S1", 180.0, 9_000.0)))
    chain = [
        (ev.kind, ev.get("unit") or ev.get("reason"))
        for r in results
        for ev in r.events
        if ev.kind in ("released", "occupied")
    ]
    assert chain == [("released", "timer"), ("occupied", "T3"), ("released", "timer"), ("occupied", "T2")]
    st = e.state.stations["S1"]
    assert (st.occupant_uuid, st.occupied_at, st.last_release_at) == ("T2", NOON + 460, NOON + 460)
    assert e.state.vehicles["T3"].phase == SERVED


def test_served_vehicle_does_not_reoccupy():
    cfg, e = setup()
    feed(e, *(stand(cfg, "T1", t) for t in range(NOON, NOON + 300)))
    assert e.state.stations["S1"].occupant_uuid is None
    assert e.state.vehicles["T1"].phase == SERVED


def test_self_heal_stop_at_other_station_means_arrival_there():
    cfg, e = setup_two()
    feed(e, msg("T4", NOON, at(cfg, "S1", 90.0, 900.0)))  # рекомендована S2
    assert e.state.vehicles["T4"].target_station == "S2"
    results = feed(e, stand(cfg, "T4", NOON + 90))  # но встала у S1, где ещё стоит T1
    assert "fsm_resync" in kinds(results)
    v = e.state.vehicles["T4"]
    assert (v.target_station, v.phase, v.arrived_at) == ("S1", WAITING, NOON + 90)


def test_self_heal_recommended_elsewhere_but_stopped_at_free_station_occupies_it():
    cfg, e = setup_two()
    feed(e, msg("T4", NOON, at(cfg, "S1", 90.0, 900.0)))  # рекомендована S2
    feed(e, away(cfg, "T1", NOON + 60))  # T1 уехала: S1 свободна
    results = feed(e, stand(cfg, "T4", NOON + 90))
    assert [ev.get("station") for r in results for ev in r.events if ev.kind == "fsm_resync"] == ["S1"]
    assert occupied(results) == [("T4", "arrival")]
    st, v = e.state.stations["S1"], e.state.vehicles["T4"]
    assert (st.occupant_uuid, st.occupied_at) == ("T4", NOON + 90)
    assert (v.phase, v.target_station, v.arrived_at, v.decided) == (SERVICE, "S1", NOON + 90, True)


def test_self_heal_arrival_is_radius_entry_not_stop():
    # ТЗ: «приезд считается наступившим в момент входа в радиус» — и у машины, вставшей у нецелевой станции.
    # T2 (цель S2) въехала в радиус S1 в t и встала в t+5; T3 (цель S1) въехала в t+2 и встала в t+4; S1 занята T1.
    # Передача — T2 раньше T3, ожидание T2 — от t. По остановке (t+5 > t+2) было бы наоборот
    cfg = two_stations()
    e = Engine(cfg)
    feed(
        e,
        msg("T3", NOON - 300, at(cfg, "S1", 180.0, 1_400.0)),  # решение заезда T3: no_gain, цель — своя S1
        stand(cfg, "T1", NOON - 60),  # S1 занята до NOON+170
        msg("T2", NOON, at(cfg, "S1", 90.0, 900.0)),  # T2 рекомендована S2 (как T4 в примере ТЗ)
    )
    t2, t3 = e.state.vehicles["T2"], e.state.vehicles["T3"]
    assert (t2.target_station, t3.target_station, t3.decided) == ("S2", "S1", True)
    t = NOON + 90
    r = feed(
        e,
        msg("T2", t, at(cfg, "S1", 90.0, 40.0), speed=20.0),  # T2 въезжает в радиус S1 на ходу
        msg("T3", t + 2, at(cfg, "S1", 180.0, 40.0), speed=20.0),
        stand(cfg, "T3", t + 4, dist=20.0),
        stand(cfg, "T2", t + 5),  # самолечение: приезд — вход в радиус S1
    )
    assert resyncs(r) == ["S1"]
    queue = last_queue(r, "S1")["queue"]
    assert [(q["unit_uuid"], q["eta"], q["service_start"], q["wait_seconds"]) for q in queue] == [
        ("T1", None, fmt_ts(NOON - 60), 0),
        ("T2", None, fmt_ts(NOON + 170), 80),  # 170 − 90: от входа в радиус, а не от остановки
        ("T3", None, fmt_ts(NOON + 400), 308),
    ]
    assert (t2.phase, t2.target_station, t2.arrived_at, t2.decided) == (WAITING, "S1", t, True)
    assert (t3.phase, t3.arrived_at) == (WAITING, t + 2)
    for ts in range(t + 6, NOON + 170):
        feed(e, stand(cfg, "T2", ts), stand(cfg, "T3", ts, dist=20.0))
    r = feed(e, stand(cfg, "T3", NOON + 170, dist=20.0))  # таймер T1
    assert (released(r), occupied(r)) == ([("S1", "timer")], [("T2", "handover")])
    assert last_queue(r, "S1")["queue"][0] == {
        "unit_uuid": "T2",
        "eta": None,
        "service_start": fmt_ts(NOON + 170),
        "free_at": fmt_ts(NOON + 400),
        "wait_seconds": 80,
    }


def test_pass_through_other_station_radius_tracks_entry_and_changes_no_output():
    # Проезд радиуса нецелевой станции на ходу только запоминает вход, выход из радиуса его забывает. Выходы те же,
    # что у потока, где машина едет на тех же расстояниях до своей цели S2, но в ~290 м в стороне от S1
    def run(off_deg: float) -> tuple[list[tuple[str, bytes, bytes]], list[tuple[str | None, int | None]]]:
        cfg, e = setup_two()
        s1, s2 = cfg.station("S1"), cfg.station("S2")
        r = feed(e, msg("T4", NOON, at(cfg, "S1", 90.0, 900.0)))  # → S2
        b = bearing(s2.lat, s2.lon, s1.lat, s1.lon) + off_deg
        track = []
        for k, d in enumerate((3_200.0, 3_260.0, 3_290.0, 3_310.0, 3_340.0, 3_360.0, 3_400.0)):  # от S2 сквозь S1
            r += feed(e, msg("T4", NOON + 10 + k, destination(s2.lat, s2.lon, b, d)))
            v = e.state.vehicles["T4"]
            assert v.target_station == "S2"
            track.append((v.radius_station, v.radius_entered_at))
        return [(o.topic, o.key, o.value) for res in r for o in res.outputs], track

    through, through_track = run(0.0)
    aside, aside_track = run(5.0)
    assert through == aside
    assert through_track == [(None, None), *[("S1", NOON + 11)] * 4, (None, None), (None, None)]
    assert aside_track == [(None, None)] * 7


def test_dump_rule_ends_trip_of_vehicle_held_to_station():
    # Модель держит машину «к станции» с решением (ждала у S2, уехала необслуженной), а та ушла на разгрузку. В радиусе
    # разгрузки и станции сразу быть нельзя (≥ 1550 м, §3.1): заход «к станции» в радиус разгрузки — конец заезда.
    # Иначе машина весь цикл — фантомная строка в очереди S2, а на подъезде к своей S1 решения нет
    cfg = two_stations()
    e = Engine(cfg)
    dump, s1, s2 = (cfg.dump.lat, cfg.dump.lon), cfg.station("S1"), cfg.station("S2")
    feed(
        e,
        stand(cfg, "T1", NOON - 60),  # S1 занята до NOON+170
        msg("T4", NOON, at(cfg, "S1", 90.0, 900.0)),  # → S2
        stand(cfg, "U1", NOON + 100, station="S2"),  # S2 занята до NOON+330
        msg("T4", NOON + 240, at(cfg, "S2", 270.0, 40.0), speed=20.0),
        stand(cfg, "T4", NOON + 245, station="S2", dist=20.0),  # ждёт у S2
        msg("T4", NOON + 260, at(cfg, "S2", 180.0, 100.0)),  # уехала необслуженной: «к станции» к S2, решение было
    )
    v = e.state.vehicles["T4"]
    assert (v.phase, v.target_station, v.decided) == (TO_STATION, "S2", True)
    r = feed(e, msg("T4", NOON + 1_200, toward(dump, (s2.lat, s2.lon), 300.0)))  # у разгрузки
    assert units_in(r, "S2") == ["T4"]  # фантом: ETA до S2 ~1020 с — в пределах горизонта
    r = feed(e, msg("T4", NOON + 1_210, toward(dump, (s2.lat, s2.lon), 20.0)))  # в радиусе разгрузки
    assert resyncs(r) == ["S2"]
    assert (v.phase, v.target_station, v.decided, v.dump_visited) == (TO_DUMP, "S1", False, True)
    assert units_in(r, "S2") == []  # фантомная строка ушла из очереди S2 в этом же шаге
    assert units_in(r, "S1") == ["T4"]  # «к разгрузке» после разгрузки — строка своей станции
    r = feed(
        e,
        msg("T4", NOON + 1_220, toward(dump, (s1.lat, s1.lon), 60.0)),  # выехала из радиуса разгрузки
        *(msg("T4", NOON + 2_050 + k, at(cfg, "S1", 180.0, 1_515.0 - 10 * k)) for k in range(5)),
    )
    assert (v.phase, v.target_station) == (TO_STATION, "S1")
    assert [d["unit_uuid"] for d in outputs(r, "decision.v1")] == ["T4"]  # ровно одно решение на подъезде
    assert "fsm_resync" not in kinds(r)


def test_dump_rule_resync_when_trip_had_decision_or_foreign_target():
    # Событие — если заезд был: решение уже считалось (T5: no_gain, цель своя) или цель чужая без решения (U1 встала
    # первым сообщением у S1 — самолечение, до своей S2 ~3300 м, решения не было); station — прежняя цель
    cfg = two_stations()
    e = Engine(cfg)
    dump = (cfg.dump.lat, cfg.dump.lon)
    feed(
        e,
        msg("T5", NOON - 300, at(cfg, "S1", 180.0, 1_400.0)),  # решение T5 при свободной S1: no_gain, цель S1
        stand(cfg, "T1", NOON),  # S1 занята
        stand(cfg, "U1", NOON + 2, dist=20.0),  # U1 (своя S2) встала у S1: самолечение, ждёт
        away(cfg, "U1", NOON + 3),  # уехала необслуженной: «к станции» к S1, решения не было
    )
    t5, u1 = e.state.vehicles["T5"], e.state.vehicles["U1"]
    assert (t5.phase, t5.target_station, t5.decided) == (TO_STATION, "S1", True)
    assert (u1.phase, u1.target_station, u1.decided) == (TO_STATION, "S1", False)
    r5 = feed(e, msg("T5", NOON + 10, dump))  # в радиусе разгрузки
    ru = feed(e, msg("U1", NOON + 11, dump))
    assert (resyncs(r5), resyncs(ru)) == (["S1"], ["S1"])
    assert [(v.phase, v.target_station, v.decided) for v in (t5, u1)] == [
        (TO_DUMP, "S1", False),
        (TO_DUMP, "S2", False),
    ]


def test_dump_rule_end_of_trip_drops_prediction_of_that_trip():
    # Прогноз — заезда (§3.8 п. 5). Заезд, кончившийся в радиусе разгрузки без занятия, уносит его с собой: иначе при
    # stale_telemetry следующего заезда (прогноза нет) занятие дало бы prediction_error против прошлого, ошибку ≈ цикл
    cfg = two_stations()
    e = Engine(cfg)
    dump, s1 = (cfg.dump.lat, cfg.dump.lon), cfg.station("S1")
    feed(e, msg("T5", NOON, at(cfg, "S1", 180.0, 1_400.0)))  # решение заезда 1: S1 свободна, no_gain
    v = e.state.vehicles["T5"]
    assert v.predicted_start == NOON + 140
    r = feed(e, msg("T5", NOON + 1_000, toward(dump, (s1.lat, s1.lon), 20.0)))  # «к станции» в радиусе разгрузки
    assert resyncs(r) == ["S1"]
    assert (v.phase, v.decided, v.predicted_start) == (TO_DUMP, False, None)
    feed(
        e,
        msg("T5", NOON + 1_010, toward(dump, (s1.lat, s1.lon), 60.0)),  # выехала из радиуса разгрузки: заезд 2
        msg("U1", NOON + 1_900, at(cfg, "S2", 180.0, 9_000.0)),  # «сейчас» = NOON+1900
    )
    r = feed(e, msg("T5", NOON + 1_860, at(cfg, "S1", 180.0, 1_450.0)))  # триггер опоздал на 40 с
    assert [d.get("reason") for d in outputs(r, "decision.v1")] == ["stale_telemetry"]
    r = feed(e, stand(cfg, "T5", NOON + 2_100))
    assert occupied(r) == [("T5", "arrival")]
    assert "prediction_error" not in kinds(r)


def test_first_message_inside_dump_radius_starts_trip_home_without_resync():
    # Генератор ставит часть машин в ≤ 47 м от разгрузки. Первое сообщение в её радиусе заканчивает пустой заезд без
    # события; строка «к разгрузке» после разгрузки считается от позиции прямо до своей станции, как у «к станции»,
    # поэтому очередь та же, что без этого правила
    cfg = one_station()
    e = Engine(cfg)
    dump, s1 = (cfg.dump.lat, cfg.dump.lon), cfg.station("S1")
    path = [toward(dump, (s1.lat, s1.lon), d) for d in (7.0, 17.0, 27.0, 37.0, 47.0, 57.0, 67.0)]
    r = []
    phases = []
    for k, p in enumerate(path):
        r += feed(e, msg("T1", NOON + k, p))
        v = e.state.vehicles["T1"]
        phases.append((v.phase, v.dump_visited, v.target_station, v.decided))
    assert "fsm_resync" not in kinds(r)
    assert phases == [(TO_DUMP, True, "S1", False)] * 5 + [(TO_STATION, False, "S1", False)] * 2
    etas = {NOON + k + rhu(haversine(*p, s1.lat, s1.lon) / 10) for k, p in enumerate(path)}
    assert len(etas) == 1  # 10 м за секунду ближе к S1: ETA не меняется, очередь публикуется один раз
    (eta,) = etas
    row = {"unit_uuid": "T1", "eta": fmt_ts(eta), "service_start": fmt_ts(eta), "free_at": fmt_ts(eta + 230)}
    assert [q["queue"] for q in outputs(r, "queue.v1")] == [[{**row, "wait_seconds": 0}]]


def close_pair():
    """S2 в 120 м к востоку от S1: из радиуса одной станции можно сразу встать в радиусе другой."""
    s1 = destination(*BASE, 0.0, 10_000.0)
    s2 = destination(*s1, 90.0, 120.0)
    return site({"S1": s1, "S2": s2}, {"T1": "S1", "T2": "S1", "U1": "S2"}, dump=BASE)


def test_jump_between_station_radii_takes_entry_of_the_new_radius():
    # Между двумя сообщениями (долгий пропуск) машина из радиуса S1 попала прямо в радиус S2: вход — ts первого
    # сообщения в радиусе S2, а не вход в радиус S1 и не остановка; вставшая у занятой S2, она ждёт с него
    cfg = close_pair()
    e = Engine(cfg)
    feed(
        e,
        msg("T2", NOON - 300, at(cfg, "S1", 180.0, 1_400.0)),  # решение заезда израсходовано (no_gain), цель S1
        msg("T2", NOON, at(cfg, "S1", 270.0, 30.0), speed=20.0),  # в радиусе своей S1, на ходу
        stand(cfg, "U1", NOON + 50, station="S2"),  # S2 занята
    )
    v = e.state.vehicles["T2"]
    in_s1 = copy.copy(v)
    feed(e, msg("T2", NOON + 100, at(cfg, "S2", 90.0, 30.0), speed=20.0))  # следующее сообщение — сразу у S2
    in_s2 = copy.copy(v)
    r = feed(e, stand(cfg, "T2", NOON + 103, station="S2", dist=20.0))
    assert resyncs(r) == ["S2"]
    assert (v.phase, v.target_station, v.arrived_at) == (WAITING, "S2", NOON + 100)
    assert (in_s1.radius_station, in_s1.radius_entered_at) == ("S1", NOON)
    assert (in_s2.radius_station, in_s2.radius_entered_at) == ("S2", NOON + 100)


def test_self_heal_does_not_apply_to_waiting_vehicle():
    # §3.5: самолечение — только в фазе to_station. Ждущая, вставшая у другой станции, сначала «уехала
    # необслуженной» (to_station к прежней цели), и лишь следующее сообщение — приезд к другой станции
    cfg = close_pair()
    e = Engine(cfg)
    feed(e, msg("T2", NOON - 300, at(cfg, "S1", 180.0, 1_400.0)))  # решение заезда израсходовано (no_gain)
    assert e.state.vehicles["T2"].decided
    feed(e, stand(cfg, "T1", NOON), stand(cfg, "T2", NOON + 10, dist=20.0))
    v = e.state.vehicles["T2"]
    assert (v.phase, v.arrived_at) == (WAITING, NOON + 10)
    r = feed(e, stand(cfg, "T2", NOON + 30, station="S2"))  # стоит уже в радиусе S2
    assert "fsm_resync" not in kinds(r) and occupied(r) == []
    assert (v.phase, v.target_station, v.arrived_at, v.radius_entered_at) == (TO_STATION, "S1", None, None)
    r = feed(e, stand(cfg, "T2", NOON + 31, station="S2"))
    assert [ev.get("station") for res in r for ev in res.events if ev.kind == "fsm_resync"] == ["S2"]
    assert occupied(r) == [("T2", "arrival")]
    assert (e.state.stations["S2"].occupant_uuid, e.state.stations["S2"].occupied_at) == ("T2", NOON + 31)
    assert (v.phase, v.target_station, v.arrived_at) == (SERVICE, "S2", NOON + 31)


def test_passing_other_station_at_speed_changes_nothing():
    cfg, e = setup_two()
    feed(e, msg("T4", NOON, at(cfg, "S1", 90.0, 900.0)))
    results = feed(e, msg("T4", NOON + 90, at(cfg, "S1", IN, 10.0), speed=36.0))
    assert "fsm_resync" not in kinds(results)
    assert e.state.vehicles["T4"].target_station == "S2"
    assert e.state.stations["S1"].occupant_uuid == "T1"


def test_waiting_vehicle_leaving_goes_back_to_station_phase():
    cfg, e = setup()
    feed(e, stand(cfg, "T1", NOON), stand(cfg, "T2", NOON + 1, dist=20.0), away(cfg, "T2", NOON + 2))
    v = e.state.vehicles["T2"]
    assert (v.phase, v.target_station, v.arrived_at) == (TO_STATION, "S1", None)
    assert (v.radius_station, v.radius_entered_at) == (None, None)
    # вернулась и встала у всё ещё занятой S1: приезд — новый вход в радиус
    feed(e, msg("T2", NOON + 10, at(cfg, "S1", 90.0, 40.0), speed=20.0), stand(cfg, "T2", NOON + 12, dist=20.0))
    assert (v.phase, v.arrived_at) == (WAITING, NOON + 10)


def test_cycle_after_service_goes_to_dump_and_back_to_station_phase():
    cfg, e = setup()
    d = cfg.dump
    feed(e, stand(cfg, "T1", NOON), away(cfg, "T1", NOON + 200))
    v = e.state.vehicles["T1"]
    assert (v.phase, v.target_station, v.decided, v.dump_visited, v.arrived_at) == (TO_DUMP, "S1", False, False, None)
    assert (v.radius_station, v.radius_entered_at) == (None, None)  # конец заезда стирает вход
    feed(e, msg("T1", NOON + 300, at(cfg, "S1", 180.0, 5_000.0)), stand(cfg, "T1", NOON + 301, dist=30.0))
    assert v.phase == TO_DUMP  # дорога и даже остановка у станции в фазе «к разгрузке» ничего не значат
    feed(e, msg("T1", NOON + 400, (d.lat, d.lon)), msg("T1", NOON + 401, (d.lat, d.lon), speed=0.0))
    assert (v.phase, v.dump_visited) == (TO_DUMP, True)
    feed(e, msg("T1", NOON + 410, at(cfg, "S1", 180.0, 9_900.0)))  # выехала из радиуса разгрузки
    assert (v.phase, v.dump_visited, v.radius_entered_at, v.target_station) == (TO_STATION, False, None, "S1")
    assert v.radius_station is None


def test_late_exit_after_timer_changes_only_own_phase():
    cfg, e = setup()
    feed(e, stand(cfg, "T1", NOON), stand(cfg, "T2", NOON + 1, dist=20.0))
    for t in range(NOON + 2, NOON + 240):
        feed(e, stand(cfg, "T2", t, dist=20.0))
    assert e.state.stations["S1"].occupant_uuid == "T2"
    feed(e, away(cfg, "T1", NOON + 100))  # опоздавший выход T1 с ts раньше освобождения
    st = e.state.stations["S1"]
    assert (st.occupant_uuid, st.occupied_at) == ("T2", NOON + 230)
    assert e.state.vehicles["T1"].phase == TO_DUMP


def test_late_exit_before_waiter_entry_gives_zero_not_negative_wait():
    cfg, e = setup()
    feed(
        e,
        stand(cfg, "T1", NOON),
        msg("T2", NOON + 50, at(cfg, "S1", 90.0, 40.0), speed=20.0),
        stand(cfg, "T2", NOON + 51, dist=20.0),
        away(cfg, "T1", NOON + 40),  # опоздавший выход: T1 уехала раньше, чем T2 вошла в радиус
    )
    st = e.state.stations["S1"]
    assert (st.occupant_uuid, st.occupied_at) == ("T2", NOON + 50)
    assert e.state.vehicles["T2"].arrived_at == NOON + 50  # ожидание 0, не −10


def test_late_stop_earlier_than_last_release_occupies_at_release():
    cfg, e = setup()
    feed(e, stand(cfg, "T1", NOON), away(cfg, "T1", NOON + 100))  # освобождение в +100
    feed(e, stand(cfg, "T2", NOON + 90, dist=20.0))  # первое сообщение T2 опоздало: стоит с +90
    st = e.state.stations["S1"]
    assert (st.occupant_uuid, st.occupied_at) == ("T2", NOON + 100)


def test_late_exit_before_late_occupation_releases_at_occupation_not_earlier():
    # Отступление от буквы §3.6 (R = ts): выход не раньше занятия, иначе занятие отрицательной длины
    cfg, e = setup()
    feed(e, stand(cfg, "T1", NOON), away(cfg, "T1", NOON + 100))  # освобождение в +100
    feed(e, stand(cfg, "T2", NOON + 90, dist=20.0))  # опоздавшее стояние → занятие в +100
    results = feed(e, away(cfg, "T2", NOON + 95))  # опоздавший выход T2 с ts раньше её занятия
    st = e.state.stations["S1"]
    assert released(results) == [("S1", "exit")]
    assert (st.occupant_uuid, st.last_release_at) == (None, NOON + 100)


def test_radius_boundary_exactly_radius_is_outside():
    s1 = destination(*BASE, 0.0, 10_000.0)
    p = destination(*s1, 45.0, 50.0)
    cfg = site({"S1": s1}, {"T1": "S1"}, dump=BASE, radius_m=haversine(*p, *s1))  # точка ровно на радиусе
    e = Engine(cfg)
    feed(e, msg("T1", NOON, p, speed=0.0))
    v = e.state.vehicles["T1"]
    assert (v.phase, v.radius_entered_at) == (TO_STATION, None)
    feed(e, msg("T1", NOON + 1, destination(*s1, 45.0, 49.99), speed=0.0))
    assert e.state.stations["S1"].occupant_uuid == "T1"


def test_gps_jitter_past_radius_in_service_is_exit_and_hands_station_over():
    """Фиксирует известное ограничение (§10): дрожание GPS у границы 50 м. Это буква ТЗ: станция освобождается, когда
    занявшая вышла из радиуса 50 м; порог строгий, гистерезиса нет (§3.11 п. 16) — одна точка в 51 м и есть выход."""
    cfg, e = setup()
    feed(e, stand(cfg, "T1", NOON), stand(cfg, "T2", NOON + 10, dist=20.0))  # T1 занимает S1, T2 ждёт
    r = feed(
        e,
        msg("T1", NOON + 30, at(cfg, "S1", IN, 51.0), speed=0.0),  # T1 стоит, а точка GPS ушла на 51 м
        *(stand(cfg, "T1", t) for t in range(NOON + 31, NOON + 34)),  # и вернулась в 10 м
    )
    assert (released(r), occupied(r)) == ([("S1", "exit")], [("T2", "handover")])
    st = e.state.stations["S1"]
    assert (st.occupant_uuid, st.occupied_at, st.last_release_at) == ("T2", NOON + 30, NOON + 30)
    # T1 «к разгрузке»: станцию не занимает и не ждёт; её ETA через разгрузку (≈ 2000 с) за горизонтом
    assert e.state.vehicles["T1"].phase == TO_DUMP
    assert units_in(r, "S1") == ["T2"]


def test_dump_radius_boundary_exactly_radius_is_outside():
    s1 = destination(*BASE, 0.0, 10_000.0)
    p = destination(*BASE, 45.0, 50.0)
    cfg = site({"S1": s1}, {"T1": "S1"}, dump=BASE, radius_m=haversine(*p, *BASE))  # p ровно на радиусе разгрузки
    e = Engine(cfg)
    feed(e, stand(cfg, "T1", NOON), away(cfg, "T1", NOON + 100))
    v = e.state.vehicles["T1"]
    assert v.phase == TO_DUMP
    feed(e, msg("T1", NOON + 1_000, p))
    assert (v.phase, v.dump_visited) == (TO_DUMP, False)
    feed(e, msg("T1", NOON + 1_001, destination(*BASE, 45.0, 49.99)))
    assert (v.phase, v.dump_visited) == (TO_DUMP, True)


def test_stopped_threshold_is_strict():
    cfg, e = setup()
    feed(e, msg("T1", NOON, at(cfg, "S1", IN, 10.0), speed=1.0))  # 1 км/ч — ещё едет
    assert e.state.vehicles["T1"].phase == TO_STATION
    feed(e, msg("T1", NOON + 1, at(cfg, "S1", IN, 10.0), speed=0.99))
    assert e.state.vehicles["T1"].phase == SERVICE
