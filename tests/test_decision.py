"""Рекомендация (§3.8).

Строки §3.11: п. 8 (ничьи по приезду в «приедет раньше нашей»), п. 9 (без предшественника ожидание считается от
начала цепочки — последнего освобождения, §3.8 п. 2), п. 10 (триггер — первое сообщение заезда ближе 1500 м; ровно
на границе — снаружи), п. 11 (один расчёт на заезд, отказ тоже расходует, сброс после обслуживания), п. 12 (ничья при
выборе: своя → ближайшая → station_uuid). Прогноз для метрики точности при no_gain — по своей станции (§3.8 п. 5).
"""

import copy

from tests.helpers import BASE, NOON, at, feed, msg, outputs, site, two_stations
from vq.core.engine import Engine
from vq.core.model import TO_DUMP
from vq.geo import destination, haversine


def busy_s1():
    cfg = two_stations()
    e = Engine(cfg)
    feed(e, msg("T1", NOON - 60, at(cfg, "S1", 0.0, 10.0), speed=0.0))  # S1 занята до NOON+170
    return cfg, e


def decisions(results):
    return outputs(results, "decision.v1")


def decision_events(results):
    return [dict(ev.attrs) for r in results for ev in r.events if ev.kind == "decision"]


def test_trigger_first_message_inside_1500_without_outside_point():
    cfg, e = busy_s1()
    r = feed(e, msg("T4", NOON, at(cfg, "S1", 180.0, 1_400.0)))
    assert len(decisions(r)) == 1


def test_no_trigger_outside_and_radius_boundary():
    cfg, e = busy_s1()
    assert decisions(feed(e, msg("T4", NOON, at(cfg, "S1", 180.0, 1_500.01)))) == []
    assert len(decisions(feed(e, msg("T4", NOON + 1, at(cfg, "S1", 180.0, 1_499.99))))) == 1


def test_trigger_zone_exactly_decision_radius_is_outside():
    s1 = destination(*BASE, 0.0, 10_000.0)
    p = destination(*s1, 180.0, 1_500.0)
    cfg = site({"S1": s1}, {"T4": "S1"}, dump=BASE, decision_radius_m=haversine(*p, *s1))  # p ровно на границе
    e = Engine(cfg)
    assert decisions(feed(e, msg("T4", NOON, p))) == []
    assert not e.state.vehicles["T4"].decided
    assert len(decisions(feed(e, msg("T4", NOON + 1, destination(*s1, 180.0, 1_499.99))))) == 1


def test_one_decision_per_trip_reject_also_counts_and_resets_after_service():
    cfg, e = busy_s1()
    r = feed(e, msg("T5", NOON, at(cfg, "S1", 270.0, 1_400.0)), msg("T5", NOON + 1, at(cfg, "S1", 270.0, 1_390.0)))
    assert len(decisions(r)) == 1
    assert e.state.vehicles["T5"].decided
    # обслуживание и выезд → новый заезд
    feed(e, *(msg("T5", t, at(cfg, "S1", 0.0, 10.0), speed=0.0) for t in range(NOON + 300, NOON + 302)))
    feed(e, msg("T5", NOON + 900, at(cfg, "S1", 180.0, 100.0)))  # выехала из радиуса
    assert not e.state.vehicles["T5"].decided


def test_second_trip_gets_its_own_decision():
    cfg, e = busy_s1()
    d = cfg.dump
    r = feed(
        e,
        msg("T5", NOON, at(cfg, "S1", 180.0, 1_400.0)),  # решение 1
        msg("T5", NOON + 300, at(cfg, "S1", 0.0, 10.0), speed=0.0),  # обслуживание
        msg("T5", NOON + 400, at(cfg, "S1", 180.0, 100.0)),  # выезд
        msg("T5", NOON + 1_400, at(cfg, "S1", 180.0, 1_400.0)),  # «к разгрузке»: зона решения не действует
        msg("T5", NOON + 1_500, (d.lat, d.lon)),  # разгрузка
        msg("T5", NOON + 1_510, at(cfg, "S1", 180.0, 9_900.0)),  # снова «к станции»
        msg("T5", NOON + 2_400, at(cfg, "S1", 180.0, 1_400.0)),  # решение 2
    )
    assert [x["at"] for x in decisions(r)] == ["2026-09-15T12:00:00Z", "2026-09-15T12:40:00Z"]


def test_stale_telemetry_30_ok_31_rejected():
    cfg, e = busy_s1()
    feed(e, msg("T9", NOON + 100, at(cfg, "S1", 180.0, 5_000.0)))  # «сейчас» = NOON+100
    r = feed(e, msg("T4", NOON + 70, at(cfg, "S1", 180.0, 1_000.0)))  # 30 с — ещё считается
    assert decisions(r)[0].get("reason") != "stale_telemetry"
    r = feed(e, msg("T5", NOON + 69, at(cfg, "S1", 180.0, 1_000.0)))  # 31 с — отказ, расчёт израсходован
    assert decisions(r) == [
        {"unit_uuid": "T5", "at": "2026-09-15T12:01:40Z", "result": "rejected", "reason": "stale_telemetry"}
    ]
    assert e.state.vehicles["T5"].decided
    assert decision_events(r) == [{"result": "rejected", "reason": "stale_telemetry"}]


def test_decision_at_is_now_not_message_ts():
    cfg, e = busy_s1()
    feed(e, msg("T9", NOON + 20, at(cfg, "S1", 180.0, 5_000.0)))
    r = feed(e, msg("T4", NOON + 5, at(cfg, "S1", 90.0, 900.0)))  # опоздало на 15 с
    assert decisions(r)[0]["at"] == "2026-09-15T12:00:20Z"


def test_gain_59_no_gain_60_recommended():
    # S1 занята T1 с NOON-60 → свободна в NOON+170. Машина в 1100 м от S1 и подальше от S2:
    # ожидание у S1 = 170 - 110 = 60 (при ETA NOON+110), у S2 0 → gain 60 → recommended.
    cfg, e = busy_s1()
    r = feed(e, msg("T4", NOON, at(cfg, "S1", 180.0, 1_100.0)))
    assert decisions(r)[0]["result"] == "recommended" and decisions(r)[0]["gain_seconds"] == 60
    assert decision_events(r) == [{"result": "recommended", "reason": "", "gain": 60}]
    cfg, e = busy_s1()
    r = feed(e, msg("T5", NOON, at(cfg, "S1", 180.0, 1_110.0)))  # ETA NOON+111 → ожидание 59
    assert decisions(r)[0] == {
        "unit_uuid": "T5",
        "at": "2026-09-15T12:00:00Z",
        "result": "rejected",
        "reason": "no_gain",
    }
    assert decision_events(r) == [{"result": "rejected", "reason": "no_gain", "gain": 59}]
    # лучшая — S2 (ожидание 0), но при no_gain прогноз — своя: eta_home + wait_home (§3.8 п. 5)
    assert e.state.vehicles["T5"].predicted_start == NOON + 111 + 59


def test_tie_prefers_home():
    cfg = two_stations()
    e = Engine(cfg)
    r = feed(e, msg("T4", NOON, at(cfg, "S1", 90.0, 900.0)))  # обе свободны: ожидание 0 и 0 → своя
    assert decisions(r)[0].get("reason") == "no_gain"
    assert e.state.vehicles["T4"].target_station == "S1"


def test_tie_on_wait_prefers_home_even_if_other_is_nearer():
    # при ничьей ожидание 0 и 0 выигрыш 0 < 60, поэтому «своя» видна только при min_gain_s = 0
    s1 = destination(49.5, 142.0, 0.0, 10_000.0)
    s2 = destination(*s1, 90.0, 1_000.0)
    cfg = site({"S1": s1, "S2": s2}, {"T4": "S1"}, dump=(49.5, 142.0), min_gain_s=0)
    e = Engine(cfg)
    p = destination(*s1, 90.0, 800.0)  # 800 м до своей S1, 200 м до S2; обе свободны
    r = feed(e, msg("T4", NOON, p))
    assert decisions(r)[0].get("reason") == "no_gain"
    assert e.state.vehicles["T4"].target_station == "S1"


def test_trigger_counts_distance_to_home_not_to_target():
    cfg = two_stations()
    e = Engine(cfg)
    r = feed(
        e,
        msg("U1", NOON, at(cfg, "S2", 0.0, 10.0), speed=0.0),  # S2 занята
        msg("T5", NOON + 1, at(cfg, "S2", 0.0, 20.0), speed=0.0),  # T5 (своя S1) встала у S2: самолечение, ждёт
        msg("T5", NOON + 2, at(cfg, "S2", 180.0, 60.0)),  # уехала необслуженной: «к станции» S2, decided = false
    )
    v = e.state.vehicles["T5"]
    assert (v.phase, v.target_station, v.decided) == ("to_station", "S2", False)
    assert decisions(r) == []  # до своей S1 ~3300 м — вне зоны решения, хотя до цели S2 — 60 м


def three_stations(s3_lon: float):
    """Машина в P; S1 (своя) в 1000 м к северу; S2 и S3 к востоку и западу на одной параллели с P.

    Долготы S2 и S3 отличаются от P на двоичные дроби, поэтому при симметрии расстояния совпадают побитово.
    """
    p = (49.5, 142.0)
    s1 = destination(*p, 0.0, 1_000.0)
    cfg = site(
        {"S1": s1, "S2": (49.5, 142.03125), "S3": (49.5, s3_lon)},
        {"T1": "S1", "T4": "S1"},
        dump=destination(*p, 180.0, 10_000.0),
    )
    e = Engine(cfg)
    feed(e, msg("T1", NOON - 60, at(cfg, "S1", 0.0, 10.0), speed=0.0))  # своя занята до NOON+170: ждать 70 с
    return e, p


def test_tie_on_wait_prefers_nearest_station():
    e, p = three_stations(s3_lon=141.97)  # S3 ближе S2
    assert haversine(*p, 49.5, 141.97) < haversine(*p, 49.5, 142.03125)
    r = feed(e, msg("T4", NOON, p))
    assert decisions(r)[0] == {
        "unit_uuid": "T4",
        "at": "2026-09-15T12:00:00Z",
        "result": "recommended",
        "from_station": "S1",
        "to_station": "S3",
        "gain_seconds": 70,
    }


def test_tie_on_wait_and_distance_prefers_station_uuid():
    e, p = three_stations(s3_lon=141.96875)  # S2 и S3 симметричны относительно P
    assert haversine(*p, 49.5, 141.96875) == haversine(*p, 49.5, 142.03125)
    r = feed(e, msg("T4", NOON, p))
    assert (decisions(r)[0]["to_station"], decisions(r)[0]["gain_seconds"]) == ("S2", 70)


def test_next_vehicle_sees_redirected_one():
    cfg, e = busy_s1()
    # T4 перенаправлена на S2 (приедет NOON+240); T5 следом в ту же точку: S2 уже не пуста
    r = feed(e, msg("T4", NOON, at(cfg, "S1", 90.0, 900.0)), msg("T5", NOON, at(cfg, "S1", 90.0, 900.0)))
    d = decisions(r)
    assert d[0]["result"] == "recommended" and d[0]["to_station"] == "S2"
    assert d[1].get("reason") == "no_gain"  # у S2 теперь ждать 230 с, у S1 — 80


def test_tie_in_arrival_order_vs_queue_rows():
    # строка очереди с тем же приездом, вид 1 и uuid меньше — «приедет раньше нашей»
    cfg, e = busy_s1()
    r = feed(
        e,
        msg("T2", NOON + 100, at(cfg, "S1", 200.0, 1_000.0)),  # ETA NOON+200; её решение: no_gain
        msg("T3", NOON + 100, at(cfg, "S1", 180.0, 1_000.0)),  # тоже ETA NOON+200, T2 < T3
    )
    d = decisions(r)
    assert d[0].get("reason") == "no_gain"
    # предшественник T3 у S1 — T2: free_at = max(200, 170) + 230 = NOON+430 → ожидание 230; у S2 — 0
    assert d[1]["result"] == "recommended" and d[1]["gain_seconds"] == 230


def test_same_eta_with_larger_uuid_is_not_predecessor():
    cfg, e = busy_s1()
    r = feed(
        e,
        msg("T5", NOON + 100, at(cfg, "S1", 200.0, 1_000.0)),  # ETA NOON+200, T5 > T3
        msg("T3", NOON + 100, at(cfg, "S1", 180.0, 1_000.0)),  # ETA NOON+200
    )
    # для T3 предшественник у S1 — только занявшая T1 (свободна в NOON+170): ожидание 0 → no_gain
    assert [x.get("reason") for x in decisions(r)] == ["no_gain", "no_gain"]


def test_waiting_vehicle_with_arrival_equal_to_eta_is_predecessor():
    cfg = two_stations()
    e = Engine(cfg)
    feed(
        e,
        msg("T1", NOON, at(cfg, "S1", 0.0, 10.0), speed=0.0),  # занята до NOON+230
        msg("T6", NOON + 20, at(cfg, "S1", 0.0, 20.0), speed=0.0),  # ждёт с NOON+20
    )
    r = feed(e, msg("T3", NOON, at(cfg, "S1", 180.0, 200.0)))  # опоздало на 20 с: ETA NOON+20 = приезд T6
    # ждущая (вид 0) раньше едущей: предшественник T6, free_at NOON+460 → ожидание 440 (без неё было бы 210)
    assert decisions(r)[0]["gain_seconds"] == 440


def test_wait_counts_from_last_release_when_no_predecessor():
    # §3.8 п. 2: нет предшественника — prev = начало цепочки §3.7 п. 4 (последнее освобождение), а не «свободна,
    # ждать 0». Различается только у опоздавшего сообщения: ETA раньше освобождения, которое уже случилось.
    # S2 в 300 м от S1 освобождена выездом в NOON+100. T4 (своя S1, занята T1 до NOON+185) шлёт сообщение, опоздавшее
    # на 25 с, в 100 м от S2 и 400 м от S1: у S1 ETA NOON+115 → ждать 70; у S2 ETA NOON+85 → ждать 15 (а не 0).
    # Выигрыш 55 < 60 → no_gain; при «ждать 0» был бы recommended с выигрышем 70.
    s1 = destination(*BASE, 0.0, 10_000.0)
    s2 = destination(*s1, 90.0, 300.0)
    cfg = site({"S1": s1, "S2": s2}, {"T1": "S1", "T4": "S1", "U1": "S2"}, dump=BASE)
    e = Engine(cfg)
    feed(
        e,
        msg("T1", NOON - 45, at(cfg, "S1", 0.0, 10.0), speed=0.0),
        msg("U1", NOON, at(cfg, "S2", 0.0, 10.0), speed=0.0),
        msg("U1", NOON + 100, at(cfg, "S2", 0.0, 60.0), speed=6.0),  # выезд: S2 свободна с NOON+100
    )
    assert (e.state.stations["S2"].occupant_uuid, e.state.stations["S2"].last_release_at) == (None, NOON + 100)
    r = feed(e, msg("T4", NOON + 75, at(cfg, "S2", 90.0, 100.0)))
    assert decisions(r) == [
        {"unit_uuid": "T4", "at": "2026-09-15T12:01:40Z", "result": "rejected", "reason": "no_gain"}
    ]
    assert decision_events(r) == [{"result": "rejected", "reason": "no_gain", "gain": 55}]
    assert e.state.vehicles["T4"].predicted_start == NOON + 115 + 70  # прогноз по своей станции


def test_recommendation_changes_only_target_radius_entry_is_kept():
    # §3.8 п. 4: рекомендация меняет только цель; вход в радиус — геометрия, а не намерение. Рекомендованная из радиуса
    # своей S1 и вставшая там же (S1 занята T1) ждёт с первого сообщения в радиусе, а не с остановки (§3.5)
    cfg, e = busy_s1()
    r = feed(e, msg("T4", NOON, at(cfg, "S1", 180.0, 30.0), speed=20.0))  # первое сообщение — в радиусе S1, на ходу
    assert decisions(r)[0]["to_station"] == "S2"
    v = e.state.vehicles["T4"]
    after_decision = copy.copy(v)
    r = feed(e, msg("T4", NOON + 3, at(cfg, "S1", 180.0, 15.0), speed=0.0))
    assert [ev.get("station") for res in r for ev in res.events if ev.kind == "fsm_resync"] == ["S1"]
    assert (v.phase, v.target_station, v.arrived_at, v.decided) == ("waiting", "S1", NOON, True)
    assert (after_decision.target_station, after_decision.radius_station, after_decision.radius_entered_at) == (
        "S2",
        "S1",
        NOON,
    )


def test_recommended_vehicle_returns_home_after_service():
    cfg, e = busy_s1()
    feed(
        e,
        msg("T4", NOON, at(cfg, "S1", 90.0, 900.0)),  # → S2
        msg("T4", NOON + 240, at(cfg, "S2", 0.0, 10.0), speed=0.0),  # обслуживается на S2
        msg("T4", NOON + 300, at(cfg, "S2", 0.0, 60.0)),  # выезд
    )
    v = e.state.vehicles["T4"]
    assert (v.phase, v.target_station, v.decided) == (TO_DUMP, "S1", False)
    assert e.state.stations["S2"].last_release_at == NOON + 300


def test_prediction_error_event_on_occupation():
    cfg, e = busy_s1()
    feed(e, msg("T4", NOON, at(cfg, "S1", 90.0, 900.0)))  # → S2: ETA NOON+240, ожидание 0
    assert e.state.vehicles["T4"].predicted_start == NOON + 240
    r = feed(e, msg("T4", NOON + 245, at(cfg, "S2", 0.0, 10.0), speed=0.0))
    assert [ev.get("seconds") for res in r for ev in res.events if ev.kind == "prediction_error"] == [5]
    assert e.state.vehicles["T4"].predicted_start is None
    feed(e, msg("T2", NOON + 245, at(cfg, "S1", 270.0, 1_200.0)))  # no_gain: прогноз — своя станция
    assert e.state.vehicles["T2"].predicted_start == NOON + 245 + 120 + 0
