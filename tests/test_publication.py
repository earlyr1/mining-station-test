"""Публикация queue.v1 и формат записей (§3.9).

Строки §3.11: п. 15 (на каждое сообщение пересчитываются все очереди; сравнение без `at`; пустая
очередь публикуется; очередь меняется от таймера, свежести и горизонта без сообщений её машин).
"""

import json

from tests.helpers import BASE, NOON, at, feed, msg, site, two_stations
from vq.core.engine import QUEUE_TOPIC, Engine
from vq.geo import destination

CFG = two_stations()


def queues(results) -> list[tuple[str, dict]]:
    """(ключ записи, payload) всех записей queue.v1 по порядку."""
    return [(o.key.decode(), json.loads(o.value)) for r in results for o in r.outputs if o.topic == QUEUE_TOPIC]


def units(q: dict) -> list[str]:
    return [row["unit_uuid"] for row in q["queue"]]


def far_u1(ts: int) -> bytes:
    """Машина S2 в 30 км — за горизонтом: сообщение двигает «сейчас», в очереди S2 её нет."""
    return msg("U1", ts, at(CFG, "S2", 180.0, 30_000.0))


def started(*raws: bytes) -> tuple[Engine, list]:
    e = Engine(CFG)
    return e, feed(e, *raws)


def test_first_computation_publishes_every_station_including_empty():
    _, r = started(msg("T3", NOON, at(CFG, "S1", 180.0, 5_000.0)))
    qs = queues(r)
    assert [k for k, _ in qs] == ["S1", "S2"]  # все станции, в порядке station_uuid
    assert units(qs[0][1]) == ["T3"]
    assert qs[1][1] == {"station_uuid": "S2", "at": "2026-09-15T12:00:00Z", "queue": []}


def test_only_changed_queues_are_published():
    e, _ = started(msg("T3", NOON, at(CFG, "S1", 180.0, 5_000.0)))
    r = feed(e, msg("U2", NOON + 1, at(CFG, "S2", 180.0, 4_000.0)))
    assert [k for k, _ in queues(r)] == ["S2"]


def test_at_is_not_part_of_comparison():
    e, _ = started(msg("T3", NOON, at(CFG, "S1", 180.0, 5_000.0)))
    # едет 10 м/с: ETA от ts позиции не меняется, строки те же — новой записи нет, хотя «сейчас» другое
    r = feed(e, *(msg("T3", NOON + k, at(CFG, "S1", 180.0, 5_000.0 - 10 * k)) for k in range(1, 20)))
    assert queues(r) == []
    assert e.state.now == NOON + 19


def test_empty_queue_is_published_when_last_row_leaves():
    e, _ = started(msg("T3", NOON, at(CFG, "S1", 180.0, 5_000.0)))
    r = feed(e, far_u1(NOON + 31))  # T3 несвежая
    qs = [q for k, q in queues(r) if k == "S1"]
    assert qs == [{"station_uuid": "S1", "at": "2026-09-15T12:00:31Z", "queue": []}]
    assert e.state.stations["S1"].last_published_queue == "[]"  # «[]», а не None: опубликована пустая


def test_timer_changes_queue_without_messages_from_its_vehicles():
    e, _ = started(msg("T1", NOON, at(CFG, "S1", 0.0, 10.0), speed=0.0))
    assert queues(feed(e, far_u1(NOON + 229))) == []  # очередь S1 та же — записи нет
    r = feed(e, far_u1(NOON + 230))  # таймер T1: станция свободна, T1 — served, очередь пуста
    assert [(k, q["at"], q["queue"]) for k, q in queues(r)] == [("S1", "2026-09-15T12:03:50Z", [])]


def test_timer_handover_changes_queue_without_messages_from_its_vehicles():
    e, _ = started(
        msg("T1", NOON, at(CFG, "S1", 0.0, 10.0), speed=0.0),
        msg("T2", NOON + 10, at(CFG, "S1", 0.0, 20.0), speed=0.0),
        msg("T2", NOON + 229, at(CFG, "S1", 0.0, 20.0), speed=0.0),
    )
    r = feed(e, far_u1(NOON + 230))
    (key, q), *_ = queues(r)
    assert key == "S1" and units(q) == ["T2"]
    assert q["queue"][0] == {
        "unit_uuid": "T2",
        "eta": None,
        "service_start": "2026-09-15T12:03:50Z",
        "free_at": "2026-09-15T12:07:40Z",
        "wait_seconds": 220,
    }


def test_freshness_changes_queue_without_messages_from_its_vehicles():
    e, _ = started(msg("T3", NOON, at(CFG, "S1", 180.0, 5_000.0)))
    assert queues(feed(e, far_u1(NOON + 30))) == []  # 30 с — ещё свежая
    assert [(k, units(q)) for k, q in queues(feed(e, far_u1(NOON + 31)))] == [("S1", [])]


def test_horizon_changes_queue_without_messages_from_its_vehicles():
    e, r = started(msg("T3", NOON, at(CFG, "S1", 180.0, 18_010.0)))  # ETA NOON+1801 — за горизонтом
    assert units(queues(r)[0][1]) == []
    r = feed(e, far_u1(NOON + 1))  # горизонт сдвинулся на NOON+1801
    assert [(k, units(q)) for k, q in queues(r)] == [("S1", ["T3"])]


def test_decision_precedes_queues_and_record_keys():
    e, _ = started(msg("T1", NOON - 60, at(CFG, "S1", 0.0, 10.0), speed=0.0))
    r = feed(e, msg("T4", NOON, at(CFG, "S1", 90.0, 900.0)))  # пример задания: T4 → S2
    assert [(o.topic, o.key) for o in r[0].outputs] == [("decision.v1", b"T4"), ("queue.v1", b"S2")]


def test_late_message_publishes_with_at_equal_now():
    e, _ = started(msg("T3", NOON, at(CFG, "S1", 180.0, 5_000.0)), far_u1(NOON + 20))
    r = feed(e, msg("T5", NOON + 5, at(CFG, "S1", 180.0, 6_000.0)))  # опоздавшее, первое для T5
    ((key, q),) = queues(r)
    assert (key, q["at"], units(q)) == ("S1", "2026-09-15T12:00:20Z", ["T3", "T5"])


def test_published_event_and_stored_rows_match_record():
    e, r = started(
        msg("T1", NOON, at(CFG, "S1", 0.0, 10.0), speed=0.0), msg("T2", NOON, at(CFG, "S1", 0.0, 20.0), speed=0.0)
    )
    events = [(ev.get("station"), ev.get("length")) for res in r for ev in res.events if ev.kind == "queue_published"]
    assert events == [("S1", 1), ("S2", 0), ("S1", 2)]
    last = [o.value for res in r for o in res.outputs if o.key == b"S1"][-1]
    stored = e.state.stations["S1"].last_published_queue
    assert last.endswith(b',"queue":' + stored.encode() + b"}")


def test_serialization_is_compact_utf8_without_ascii_escapes():
    s1 = destination(*BASE, 0.0, 10_000.0)
    cfg = site({"С1": s1}, {"Т1": "С1"}, dump=BASE)  # кириллица в идентификаторах
    e = Engine(cfg)
    (out,) = e.apply(msg("Т1", NOON, (s1[0], s1[1]), speed=0.0)).outputs
    assert out.key == "С1".encode()
    assert (
        out.value
        == (
            '{"station_uuid":"С1","at":"2026-09-15T12:00:00Z","queue":[{"unit_uuid":"Т1","eta":null,'
            '"service_start":"2026-09-15T12:00:00Z","free_at":"2026-09-15T12:03:50Z","wait_seconds":0}]}'
        ).encode()
    )


def test_rows_changed_without_length_change_are_published():
    # T3 постоял 5 с на дороге: строка одна, как была, но ETA сдвинулся на 5 с — очередь изменилась, запись есть
    e, _ = started(msg("T3", NOON, at(CFG, "S1", 180.0, 5_000.0)))
    r = feed(e, msg("T3", NOON + 5, at(CFG, "S1", 180.0, 5_000.0), speed=0.0))
    assert [(k, units(q), q["queue"][0]["eta"]) for k, q in queues(r)] == [("S1", ["T3"], "2026-09-15T12:08:25Z")]
