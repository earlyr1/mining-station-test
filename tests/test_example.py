"""Пример из ТЗ: очередь S1, решение T4 → S2 с выигрышем 80, T2 получает no_gain (выигрыш 50 < 60).

Выходы сравниваются байтами: json.loads не видит порядка ключей, а §3.9 требует порядок ТЗ.
"""

import json

import pytest

from tests.helpers import NOON, at, feed, last_queue, msg, outputs, two_stations
from vq.core.decision import options
from vq.core.engine import Engine
from vq.geo import fmt_ts, haversine

EXPECTED_S1 = (
    b'{"station_uuid":"S1","at":"2026-09-15T12:00:00Z","queue":['
    b'{"unit_uuid":"T1","eta":null,"service_start":"2026-09-15T11:59:00Z","free_at":"2026-09-15T12:02:50Z","wait_seconds":0},'
    b'{"unit_uuid":"T2","eta":"2026-09-15T12:02:00Z","service_start":"2026-09-15T12:02:50Z","free_at":"2026-09-15T12:06:40Z","wait_seconds":50},'
    b'{"unit_uuid":"T3","eta":"2026-09-15T12:05:00Z","service_start":"2026-09-15T12:06:40Z","free_at":"2026-09-15T12:10:30Z","wait_seconds":100}]}'
)

# образцы выхода из раздела «Данные» ТЗ как есть, с пробелами и переносами строк
TASK_QUEUE = """{"station_uuid": "S1", "at": "2026-09-15T12:00:00Z", "queue": [
  {"unit_uuid": "T1", "eta": null, "service_start": "2026-09-15T11:59:00Z",
   "free_at": "2026-09-15T12:02:50Z", "wait_seconds": 0},
  {"unit_uuid": "T2", "eta": "2026-09-15T12:02:00Z", "service_start": "2026-09-15T12:02:50Z",
   "free_at": "2026-09-15T12:06:40Z", "wait_seconds": 50}]}"""
TASK_RECOMMENDED = """{"unit_uuid": "T4", "at": "2026-09-15T12:00:00Z", "result": "recommended",
 "from_station": "S1", "to_station": "S2", "gain_seconds": 80}"""
TASK_REJECTED = """{"unit_uuid": "T4", "at": "2026-09-15T12:00:00Z", "result": "rejected", "reason": "no_gain"}"""


def compact(sample: str) -> bytes:
    """Образец ТЗ в сериализации §3.9: без пробелов, ключи в порядке образца (json.loads его сохраняет)."""
    return json.dumps(json.loads(sample), separators=(",", ":"), ensure_ascii=False).encode()


def run_example():
    cfg = two_stations()
    e = Engine(cfg)
    results = feed(
        e,
        msg("T1", NOON - 60, at(cfg, "S1", 0.0, 15.0), speed=0.0),  # стоит в 15 м с 11:59:00
        msg("T1", NOON, at(cfg, "S1", 0.0, 15.0), speed=0.0),
        msg("T2", NOON, at(cfg, "S1", 270.0, 1_200.0)),
        msg("T3", NOON, at(cfg, "S1", 180.0, 3_000.0)),
        msg("T4", NOON, at(cfg, "S1", 90.0, 900.0)),  # между S1 и S2: 900 м до S1, 2400 м до S2
    )
    return e, results


def raw_decisions(results, unit: str) -> list[bytes]:
    return [o.value for r in results for o in r.outputs if o.topic == "decision.v1" and o.key == unit.encode()]


def test_positions_are_at_exact_task_distances():
    cfg = two_stations()
    s1, s2 = cfg.station("S1"), cfg.station("S2")
    t2, t3, t4 = at(cfg, "S1", 270.0, 1_200.0), at(cfg, "S1", 180.0, 3_000.0), at(cfg, "S1", 90.0, 900.0)
    assert haversine(*t2, s1.lat, s1.lon) == pytest.approx(1_200.0, abs=1e-6)
    assert haversine(*t3, s1.lat, s1.lon) == pytest.approx(3_000.0, abs=1e-6)
    assert haversine(*t4, s1.lat, s1.lon) == pytest.approx(900.0, abs=1e-6)
    assert haversine(*t4, s2.lat, s2.lon) == pytest.approx(2_400.0, abs=1e-6)  # T4 на дуге S1 → S2


def test_queue_s1_matches_task_bytes():
    _, results = run_example()
    raw = [o.value for r in results for o in r.outputs if o.topic == "queue.v1" and o.key == b"S1"]
    assert raw[-1] == EXPECTED_S1


def test_queue_s1_after_t2_is_task_sample():
    """Образец queue.v1 в ТЗ — две строки: такой очередь S1 становится на сообщении T2, до сообщения T3."""
    _, results = run_example()
    t2_step = results[2]
    assert [o.value for o in t2_step.outputs if o.topic == "queue.v1" and o.key == b"S1"] == [compact(TASK_QUEUE)]


def test_t4_arrival_and_wait_per_station_match_task_table():
    # таблица ТЗ: у S1 приезд 12:01:30, освободится в 12:02:50 после T1 — ждать 80 с; у S2 приезд 12:04:00, свободна.
    # Решение меняет только цель T4, а расчёт идёт по очередям без неё: после шага таблица та же, что при решении
    e, _ = run_example()
    t4 = e.state.vehicles["T4"]
    table = {o.station: (fmt_ts(o.eta), o.wait) for o in options(e.state, e.cfg, t4)}
    assert table == {"S1": ("2026-09-15T12:01:30Z", 80), "S2": ("2026-09-15T12:04:00Z", 0)}


def test_t4_recommended_to_s2_with_gain_80():
    _, results = run_example()
    assert raw_decisions(results, "T4") == [compact(TASK_RECOMMENDED)]


def test_t2_rejected_no_gain():
    _, results = run_example()
    # образец отказа в ТЗ — для T4; у T2 те же байты со своим unit_uuid
    assert raw_decisions(results, "T2") == [compact(TASK_REJECTED.replace('"T4"', '"T2"'))]
    # выигрыш 50 < 60: у S1 приезд 12:02:00, освобождение 12:02:50 → ждать 50 с; у S2 — 0
    t2_step = results[2]
    assert [dict(ev.attrs) for ev in t2_step.events if ev.kind == "decision"] == [
        {"result": "rejected", "reason": "no_gain", "gain": 50}
    ]


def test_t4_moves_to_s2_queue():
    _, results = run_example()
    s2 = last_queue(results, "S2")
    assert s2 is not None
    assert [r["unit_uuid"] for r in s2["queue"]] == ["T4"]
    assert s2["queue"][0]["eta"] == "2026-09-15T12:04:00Z"
    assert s2["queue"][0]["wait_seconds"] == 0


def test_only_t2_and_t4_get_decisions():
    # T1 с первого сообщения стоит в радиусе — занятие, не решение; T3 в 3000 м — вне зоны 1500 м
    _, results = run_example()
    assert [(d["unit_uuid"], d["result"]) for d in outputs(results, "decision.v1")] == [
        ("T2", "rejected"),
        ("T4", "recommended"),
    ]
