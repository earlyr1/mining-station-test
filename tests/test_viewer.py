"""Витрина без Kafka и Postgres: свёртка топиков, сборка /api/state, пометки источников, метрики, маршруты, страница."""

import asyncio
import json
import logging
import math
import re
import shutil
import subprocess
import threading
import time
from types import SimpleNamespace

import pytest
import uvicorn
from prometheus_client import CollectorRegistry, generate_latest

from tests.helpers import NOON, two_stations
from vq.core.model import event
from vq.dispatcher.metrics import DispatcherMetrics
from vq.generator.metrics import GeneratorMetrics
from vq.geo import fmt_ts
from vq.logs import JsonFormatter
from vq.viewer import app as va
from vq.viewer import sources as vs

QUEUE, DECISION = "queue.v1", "decision.v1"
WALL = 2_000_000_000.0  # время стены для build_state


def hdr(src: int) -> list[tuple[str, bytes]]:
    return [("vq-src-offset", str(src).encode())]


def qrow(unit: str, at: int, i: int = 0) -> dict:
    return {"unit_uuid": unit, "eta": None, "service_start": fmt_ts(at), "free_at": fmt_ts(at + 230), "wait_seconds": i}


def qrec(station: str, at: int, units: list[str]) -> bytes:
    rows = [qrow(u, at, i) for i, u in enumerate(units)]
    return json.dumps({"station_uuid": station, "at": fmt_ts(at), "queue": rows}).encode()


def drec(unit: str, at: int, result: str = "rejected", **extra) -> bytes:
    body = {"unit_uuid": unit, "at": fmt_ts(at), "result": result}
    body.update({"reason": "no_gain"} if result == "rejected" and not extra else extra)
    return json.dumps(body).encode()


class Feed:
    """Записи топиков подряд; offset в каждом топике собственный."""

    def __init__(self) -> None:
        self.cache = vs.StreamCache()
        self.offsets = {QUEUE: 0, DECISION: 0}

    def q(self, src: int, station: str, at: int, units: list[str]) -> None:
        self._put(QUEUE, qrec(station, at, units), src)

    def d(self, src: int, unit: str, at: int, result: str = "rejected", **extra) -> None:
        self._put(DECISION, drec(unit, at, result, **extra), src)

    def raw(self, topic: str, value: bytes, src: int) -> None:
        self._put(topic, value, src)

    def _put(self, topic: str, value: bytes, src: int) -> None:
        self.cache.ingest(topic, self.offsets[topic], value, hdr(src))
        self.offsets[topic] += 1

    def snap(self, ok: bool = True, error: str | None = None) -> vs.Snapshot:
        return vs.Snapshot(ok, self.cache.data(), error, WALL - 0.2)


def vehicle(unit: str, home: str, target: str, phase: str, last_ts: int, lat: float, speed: float = 0.0) -> dict:
    return {
        "unit_uuid": unit,
        "home_station": home,
        "target_station": target,
        "phase": phase,
        "last_ts": last_ts,
        "lat": lat,
        "lon": 142.6601,
        "speed_kmh": speed,
        "arrived_at": NOON if phase in ("waiting", "service") else None,
    }


def pg_snapshot(next_offset: int = 101, now_ts: int = NOON + 40, not_newer: int = 7, vehicles=None) -> vs.Snapshot:
    proc = {
        "site_id": "test",
        "epoch": 3,
        "next_offset": next_offset,
        "now_ts": now_ts,
        "logic_version": "1",
        "config_hash": "h",
        "msgs_accepted": 1000,
        "msgs_not_newer": not_newer,
        "msgs_invalid": 2,
        "updated_at": "2033-05-18T03:33:19.500000Z",
        "updated_at_epoch": WALL - 0.5,
    }
    stations = (
        {"station_uuid": "S1", "occupant_uuid": "T1", "occupied_at": NOON, "last_release_at": NOON - 5},
        {"station_uuid": "S2", "occupant_uuid": None, "occupied_at": None, "last_release_at": None},
    )
    if vehicles is None:
        vehicles = (
            vehicle("T1", "S1", "S1", "service", NOON + 40, 49.2, 0.0),
            vehicle("T2", "S1", "S2", "to_station", NOON + 9, 49.1, 36.0),
        )
    return vs.Snapshot(True, vs.PgData(proc, stations, tuple(vehicles)), None, WALL - 0.3)


def ok(data) -> vs.Snapshot:
    return vs.Snapshot(True, data, None, WALL - 0.1)


def build(feed: Feed | None = None, **snaps) -> dict:
    kafka = snaps.pop("kafka", feed.snap() if feed is not None else None)
    return va.build_state(two_stations(), vs.SourceSnapshots(kafka=kafka, **snaps), WALL)


def station(state: dict, uuid: str) -> dict:
    return next(s for s in state["stations"] if s["uuid"] == uuid)


# --- правила потребителя -------------------------------------------------------------


def test_keeps_greatest_src_offset_per_station_and_drops_replayed_tail():
    f = Feed()
    f.q(10, "S1", NOON, ["T1"])
    f.q(11, "S2", NOON, ["U1"])
    f.q(12, "S1", NOON + 1, ["T1", "T2"])
    # dispatcher упал и повторил хвост: те же записи и те же vq-src-offset
    f.q(11, "S2", NOON, ["U1"])
    f.q(12, "S1", NOON + 1, ["T1", "T2"])
    f.q(13, "S2", NOON + 2, [])
    st = build(f, pg=pg_snapshot())
    s1, s2 = station(st, "S1"), station(st, "S2")
    assert s1["queue"]["src_offset"] == 12 and [r["unit_uuid"] for r in s1["queue"]["rows"]] == ["T1", "T2"]
    assert s2["queue"]["src_offset"] == 13 and s2["queue"]["rows"] == []  # пустая очередь — тоже очередь
    assert st["health"]["stream"]["repeats"] == {QUEUE: 2, DECISION: 0}


def test_greater_at_is_not_the_rule_src_offset_is():
    """Очередь станции публикуется несколько раз в одну секунду, at одинаковый: решает vq-src-offset."""
    f = Feed()
    f.q(20, "S1", NOON, ["T1"])
    f.q(21, "S1", NOON, ["T1", "T3"])
    f.q(21, "S1", NOON, ["T1", "T3"])
    st = build(f)
    assert [r["unit_uuid"] for r in station(st, "S1")["queue"]["rows"]] == ["T1", "T3"]


def test_queue_rows_carry_home_station_for_redirect_marks():
    f = Feed()
    f.q(5, "S2", NOON, ["U1", "T4"])
    rows = station(build(f), "S2")["queue"]["rows"]
    assert [(r["unit_uuid"], r["home"]) for r in rows] == [("U1", "S2"), ("T4", "S1")]
    assert set(rows[0]) == {"unit_uuid", "eta", "service_start", "free_at", "wait_seconds", "home"}


def test_decisions_dedup_by_unit_and_at_newest_first():
    f = Feed()
    f.d(5, "T1", NOON)
    f.d(6, "T2", NOON, "recommended", from_station="S1", to_station="S2", gain_seconds=80)
    f.d(5, "T1", NOON)  # повтор после падения
    f.d(50, "T1", NOON + 2400)  # следующий заезд той же машины
    f.d(51, "T3", NOON + 2401, "rejected", reason="stale_telemetry")
    st = build(f)
    assert [(d["unit_uuid"], d["at"]) for d in st["decisions"]] == [
        ("T3", fmt_ts(NOON + 2401)),
        ("T1", fmt_ts(NOON + 2400)),
        ("T2", fmt_ts(NOON)),
        ("T1", fmt_ts(NOON)),
    ]
    rec = st["decisions"][2]
    assert rec["result"] == "recommended" and rec["to_station"] == "S2" and rec["gain_seconds"] == 80
    assert rec["src_offset"] == 6
    assert st["health"]["stream"]["decision_totals"] == {"no_gain": 2, "recommended": 1, "stale_telemetry": 1}
    assert st["health"]["stream"]["repeats"][DECISION] == 1


def test_decision_feed_keeps_last_200():
    f = Feed()
    for i in range(250):
        f.d(i, f"T{i % 9 + 1}", NOON + i)
    st = build(f)
    assert len(st["decisions"]) == 200
    assert st["decisions"][0]["at"] == fmt_ts(NOON + 249) and st["decisions"][-1]["at"] == fmt_ts(NOON + 50)
    assert st["health"]["stream"]["decision_totals"]["no_gain"] == 250


def test_recreated_topic_resets_cache(caplog):
    """make reset пересоздаёт топики: offset топика ушёл назад — старые записи и их vq-src-offset забываются."""
    c = vs.StreamCache()
    for i in range(5):
        c.ingest(QUEUE, i, qrec("S1", NOON + i, ["T1"]), hdr(1000 + i))
    with caplog.at_level(logging.WARNING, logger="vq.viewer"):
        c.ingest(QUEUE, 0, qrec("S1", NOON + 99, ["T9"]), hdr(3))  # новый лог: src offset меньше прежних
    d = c.data()
    assert d.resets == 1 and d.queues["S1"].src_offset == 3 and d.max_src_offset[QUEUE] == 3
    assert [(r.getMessage(), r.fields) for r in caplog.records] == [("stream_reset", {"topic": QUEUE})]
    c.note_high(QUEUE, 1)
    assert c.data().resets == 1  # high не ниже позиции чтения — кеш не сбрасывается
    c.note_high(QUEUE, 0)
    assert c.data().resets == 2 and c.data().queues == {}


def test_malformed_records_are_counted_not_stored():
    c = vs.StreamCache()
    c.ingest(QUEUE, 0, qrec("S1", NOON, ["T1"]), None)  # без заголовка
    c.ingest(QUEUE, 1, b"{oops", hdr(1))
    c.ingest(QUEUE, 2, qrec("S1", NOON, ["T1"]), [("vq-src-offset", b"x")])
    c.ingest(DECISION, 0, json.dumps({"result": "rejected"}).encode(), hdr(4))
    assert c.data().malformed == 4 and c.data().queues == {} and c.data().decisions == ()
    assert c.data().max_src_offset == {}  # отметка offset — только по валидным записям


# --- записи не по контракту: страница не гаснет (GM-4) ----------------------------------------


def queue_bytes(station: str, at: int, wait: str) -> bytes:
    """Запись queue.v1 с wait_seconds, подставленным как есть (текст JSON)."""
    return (
        f'{{"station_uuid":"{station}","at":"{fmt_ts(at)}","queue":[{{"unit_uuid":"U1","eta":null,'
        f'"service_start":"{fmt_ts(at)}","free_at":"{fmt_ts(at + 230)}","wait_seconds":{wait}}}]}}'
    ).encode()


def nested(depth: int) -> str:
    """JSON-массив вложенности depth — строкой: json.dumps на такой глубине сам упал бы в RecursionError."""
    return "[" * depth + "]" * depth


def record_with(topic: str, value: str, at: int = NOON + 1) -> bytes:
    """Запись topic, где значение (текст JSON) стоит в неконтрактном месте: wait_seconds строки очереди S1 или
    лишний ключ решения T2."""
    if topic == QUEUE:
        return queue_bytes("S1", at, value)
    head = f'{{"unit_uuid":"T2","at":"{fmt_ts(at)}","result":"rejected","reason":"no_gain"'
    return f'{head},"extra":{value}}}'.encode()


@pytest.mark.parametrize("bad", ["NaN", "Infinity", "-Infinity", "1e999", "-1e999"])
def test_non_finite_number_in_a_record_does_not_break_the_page(bad):
    """json.loads пропускает NaN, Infinity и 1e999 (→ inf), а JSONResponse (allow_nan=False) их не сериализует:
    без json_safe одна такая запись превращала бы каждый /api/state в 500. На странице такое число — null."""
    c = vs.StreamCache()
    c.ingest(QUEUE, 0, queue_bytes("S2", NOON, bad), hdr(10))
    snaps = vs.SourceSnapshots(kafka=ok(c.data()), pg=pg_snapshot())
    app = va.create_app(two_stations(), FakeSources(snaps), lambda: WALL)
    status, _, body = asyncio.run(asgi_get(app, "/api/state"))
    assert status == 200
    assert station(json.loads(body), "S2")["queue"]["rows"][0]["wait_seconds"] is None


def test_invalid_json_and_deep_nesting_are_malformed():
    c = vs.StreamCache()
    c.ingest(DECISION, 0, drec("T1", NOON, "recommended", from_station="S1", to_station="S2", gain_seconds=80), hdr(6))
    c.ingest(DECISION, 1, b"[" * 100_000, hdr(7))  # json.loads падает с RecursionError, не ValueError
    c.ingest(QUEUE, 0, b'{"a":' * 100_000, hdr(8))
    c.ingest(QUEUE, 1, b"[1, 2]", hdr(9))
    d = c.data()
    assert d.malformed == 3 and [x.unit for x in d.decisions] == ["T1"] and d.queues == {}
    assert d.max_src_offset == {DECISION: 6}


@pytest.mark.parametrize("topic", [QUEUE, DECISION])
def test_deep_nesting_below_the_json_limit_is_malformed_and_the_page_stays_up(topic):
    """§5.1: глубокая вложенность — запись не той формы. json.loads (C) разбирает около 10 000 уровней, а json_safe
    рекурсивен в Python и падает на 1 000: запись в 2 000 уровней (лишний ключ решения, поле строки очереди) при
    malformed=0 превращала бы каждый /api/state в 500, пока её не вытеснят новые записи."""
    f = Feed()
    f.q(1, "S1", NOON, ["T1"])
    f.d(1, "T1", NOON)
    f.raw(topic, record_with(topic, nested(2_000)), 2)
    d = f.cache.data()
    # запись не той формы ничего не меняет: последние хорошие очередь и решение на месте, отметка offset прежняя
    assert d.malformed == 1 and d.queues["S1"].src_offset == 1 and [x.unit for x in d.decisions] == ["T1"]
    assert d.max_src_offset == {QUEUE: 1, DECISION: 1} and d.decision_totals == {"no_gain": 1}
    app = va.create_app(two_stations(), FakeSources(vs.SourceSnapshots(kafka=ok(d), pg=pg_snapshot())), lambda: WALL)
    status, _, body = asyncio.run(asgi_get(app, "/api/state"))
    assert status == 200 and json.loads(body)["health"]["stream"]["malformed"] == 1


def at_depth(topic: str, total: int, at: int = NOON + 1) -> bytes:
    """Запись topic, вложенная ровно на total уровней: вокруг значения у очереди ещё запись, список и строка, а у
    решения — только запись."""
    return record_with(topic, nested(total - (3 if topic == QUEUE else 1)), at)


@pytest.mark.parametrize("topic", [QUEUE, DECISION])
def test_nesting_up_to_the_bound_is_kept_and_rendered_one_level_deeper_is_malformed(topic):
    """Граница глубины: запись ровно в MAX_RECORD_DEPTH уровней принимается и /api/state её отдаёт — граница ниже того,
    что осиливает json_safe; на уровень глубже — malformed."""
    bound = vs.MAX_RECORD_DEPTH
    f = Feed()
    f.raw(topic, at_depth(topic, bound), 1)
    d = f.cache.data()
    assert d.malformed == 0 and d.max_src_offset == {topic: 1}
    app = va.create_app(two_stations(), FakeSources(vs.SourceSnapshots(kafka=ok(d), pg=pg_snapshot())), lambda: WALL)
    status, _, _ = asyncio.run(asgi_get(app, "/api/state"))
    assert status == 200
    f.raw(topic, at_depth(topic, bound + 1, NOON + 2), 2)
    d = f.cache.data()
    assert d.malformed == 1 and d.max_src_offset == {topic: 1}


@pytest.mark.parametrize(
    "queue",
    [
        "T1",  # не список
        [["T1"]],  # строка — не объект
        [{k: v for k, v in qrow("T1", NOON).items() if k != "free_at"}],  # нет поля
    ],
)
def test_queue_record_without_contract_rows_is_malformed_and_changes_nothing(queue):
    c = vs.StreamCache()
    c.ingest(QUEUE, 0, qrec("S1", NOON, ["T1"]), hdr(1))
    c.ingest(QUEUE, 1, json.dumps({"station_uuid": "S1", "at": fmt_ts(NOON + 1), "queue": queue}).encode(), hdr(2))
    d = c.data()
    assert d.malformed == 1 and d.queues["S1"].src_offset == 1 and d.max_src_offset[QUEUE] == 1


@pytest.mark.parametrize(
    "body",
    [
        {"unit_uuid": "T1", "at": fmt_ts(NOON), "result": "maybe"},
        {"unit_uuid": "T1", "at": fmt_ts(NOON), "result": "rejected"},  # нет reason
        {"unit_uuid": "T1", "at": fmt_ts(NOON), "result": "rejected", "reason": ["no_gain"]},  # reason — ключ итогов
        {"unit_uuid": "T1", "at": fmt_ts(NOON), "result": "recommended", "from_station": "S1", "to_station": "S2"},
        {"unit_uuid": 7, "at": fmt_ts(NOON), "result": "rejected", "reason": "no_gain"},
    ],
)
def test_decisions_without_contract_keys_are_malformed(body):
    c = vs.StreamCache()
    c.ingest(DECISION, 0, json.dumps(body).encode(), hdr(1))
    assert c.data().malformed == 1 and c.data().decisions == () and c.data().decision_totals == {}


def test_field_types_are_not_policed_the_records_come_from_our_dispatcher():
    """Витрина проверяет форму записи — объект, ключи §3.9, заголовок (A4-06), — но не типы полей: записи пишет
    наш dispatcher. Неожиданный тип доходит до страницы как есть, а /api/state всё равно отдаётся."""
    row = {**qrow("T1", NOON), "wait_seconds": "50", "eta": 5, "unit_uuid": None}
    c = vs.StreamCache()
    c.ingest(QUEUE, 0, json.dumps({"station_uuid": "S1", "at": fmt_ts(NOON), "queue": [row]}).encode(), hdr(1))
    weird = drec("T2", NOON, "recommended", from_station="S1", to_station=["S2"], gain_seconds="80")
    c.ingest(DECISION, 0, weird, hdr(2))
    d = c.data()
    assert d.malformed == 0 and d.queues["S1"].src_offset == 1 and d.decision_totals == {"recommended": 1}
    app = va.create_app(two_stations(), FakeSources(vs.SourceSnapshots(kafka=ok(d), pg=pg_snapshot())), lambda: WALL)
    status, _, body = asyncio.run(asgi_get(app, "/api/state"))
    assert status == 200 and station(json.loads(body), "S1")["queue"]["rows"][0]["wait_seconds"] == "50"


@pytest.mark.parametrize(
    "at",
    [
        "yesterday",
        "2026-02-30T00:00:00Z",
        "1970-01-01T00:00:00Z",
        "2100-01-01T00:00:00Z",
        7,
        # вид строже strptime: ровно то, что пишет fmt_ts
        "2026-9-15T12:00:00Z",
        "2026-09-15t12:00:00z",
        "2026-09-15T12:00:00.250Z",
        "2026-09-15T12:00:00+00:00",
    ],
)
def test_records_with_at_outside_the_contract_are_malformed(at):
    """at — 'YYYY-MM-DDTHH:MM:SSZ' в диапазоне ts ядра (2001–2100): иначе «сейчас» страницы стало бы мусором."""
    c = vs.StreamCache()
    c.ingest(QUEUE, 0, json.dumps({"station_uuid": "S1", "at": at, "queue": []}).encode(), hdr(1))
    c.ingest(
        DECISION,
        0,
        json.dumps({"unit_uuid": "T1", "at": at, "result": "rejected", "reason": "no_gain"}).encode(),
        hdr(1),
    )
    assert c.data().malformed == 2 and c.data().queues == {} and c.data().decisions == ()


def test_non_finite_values_from_postgres_or_metrics_do_not_break_the_state():
    """Не только Kafka: NaN в строке Postgres или в /metrics тоже не должен ронять /api/state."""
    vehicles = (
        {**vehicle("T1", "S1", "S1", "service", NOON + 40, 49.2), "lat": math.nan, "speed_kmh": math.inf},
        vehicle("T2", "S1", "S2", "to_station", NOON + 9, 49.1, 36.0),
    )
    text = (
        "# TYPE vq_decisions_total counter\n"
        'vq_decisions_total{result="recommended",reason=""} NaN\n'
        "# TYPE vq_consumer_lag_messages gauge\nvq_consumer_lag_messages +Inf\n"
        "# TYPE vq_fsm_resync_total counter\n"
        'vq_fsm_resync_total{station="S1"} NaN\n'
        "# TYPE vq_prediction_abs_error_seconds histogram\n"
        'vq_prediction_abs_error_seconds_bucket{le="5.0"} NaN\n'
        'vq_prediction_abs_error_seconds_bucket{le="+Inf"} NaN\n'
        "vq_prediction_abs_error_seconds_count NaN\n"
        "vq_prediction_abs_error_seconds_sum NaN\n"
    )
    gen = "# TYPE vq_gen_expected_drops_total counter\nvq_gen_expected_drops_total NaN\n"
    st = build(
        Feed(),
        pg=pg_snapshot(vehicles=vehicles),
        dispatcher=ok(vs.parse_metrics(text)),
        generator=ok(vs.parse_metrics(gen)),
    )
    json.dumps(st, allow_nan=False)  # так сериализует JSONResponse
    t1 = st["vehicles"][0]
    assert t1["lat"] is None and t1["speed_kmh"] is None and t1["phase"] == "service"
    h = st["health"]
    assert h["decisions"] == {"recommended": 0, "no_gain": 0, "stale_telemetry": 0}
    assert (h["consumer_lag"], h["resync"], h["expected_drops"]) == (None, None, None)
    assert h["prediction"] == {"count": 0, "within_5s": None}


# --- пометки источников и отметки offset ---------------------------------------------------


def test_unavailable_sources_are_marked_and_the_rest_still_works():
    f = Feed()
    f.q(7, "S1", NOON, ["T1"])
    kafka_down = vs.Snapshot(False, f.cache.data(), "KafkaException: _TRANSPORT", WALL - 30)
    st = build(kafka=kafka_down, pg=pg_snapshot())
    src = st["sources"]
    assert src["kafka"]["ok"] is False and "TRANSPORT" in src["kafka"]["error"] and src["kafka"]["age_s"] == 30.0
    assert src["postgres"]["ok"] is True
    assert src["dispatcher"] == {"ok": False, "error": "источник не настроен", "age_s": None, "has_data": False}
    # Postgres жив — карта есть; последние известные очереди показываются, источник помечен
    assert [v["unit"] for v in st["vehicles"]] == ["T1", "T2"]
    assert station(st, "S1")["occupant"] == "T1" and station(st, "S1")["queue"]["src_offset"] == 7
    # и наоборот: Postgres недоступен изначально, очереди и решения идут из Kafka
    pg_down = vs.Snapshot(False, None, "OperationalError: connection refused", None)
    st = build(f, pg=pg_down)
    assert st["sources"]["postgres"]["ok"] is False and st["sources"]["kafka"]["ok"] is True
    assert st["vehicles"] == [] and st["health"]["counters"] is None
    assert station(st, "S1")["queue"]["rows"][0]["unit_uuid"] == "T1"
    assert st["now"] == NOON  # «сейчас» — из at очереди, раз Postgres молчит


def test_stale_snapshot_is_not_ok_even_if_last_attempt_succeeded():
    snap = vs.Snapshot(True, (), None, WALL - va.STALE_S - 1)
    status = va.source_status(snap, WALL)
    assert status["ok"] is False and "нет обновлений" in status["error"]


def test_offset_marks_queue_mark_is_the_last_change_not_a_lag():
    """GM-2: queue.v1 публикуется только при изменении очереди, поэтому X — offset последнего изменения, а не
    offset, на котором очереди актуальны. В установившемся режиме X < Y — норма: страница не должна читаться
    как «очереди отстают от карты». X > Y — очереди опубликованы, а чекпоинт ещё нет (не больше пачки)."""
    f = Feed()
    f.q(140, "S1", NOON, ["T1"])
    f.q(150, "S2", NOON, [])
    f.d(160, "T2", NOON)  # решения в отметку очередей не входят
    m = build(f, pg=pg_snapshot(next_offset=301))["health"]["marks"]
    assert (m["queues_offset"], m["map_offset"], m["queues_ahead"]) == (150, 300, 0)
    assert m["text"] == "очереди — последнее изменение на offset 150, карта на offset 300"
    m = build(f, pg=pg_snapshot(next_offset=151))["health"]["marks"]
    assert (m["queues_ahead"], m["text"]) == (0, "очереди — последнее изменение на offset 150, карта на offset 150")
    m = build(f, pg=pg_snapshot(next_offset=146))["health"]["marks"]
    assert (m["queues_offset"], m["map_offset"], m["queues_ahead"]) == (150, 145, 5)
    assert m["text"] == "очереди — последнее изменение на offset 150, карта на offset 145; очереди впереди карты на 5"
    m = build(None, kafka=None, pg=pg_snapshot(next_offset=0))["health"]["marks"]
    assert (m["queues_offset"], m["map_offset"], m["queues_ahead"]) == (None, None, None)
    assert m["text"] == "очереди — последнее изменение на offset —, карта на offset —"


def test_health_from_processor_row_and_vehicle_freshness():
    st = build(Feed(), pg=pg_snapshot(now_ts=NOON + 40))
    h = st["health"]
    assert (h["epoch"], h["next_offset"], h["logic_version"]) == (3, 101, "1")
    assert h["counters"] == {"accepted": 1000, "not_newer": 7, "invalid": 2}
    assert h["updated_age_s"] == 0.5
    t1, t2 = st["vehicles"]
    assert t1["fresh"] and not t1["redirected"] and t1["age_s"] == 0
    assert not t2["fresh"] and t2["redirected"] and t2["age_s"] == 31  # 31 секунда — уже не свежая
    s1 = station(st, "S1")
    assert s1["free_at"] == NOON + 230 and s1["assigned"] == 9 and s1["lat"] == two_stations().station("S1").lat
    assert st["now"] == NOON + 40 and st["map_now"] == NOON + 40


# --- /metrics: экспозиции настоящих классов метрик (GM-5) ----------------------------------------


def dispatcher_metrics_text() -> str:
    """Экспозиция настоящего DispatcherMetrics: имена, метки и корзины — те, что отдаёт dispatcher."""
    reg = CollectorRegistry()
    metrics = DispatcherMetrics(reg, stations=["S1", "S2"], clock=lambda: WALL)
    events = [event("decision", result="recommended", reason="", gain=80)] * 4
    events += [event("decision", result="rejected", reason="no_gain")] * 10
    events += [event("decision", result="rejected", reason="stale_telemetry")]
    events += [event("prediction_error", seconds=e) for e in (0, 0, 0, 1, -1, 2, 3, -4, 6, 20)]
    events += [event("fsm_resync", station="S2")]
    metrics.observe_batch(events, batch_seconds=0.01, checkpoint_seconds=0.002, now=NOON)
    metrics.observe_lag(12)
    return generate_latest(reg).decode()


def generator_metrics_text(drops: int) -> str:
    """Экспозиция настоящего GeneratorMetrics после синхронизации с генератором."""
    reg = CollectorRegistry()
    GeneratorMetrics(reg).sync(
        produced=100,
        faults={"dup": drops},
        expected_drops=drops,
        received=0,
        applied=0,
        ignored={},
        delays=[],
        sim_time=NOON,
    )
    return generate_latest(reg).decode()


def test_decisions_by_type_and_prediction_from_dispatcher_metrics():
    samples = vs.parse_metrics(dispatcher_metrics_text())
    h = build(Feed(), dispatcher=ok(samples))["health"]
    assert h["decisions"] == {"recommended": 4, "no_gain": 10, "stale_telemetry": 1}
    # |ошибки| 0,0,0,1,1,2,3,4,6,20: до 5 с (корзина le=5 гистограммы модуля) — 8 из 10. Квантили — в Grafana (§5.2).
    assert h["prediction"] == {"count": 10, "within_5s": 0.8}
    assert h["resync"] == 1 and h["consumer_lag"] == 12


def instance_metrics(epoch: int, recommended: int) -> tuple[vs.Sample, ...]:
    """Срез /metrics экземпляра dispatcher: vq_epoch и число рекомендаций."""
    reg = CollectorRegistry()
    metrics = DispatcherMetrics(reg, stations=["S1", "S2"], clock=lambda: WALL)
    metrics.epoch.set(epoch)
    metrics.observe_events([event("decision", result="recommended", reason="", gain=80)] * recommended)
    return vs.parse_metrics(generate_latest(reg).decode())


def test_dispatcher_metrics_come_from_the_instance_that_owns_the_site():
    """§2.2, демо перехвата (A1-07): экземпляров два — основной и standby. Показывается тот, у кого vq_epoch
    больше (владелец площадки), как в панелях Grafana; при равенстве — первый в списке. Если не ответил ни один —
    срез с самым свежим удачным чтением, источник помечен недоступным."""
    main, standby = ok(instance_metrics(3, 4)), ok(instance_metrics(0, 0))
    assert vs.pick_dispatcher([main, standby]) is main
    assert vs.pick_dispatcher([standby, main]) is main
    # основной остановлен, standby перехватил площадку
    main_down = vs.Snapshot(False, main.data, "URLError: connection refused", WALL - 20)
    standby_owner = ok(instance_metrics(4, 1))
    assert vs.pick_dispatcher([main_down, standby_owner]) is standby_owner
    h = build(Feed(), dispatcher=vs.pick_dispatcher([main_down, standby_owner]))["health"]
    assert h["decisions"]["recommended"] == 1
    # основной вернулся и забрал площадку обратно (epoch 5); у standby epoch застыл на 4
    main_back = ok(instance_metrics(5, 0))
    assert vs.pick_dispatcher([main_back, standby_owner]) is main_back
    # оба ещё без claim — первый
    idle = ok(instance_metrics(0, 0))
    assert vs.pick_dispatcher([idle, standby]) is idle
    # не ответил никто
    old = vs.Snapshot(False, main.data, "URLError: a", WALL - 30)
    newer = vs.Snapshot(False, standby_owner.data, "URLError: b", WALL - 10)
    never = vs.Snapshot(False, None, "ещё не опрашивался", None)
    assert vs.pick_dispatcher([never, old, newer]) is newer
    assert vs.pick_dispatcher([never]) is never
    assert vs.pick_dispatcher([]) is None


def test_sources_scrape_every_dispatcher_listed_in_the_env(monkeypatch):
    """VQ_DISPATCHER_METRICS — список через запятую (основной и dispatcher-b); каждый адрес — свой опрос со своим
    таймаутом 1 с, а страница получает срез владельца площадки (A1-07)."""
    by_url = {
        "http://dispatcher:9100/metrics": instance_metrics(2, 7),
        "http://dispatcher-b:9100/metrics": instance_metrics(6, 1),
    }
    scraped: list[str] = []

    def fake_scrape(url: str, timeout: float = vs.SOURCE_TIMEOUT_S) -> tuple[vs.Sample, ...]:
        scraped.append(url)
        return by_url[url]

    monkeypatch.setattr(vs, "scrape", fake_scrape)
    env = {"VQ_DISPATCHER_METRICS": "http://dispatcher:9100/metrics, http://dispatcher-b:9100/metrics"}
    sources = vs.Sources.from_env("site-x", env=env)
    assert [p.name for p in sources.dispatchers] == ["dispatcher dispatcher:9100", "dispatcher dispatcher-b:9100"]
    assert all(p.timeout == vs.SOURCE_TIMEOUT_S for p in sources.dispatchers)
    for p in sources.dispatchers:
        p.poll_once()
    assert scraped == list(by_url)
    assert sources.snapshots().dispatcher.data == by_url["http://dispatcher-b:9100/metrics"]
    single = vs.Sources.from_env("site-x", env={})
    assert [p.name for p in single.dispatchers] == ["dispatcher localhost:9100"]


def test_expected_drops_from_generator_then_summary_fallback():
    gen = vs.parse_metrics(generator_metrics_text(7))
    h = build(Feed(), pg=pg_snapshot(not_newer=7), generator=ok(gen), summary=ok({"expected_drops": 99}))["health"]
    assert (h["expected_drops"], h["expected_drops_source"], h["drops_match"]) == (7, "generator", True)
    gen_gone = vs.Snapshot(False, gen, "URLError: connection refused", WALL - 60)  # генератор завершил прогон
    h = build(Feed(), pg=pg_snapshot(not_newer=7), generator=gen_gone, summary=ok({"expected_drops": 8}))["health"]
    assert (h["expected_drops"], h["expected_drops_source"], h["drops_match"]) == (8, "summary", False)
    h = build(Feed(), pg=pg_snapshot(), generator=gen_gone)["health"]
    assert (h["expected_drops"], h["expected_drops_source"], h["drops_match"]) == (None, None, None)


def test_metric_value_sums_matching_labels():
    samples = vs.parse_metrics(dispatcher_metrics_text())
    assert vs.metric_value(samples, "vq_decisions_total") == 15
    assert vs.metric_value(samples, "vq_decisions_total", reason="no_gain") == 10
    assert vs.metric_value(samples, "vq_missing_total") is None


# --- источники на подделках ----------------------------------------------------------------


class FakeCursor:
    def __init__(self, rows):
        self.rows = rows

    def fetchone(self):
        return self.rows[0] if self.rows else None

    def fetchall(self):
        return list(self.rows)


class FakeConn:
    def __init__(self, fail_on: str | None = None) -> None:
        self.sql: list[str] = []
        self.closed = False
        self.fail_on = fail_on

    def execute(self, sql, params=None):
        self.sql.append(sql)
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("statement timeout")
        if sql.startswith("SELECT") and "FROM processor" in sql:
            assert params == ("site-x",)
            return FakeCursor([{"site_id": "site-x", "next_offset": 5}])
        return FakeCursor([])

    def close(self):
        self.closed = True


def test_pg_reader_one_read_only_repeatable_read_transaction_and_reconnect():
    conns: list[FakeConn] = []

    def connect(dsn):
        conns.append(FakeConn(fail_on="FROM vehicles" if not conns else None))
        return conns[-1]

    reader = vs.PgReader("postgresql://x", "site-x", connect=connect)
    poller = vs.Poller("postgres", reader.read, 0.5, clock=lambda: WALL)
    first = poller.poll_once()
    assert not first.ok and "statement timeout" in first.error and conns[0].closed
    second = poller.poll_once()
    assert second.ok and second.data.processor == {"site_id": "site-x", "next_offset": 5}
    assert len(conns) == 2  # после ошибки — новое соединение
    sql = conns[1].sql
    assert sql[0] == "BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY" and sql[-1] == "COMMIT" and len(sql) == 5
    assert ["FROM processor" in sql[1], "FROM stations" in sql[2], "FROM vehicles" in sql[3]] == [True] * 3


def test_poller_keeps_last_good_data_on_failure():
    calls = iter([{"v": 1}, RuntimeError("down"), {"v": 2}])

    def fetch():
        x = next(calls)
        if isinstance(x, Exception):
            raise x
        return x

    p = vs.Poller("x", fetch, 1.0, clock=lambda: WALL)
    assert p.poll_once().data == {"v": 1}
    s = p.poll_once()
    assert (s.ok, s.data, s.error, s.checked_at) == (False, {"v": 1}, "RuntimeError: down", WALL)
    assert p.poll_once().ok


def test_poller_logs_only_transitions_as_events(caplog):
    """В лог — переходы источника событиями с полями: недоступен (с ошибкой) и снова в строю; повтор ошибки — нет."""
    calls = iter([{"v": 1}, OSError("connection refused"), OSError("connection refused"), {"v": 2}])

    def fetch():
        x = next(calls)
        if isinstance(x, Exception):
            raise x
        return x

    p = vs.Poller("postgres", fetch, 1.0, clock=lambda: WALL)
    with caplog.at_level(logging.INFO, logger="vq.viewer"):
        for _ in range(4):
            p.poll_once()
    assert [(r.levelname, r.getMessage(), r.fields) for r in caplog.records] == [
        ("WARNING", "source_down", {"source": "postgres", "error": "OSError: connection refused"}),
        ("INFO", "source_up", {"source": "postgres"}),
    ]


def test_pg_close_failure_is_logged_as_an_event(caplog):
    class BadClose(FakeConn):
        def close(self):
            raise OSError("socket already closed")

    reader = vs.PgReader("postgresql://x", "site-x", connect=lambda dsn: BadClose(fail_on="FROM vehicles"))
    with caplog.at_level(logging.WARNING, logger="vq.viewer"), pytest.raises(RuntimeError, match="statement timeout"):
        reader.read()
    assert [(r.getMessage(), r.fields) for r in caplog.records] == [
        ("pg_close_failed", {"error": "OSError: socket already closed"})
    ]


def test_poller_marks_a_hung_fetch_down_after_its_timeout():
    """§5.1, таймаут 1 с на источник. Замёрзший Postgres (docker pause) не исполняет и свой statement_timeout:
    вызов не прервать, но страница видит пометку через секунду, а не через STALE_S."""
    mono = [100.0]
    started, release = threading.Event(), threading.Event()
    calls = iter(["fast", "hang"])

    def fetch():
        if next(calls) == "hang":
            started.set()
            release.wait(5.0)
            return {"v": 2}
        return {"v": 1}

    p = vs.Poller("postgres", fetch, 0.5, clock=lambda: WALL, timeout=1.0, monotonic=lambda: mono[0])
    assert p.poll_once().ok
    worker = threading.Thread(target=p.poll_once)
    worker.start()
    try:
        assert started.wait(2.0)
        mono[0] += 0.9
        assert p.snapshot().ok  # ещё в пределах таймаута
        mono[0] += 0.3
        s = p.snapshot()
        assert (s.ok, s.error, s.data, s.checked_at) == (False, "нет ответа 1.2 сек", {"v": 1}, WALL)
        status = va.source_status(s, WALL)
        assert status["ok"] is False and status["error"] == "нет ответа 1.2 сек"
    finally:
        release.set()
        worker.join(2.0)
    assert p.snapshot().ok and p.snapshot().data == {"v": 2}


def test_each_source_has_a_one_second_timeout(monkeypatch):
    """§5.1: у каждого источника свой таймаут 1 с — /metrics, Kafka, Postgres."""
    assert vs.SOURCE_TIMEOUT_S == 1.0
    seen: dict = {}

    class Opener:
        def open(self, url, timeout=None):
            seen["timeout"] = timeout
            raise OSError("connection refused")

    monkeypatch.setattr(vs.urllib.request, "build_opener", lambda *handlers: Opener())
    with pytest.raises(OSError):
        vs.scrape("http://dispatcher:9100/metrics")
    assert seen["timeout"] == 1.0
    assert vs.kafka_conf("kafka:9092")["socket.connection.setup.timeout.ms"] == 1000
    import psycopg

    def connect(dsn, **kw):
        seen.update(kw)
        raise psycopg.OperationalError("connection refused")

    monkeypatch.setattr(psycopg, "connect", connect)
    with pytest.raises(psycopg.OperationalError):
        vs.PgReader("postgresql://x", "site-x").read()
    assert seen["connect_timeout"] == 1 and "statement_timeout=1000" in seen["options"]
    sources = vs.Sources.from_env("site-x", env={})
    pollers = (sources.pg, *sources.dispatchers, sources.generator, sources.summary)
    assert all(p.timeout == 1.0 for p in pollers)


class FakeKafkaMsg:
    def __init__(self, topic, offset, value, headers):
        self._t, self._o, self._v, self._h = topic, offset, value, headers

    def topic(self):
        return self._t

    def offset(self):
        return self._o

    def value(self):
        return self._v

    def headers(self):
        return self._h

    def error(self):
        return None


class FakeKafkaConsumer:
    """Клиент для KafkaView: отдаёт пачки, потом пустые ответы; broken — следующий consume бросает, как сломанный
    клиент librdkafka."""

    def __init__(self, conf, batches, highs):
        self.conf, self.batches, self.highs = conf, list(batches), highs
        self.assigned = None
        self.broken = False
        self.closed = False
        self.watermark_timeouts: list = []

    def assign(self, parts):
        self.assigned = [(p.topic, p.partition, p.offset) for p in parts]

    def consume(self, num_messages=1, timeout=-1):
        if self.broken:
            raise RuntimeError("client broken")
        if self.batches:
            return self.batches.pop(0)
        time.sleep(0.005)
        return []

    def get_watermark_offsets(self, tp, timeout=None, cached=False):
        self.watermark_timeouts.append(timeout)
        return 0, self.highs[tp.topic]

    def close(self):
        self.closed = True


def until(predicate, timeout: float = 5.0) -> bool:
    """Ждёт условия по событию, а не фиксированной паузой; False — не дождались."""
    deadline = time.monotonic() + timeout
    while not predicate():
        if time.monotonic() > deadline:
            return False
        time.sleep(0.002)
    return True


def test_kafka_view_folds_both_topics_and_resumes_after_a_client_failure(caplog):
    """§5.1, §2.6: queue.v1 и decision.v1 с начала (assign без группы), свёртка с отбрасыванием повтора, high
    watermark с таймаутом источника 1 с. Сломанный клиент пересоздаётся, и чтение продолжается дальше, не сначала."""
    from confluent_kafka import OFFSET_BEGINNING

    batch = [
        FakeKafkaMsg(QUEUE, 0, qrec("S1", NOON, ["T1"]), hdr(3)),
        FakeKafkaMsg(DECISION, 0, drec("T1", NOON), hdr(3)),
        FakeKafkaMsg(QUEUE, 1, qrec("S1", NOON, ["T1"]), hdr(3)),  # повтор
    ]
    made: list[FakeKafkaConsumer] = []

    def make(conf):
        made.append(FakeKafkaConsumer(conf, [] if made else [batch], {QUEUE: 2, DECISION: 1}))
        return made[-1]

    view = vs.KafkaView("kafka:9092", make_consumer=make, clock=lambda: WALL, retry_after=0.01)
    caplog.set_level(logging.WARNING, logger="vq.viewer")
    view.start()
    try:
        assert until(lambda: view.snapshot().ok)
        snap = view.snapshot()
        made[0].broken = True
        assert until(lambda: len(made) == 2 and made[1].assigned is not None)
    finally:
        view.stop()
    down = [(r.getMessage(), r.fields) for r in caplog.records if r.levelno == logging.WARNING]
    assert down == [("source_down", {"source": "kafka", "error": "RuntimeError: client broken"})]
    first = made[0]
    assert first.conf == vs.kafka_conf("kafka:9092") and first.closed
    assert first.assigned == [(QUEUE, 0, OFFSET_BEGINNING), (DECISION, 0, OFFSET_BEGINNING)]
    assert first.watermark_timeouts and set(first.watermark_timeouts) == {1.0}  # таймаут источника 1 с
    assert snap.checked_at == WALL
    assert snap.data.queues["S1"].src_offset == 3 and snap.data.repeats == {QUEUE: 1}
    assert snap.data.next_offset == {QUEUE: 2, DECISION: 1} and snap.data.high == {QUEUE: 2, DECISION: 1}
    assert made[1].assigned == [(QUEUE, 0, 2), (DECISION, 0, 1)]  # продолжение после пересоздания клиента


def test_summary_reader(tmp_path):
    path = tmp_path / "summary.json"
    path.write_text(json.dumps({"expected_drops": 3}))
    assert vs.read_summary(path) == {"expected_drops": 3}
    path.write_text("[1, 2]")
    with pytest.raises(ValueError):
        vs.read_summary(path)


# --- приложение (ASGI напрямую, без сервера и httpx) -------------------------------------


class FakeSources:
    def __init__(self, snaps: vs.SourceSnapshots) -> None:
        self.snaps = snaps
        self.started = self.stopped = False

    def start(self) -> None:
        self.started = True

    def stop(self) -> None:
        self.stopped = True

    def snapshots(self) -> vs.SourceSnapshots:
        return self.snaps


async def asgi_get(app, path: str) -> tuple[int, dict, bytes]:
    sent: list[dict] = []

    async def receive():
        return {"type": "http.request", "body": b"", "more_body": False}

    async def send(message):
        sent.append(message)

    scope = {
        "type": "http",
        "asgi": {"version": "3.0"},
        "http_version": "1.1",
        "method": "GET",
        "scheme": "http",
        "path": path,
        "raw_path": path.encode(),
        "query_string": b"",
        "root_path": "",
        "headers": [(b"host", b"viewer")],
        "client": ("127.0.0.1", 5000),
        "server": ("viewer", 8000),
    }
    await app(scope, receive, send)
    start = next(m for m in sent if m["type"] == "http.response.start")
    body = b"".join(m.get("body", b"") for m in sent if m["type"] == "http.response.body")
    return start["status"], {k.decode(): v.decode() for k, v in start["headers"]}, body


def test_routes_serve_state_and_page():
    f = Feed()
    f.q(1, "S1", NOON, ["T1"])
    sources = FakeSources(vs.SourceSnapshots(kafka=f.snap(), pg=pg_snapshot()))
    app = va.create_app(two_stations(), sources, clock=lambda: WALL)
    status, headers, body = asyncio.run(asgi_get(app, "/api/state"))
    assert status == 200 and headers["cache-control"] == "no-store"
    state = json.loads(body)
    assert set(state) >= {"now", "stations", "vehicles", "decisions", "health", "sources"}
    assert station(state, "S1")["queue"]["src_offset"] == 1
    status, headers, body = asyncio.run(asgi_get(app, "/"))
    assert status == 200 and headers["content-type"].startswith("text/html")
    assert b"/api/state" in body and b"<svg" in body


@pytest.fixture
def uvicorn_loggers():
    """Логгеры uvicorn после теста — как были: uvicorn.Config их настраивает."""
    loggers = [logging.getLogger(n) for n in ("uvicorn", "uvicorn.error", "uvicorn.access", "uvicorn.asgi")]
    saved = [(lg, lg.level, lg.propagate, lg.handlers[:]) for lg in loggers]
    yield
    for lg, level, propagate, handlers in saved:
        lg.setLevel(level)
        lg.propagate = propagate
        lg.handlers[:] = handlers


def test_main_writes_uvicorn_logs_through_the_json_formatter_without_access_log(monkeypatch, caplog, uvicorn_loggers):
    """Лог витрины — JSON vq.logs. uvicorn с log_config=None своих обработчиков не ставит: его строки уходят в
    корневой логгер (event=log, текст в msg, без цветного color_message). Журнал доступа выключен: страница
    опрашивает /api/state дважды в секунду с каждой вкладки, и он заслонил бы source_down и source_up."""
    calls: list = []
    monkeypatch.setattr(va, "setup_logging", lambda: calls.append("setup_logging"))
    monkeypatch.setattr(va, "load_config", lambda path: two_stations())
    monkeypatch.setattr(va, "Sources", SimpleNamespace(from_env=lambda site: FakeSources(vs.SourceSnapshots())))
    monkeypatch.setattr(va.uvicorn, "run", lambda app, **kw: calls.append((app, kw)))
    va.main(["--port", "8123"])
    assert calls[0] == "setup_logging"  # до uvicorn: его строки старта — уже JSON
    app, kw = calls[1]
    assert (kw["host"], kw["port"], kw["log_config"], kw["access_log"]) == ("0.0.0.0", 8123, None, False)
    uvicorn.Config(app, **kw)  # так uvicorn.run настраивает логгеры
    with caplog.at_level(logging.INFO):
        logging.getLogger("uvicorn.error").info("Started server process [%d]", 7, extra={"color_message": "\x1b[36m"})
        logging.getLogger("uvicorn.access").info('%s - "GET /api/state HTTP/1.1" %d', "127.0.0.1:5000", 200)
    (rec,) = caplog.records
    doc = json.loads(JsonFormatter().format(rec))
    assert {k: v for k, v in doc.items() if k != "ts"} == {
        "level": "INFO",
        "logger": "uvicorn.error",
        "event": "log",
        "msg": "Started server process [7]",
    }


# --- страница -------------------------------------------------------------------------------

PAGE = (va.STATIC_DIR / "index.html").read_text(encoding="utf-8")
needs_node = pytest.mark.skipif(shutil.which("node") is None, reason="нет node")


def page_script() -> str:
    return PAGE.split("<script>", 1)[1].split("</script>", 1)[0]


def test_page_covers_the_spec_bullets():
    """§5.1, страница: опрос /api/state раз в 500 мс; круги 50 и 1500 м из /api/state; машины цветом фазы;
    пунктир к цели у перенаправленных; карточки очередей с пятью полями строки; лента решений; полоса здоровья."""
    js = page_script()
    assert "const POLL_MS = 500;" in js and "fetch('/api/state'" in js
    assert "state.site.radius_m" in js and "state.site.decision_radius_m" in js
    assert "`veh p-${v.phase}" in js
    for phase in ("to_station", "waiting", "service", "served", "to_dump"):
        assert f".veh.p-{phase} .dot" in PAGE
    assert re.search(r"\.link \{[^}]*stroke-dasharray", PAGE) and "v.redirected" in js
    for field in ("r.unit_uuid", "r.eta", "r.service_start", "r.free_at", "r.wait_seconds"):
        assert field in js
    assert "function renderFeed" in js and "function renderHealth" in js and "marks.text" in js
    assert "p.within_5s" in js and "p.count" in js


# Скрипт страницы в node на заглушке DOM: любой узел — Proxy, у которого есть любое свойство и любой метод,
# размеры — числа. Проверяется одно — отрисовка настоящего /api/state не бросает: ошибку отрисовки страница
# пишет в console.error, раннер их собирает.
STUB_RUNNER = r"""
'use strict';
const fs = require('fs');
const vm = require('vm');
const [scriptPath, statePath, mode] = process.argv.slice(2);
const state = JSON.parse(fs.readFileSync(statePath, 'utf8'));
const SIZE = { clientWidth: 960, clientHeight: 620, offsetWidth: 140, offsetHeight: 44, scrollTop: 0 };
const node = () => new Proxy(function () {}, {
  get(_, k) {
    if (k in SIZE) return SIZE[k];
    if (k === Symbol.iterator) return function* () {};
    if (k === Symbol.toPrimitive) return () => '';
    return k === 'then' ? undefined : node();
  },
  set: () => true,
  apply: () => node(),
  construct: () => node(),
});
const errors = [], timers = [], frames = [];
const sandbox = {
  console: { log() {}, warn() {}, error: (...a) => errors.push(a.map((x) => String((x && x.stack) || x)).join(' ')) },
  document: {
    hidden: false, documentElement: node(), getElementById: node, createElement: node, createElementNS: node,
    querySelectorAll: () => [],
  },
  window: { matchMedia: () => ({ matches: false }) },
  localStorage: { getItem: () => null, setItem() {} },
  location: { search: '' },
  URLSearchParams, performance,
  ResizeObserver: class { observe() {} },
  requestAnimationFrame: (cb) => frames.push(cb),
  setTimeout: (cb, ms) => timers.push(ms),
  CSS: { escape: String },
  fetch: async () => (mode === 'down'
    ? { ok: false, status: 500, json: async () => ({}) }
    : { ok: true, status: 200, json: async () => state }),
};
vm.createContext(sandbox);
vm.runInContext(fs.readFileSync(scriptPath, 'utf8'), sandbox);
(async () => {
  for (let i = 0; i < 10; i++) await new Promise((r) => setImmediate(r));
  frames.splice(0).forEach((f) => f());
  console.log(JSON.stringify({ errors, timers }));
})();
"""


def run_page(tmp_path, state: dict, mode: str = "up") -> dict:
    """Одна итерация опроса страницы на данном /api/state; mode=down — /api/state отвечает 500."""
    (tmp_path / "page.js").write_text(page_script(), encoding="utf-8")
    (tmp_path / "runner.js").write_text(STUB_RUNNER, encoding="utf-8")
    (tmp_path / "state.json").write_text(json.dumps(state, allow_nan=False), encoding="utf-8")
    proc = subprocess.run(
        ["node", str(tmp_path / "runner.js"), str(tmp_path / "page.js"), str(tmp_path / "state.json"), mode],
        capture_output=True,
        text=True,
        timeout=60,
    )
    assert proc.returncode == 0, proc.stderr
    return json.loads(proc.stdout.strip().splitlines()[-1])


def page_state(pg: vs.Snapshot | None = None) -> dict:
    """Срез со всеми фазами, перенаправленной машиной, очередями обеих станций, лентой решений и метриками."""
    f = Feed()
    f.q(140, "S1", NOON + 30, ["T1", "T3"])
    f.q(150, "S2", NOON + 35, ["T2"])
    f.d(120, "T2", NOON + 20, "recommended", from_station="S1", to_station="S2", gain_seconds=80)
    f.d(130, "T4", NOON + 25)
    vehicles = (
        vehicle("T1", "S1", "S1", "service", NOON + 40, 49.2187),
        vehicle("T2", "S1", "S2", "to_station", NOON + 40, 49.21, 36.0),
        vehicle("T3", "S1", "S1", "waiting", NOON + 40, 49.2186),
        vehicle("T4", "S1", "S1", "served", NOON + 40, 49.2185),
        vehicle("T5", "S1", "S1", "to_dump", NOON + 40, 49.17, 36.0),
    )
    return build(
        f,
        pg=pg or pg_snapshot(next_offset=301, vehicles=vehicles),
        dispatcher=ok(vs.parse_metrics(dispatcher_metrics_text())),
        generator=ok(vs.parse_metrics(generator_metrics_text(7))),
    )


@needs_node
def test_page_script_parses(tmp_path):
    (tmp_path / "page.js").write_text(page_script(), encoding="utf-8")
    proc = subprocess.run(["node", "--check", str(tmp_path / "page.js")], capture_output=True, text=True, timeout=60)
    assert proc.returncode == 0, proc.stderr


@needs_node
@pytest.mark.parametrize("variant", ["all_sources", "postgres_down", "no_sources", "api_down"])
def test_page_renders_real_states_without_errors(tmp_path, variant):
    """Отрисовка настоящего /api/state не бросает — при всех источниках, без Postgres, без единого источника и при
    недоступном /api/state; следующий опрос назначен не позже чем через 500 мс."""
    pg_down = vs.Snapshot(False, None, "OperationalError: connection refused", None)
    state = {
        "all_sources": page_state,
        "postgres_down": lambda: page_state(pg=pg_down),
        "no_sources": lambda: build(None, kafka=None),
        "api_down": page_state,
    }[variant]()
    r = run_page(tmp_path, state, mode="down" if variant == "api_down" else "up")
    assert r["errors"] == []
    assert len(r["timers"]) == 1 and 0 <= r["timers"][0] <= 500


@needs_node
def test_page_reports_a_render_error_instead_of_an_outage(tmp_path):
    """Ошибка отрисовки — ошибка страницы, а не недоступность витрины: она уходит в console.error (и в баннер),
    опрос продолжается. Общий catch с fetch выдавал её за «Витрина не отвечает»."""
    state = page_state()
    state["stations"][0]["queue"]["rows"] = 5  # rows.filter — не функция
    r = run_page(tmp_path, state)
    assert len(r["errors"]) == 1 and "TypeError" in r["errors"][0]
    assert len(r["timers"]) == 1
