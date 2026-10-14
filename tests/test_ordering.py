"""Порядок, дубли и невалидные сообщения (§3.4 шаги 1–2).

Строки §3.11: п. 17 (невалидные пропускаются, «сейчас» не двигают). Свойство: Engine.apply не бросает
исключений ни на каком входе, а невалидный или не новый вход не меняет состояние и не даёт выходов.
"""

import copy
import json

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from tests.helpers import NOON, at, feed, kinds, msg, outputs, two_stations
from vq.core.codec import TS_MAX, TS_MIN, parse_telemetry
from vq.core.engine import Engine
from vq.core.model import SERVED
from vq.core.queue import station_rows

CFG = two_stations()


def prepared() -> Engine:
    """S1 занята T1, T2 ждёт, U1 едет к S2, «сейчас» = NOON + 10."""
    e = Engine(CFG)
    feed(
        e,
        msg("T1", NOON, at(CFG, "S1", 0.0, 10.0), speed=0.0),
        msg("T2", NOON + 5, at(CFG, "S1", 90.0, 20.0), speed=0.0),
        msg("U1", NOON + 10, at(CFG, "S2", 180.0, 2_000.0)),
    )
    return e


def payload(**fields) -> bytes:
    base = {"unit_uuid": "T3", "ts": NOON + 500, "lat": 49.2, "lon": 142.7, "speed_kmh": 36.0}
    base.update(fields)
    return json.dumps({k: v for k, v in base.items() if v is not ...}, allow_nan=True).encode()


def assert_ignored(e: Engine, raw: bytes | None, kind: str) -> None:
    before = copy.deepcopy(e.state)
    r = e.apply(raw)
    assert [ev.kind for ev in r.events] == [kind]
    assert r.outputs == ()
    assert e.state == before


def test_duplicate_and_old_ts_are_ignored():
    e = prepared()
    assert_ignored(e, msg("T1", NOON, at(CFG, "S1", 0.0, 10.0), speed=0.0), "not_newer")  # дубль
    assert_ignored(e, msg("T1", NOON - 5, at(CFG, "S1", 180.0, 900.0)), "not_newer")  # старый ts
    assert_ignored(e, msg("U1", NOON + 10, at(CFG, "S2", 180.0, 10.0), speed=0.0), "not_newer")  # тот же ts, другое


def test_late_but_newer_message_is_accepted_and_does_not_move_now_back():
    e = prepared()
    r = feed(e, msg("T1", NOON + 3, at(CFG, "S1", 0.0, 12.0), speed=0.0))  # новее для T1, старше «сейчас»
    assert kinds(r)[0] == "accepted"
    v = e.state.vehicles["T1"]
    assert (v.last_ts, e.state.now) == (NOON + 3, NOON + 10)


def test_late_but_newer_eta_counts_from_its_own_ts():
    e = prepared()
    r = feed(e, msg("U2", NOON + 4, at(CFG, "S2", 180.0, 1_000.0)))  # опоздало на 6 с, но первое для U2
    assert kinds(r)[0] == "accepted"
    u2 = [row for row in station_rows(e.state, CFG, "S2") if row.unit_uuid == "U2"]
    assert u2[0].eta == NOON + 4 + 100


def test_clock_running_ahead_moves_now_forward_and_makes_other_vehicles_stale():
    """Фиксирует известное ограничение (§10): часы машины спешат. Это буква ТЗ: «сейчас» — наибольший учтённый ts,
    а ts на час вперёд валиден (2001–2100); защиты вроде watermark нет, остальные машины становятся несвежими."""
    e = prepared()
    r = feed(e, msg("T3", NOON + 3_600, at(CFG, "S1", 180.0, 5_000.0)))  # часы T3 спешат на час
    assert e.state.now == NOON + 3_600
    # в этом же шаге таймер T1 (NOON + 230 ≤ «сейчас»); ждущая T2 несвежая — передачи нет
    assert [(ev.get("station"), ev.get("reason")) for ev in r[0].events if ev.kind == "released"] == [("S1", "timer")]
    assert (e.state.stations["S1"].occupant_uuid, e.state.vehicles["T1"].phase) == (None, SERVED)
    # T2 и U1 несвежие и ушли из очередей своих станций в этом же шаге; осталась только T3
    queues = [(q["station_uuid"], q["at"], [row["unit_uuid"] for row in q["queue"]]) for q in outputs(r, "queue.v1")]
    assert queues == [("S1", "2026-09-15T13:00:00Z", ["T3"]), ("S2", "2026-09-15T13:00:00Z", [])]
    r = feed(e, msg("T1", NOON + 10, at(CFG, "S1", 0.0, 10.0), speed=0.0))  # обычное сообщение T1
    assert kinds(r) == ["accepted"]  # новее её last_ts — учтено; очереди не изменились
    assert (e.state.vehicles["T1"].last_ts, e.state.now) == (NOON + 10, NOON + 3_600)  # «сейчас» назад не идёт


INVALID = {
    "nan": b'{"unit_uuid":"T3","ts":1789474100,"lat":NaN,"lon":142.7,"speed_kmh":36.0}',
    "infinity": b'{"unit_uuid":"T3","ts":1789474100,"lat":49.2,"lon":Infinity,"speed_kmh":36.0}',
    "-infinity": b'{"unit_uuid":"T3","ts":1789474100,"lat":49.2,"lon":142.7,"speed_kmh":-Infinity}',
    "overflow_float": b'{"unit_uuid":"T3","ts":1789474100,"lat":1e400,"lon":142.7,"speed_kmh":36.0}',
    "overflow_int": b'{"unit_uuid":"T3","ts":1789474100,"lat":' + b"9" * 400 + b',"lon":142.7,"speed_kmh":36.0}',
    "ts_ms": payload(ts=(NOON + 500) * 1000),
    "ts_below_2001": payload(ts=TS_MIN - 1),
    "ts_2100": payload(ts=TS_MAX),
    "ts_fraction": payload(ts=NOON + 500.5),
    "ts_string": payload(ts=str(NOON + 500)),
    "ts_bool": payload(ts=True),
    "ts_missing": payload(ts=...),
    "lat_above_90": payload(lat=90.0001),
    "lat_below_-90": payload(lat=-90.5),
    "lon_above_180": payload(lon=180.0001),
    "lon_below_-180": payload(lon=-181),
    "speed_negative": payload(speed_kmh=-0.1),
    "speed_300": payload(speed_kmh=300),
    "speed_huge": payload(speed_kmh=1e6),
    "lat_bool": payload(lat=True),
    "speed_bool": payload(speed_kmh=False),
    "lat_string": payload(lat="49.2"),
    "lat_list": payload(lat=[49.2]),
    "speed_null": payload(speed_kmh=None),
    "unknown_unit": payload(unit_uuid="X9"),
    "unit_not_string": payload(unit_uuid=3),
    "unit_missing": payload(unit_uuid=...),
    "array": b"[1, 2]",
    "string": b'"T3"',
    "number": b"1789474100",
    "null": b"null",
    "true": b"true",
    "empty": b"",
    "truncated": payload()[:-3],
    "not_json": b"unit_uuid=T3",
    "non_utf8_byte": b'{"unit_uuid":"T3\xff","ts":1789474100,"lat":49.2,"lon":142.7,"speed_kmh":36.0}',
    "latin1": '{"unit_uuid":"Тé","ts":1789474100,"lat":49.2,"lon":142.7,"speed_kmh":36.0}'.encode("latin-1", "replace"),
    "utf16": payload().decode().encode("utf-16"),
    "utf8_bom": b"\xef\xbb\xbf" + payload(),
    "deep_nesting": b"[" * 100_000 + b"]" * 100_000,
}


@pytest.mark.parametrize("raw", INVALID.values(), ids=INVALID.keys())
def test_invalid_input_is_ignored_and_does_not_move_now(raw):
    e = prepared()
    assert_ignored(e, raw, "invalid")
    assert e.state.now == NOON + 10


def test_none_value_is_invalid():
    assert_ignored(prepared(), None, "invalid")


def test_invalid_first_message_publishes_nothing():
    e = Engine(CFG)
    assert_ignored(e, b"garbage", "invalid")
    assert e.state.now is None


def test_float_ts_with_integer_value_is_accepted_as_int():
    e = prepared()
    r = e.apply(payload(ts=float(NOON + 20)))
    assert r.events[0].kind == "accepted"
    v = e.state.vehicles["T3"]
    assert v.last_ts == NOON + 20 and type(v.last_ts) is int
    assert e.state.now == NOON + 20 and type(e.state.now) is int


def test_int_coordinates_and_boundary_values_are_valid():
    for fields in (
        {"lat": 49, "lon": 142, "speed_kmh": 0},
        {"lat": 90.0, "lon": 180.0, "speed_kmh": 299.99},
        {"lat": -90.0, "lon": -180.0, "speed_kmh": 0.0},
        {"ts": TS_MIN},
        {"ts": TS_MAX - 1},
        {"ts": 1.7894741e9},  # float с целым значением в экспоненциальной записи
    ):
        assert parse_telemetry(payload(**fields), CFG.assignment) is not None, fields


# --- свойства (hypothesis) -----------------------------------------------------------------

JSON_JUNK = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(),
    st.integers(min_value=10**300, max_value=10**400),
    st.floats(allow_nan=True, allow_infinity=True),
    st.text(max_size=6),
    st.lists(st.integers(), max_size=2),
    st.dictionaries(st.text(max_size=2), st.integers(), max_size=2),
)
UNITS = sorted(CFG.assignment)


@st.composite
def near_valid_message(draw) -> bytes:
    """Почти валидный JSON: поля иногда другого типа, вне диапазона, пропущены, с лишними ключами."""

    def pick(valid):
        return draw(valid if draw(st.integers(0, 4)) else JSON_JUNK)

    station = draw(st.sampled_from(["S1", "S2"]))
    dist = draw(st.sampled_from([0.0, 10.0, 49.0, 51.0, 900.0, 1_499.0, 1_600.0, 3_000.0, 10_000.0]))
    lat, lon = at(CFG, station, draw(st.floats(0.0, 360.0)), dist)
    ts_valid = st.integers(NOON - 60, NOON + 400)
    fields = {
        "unit_uuid": pick(st.sampled_from([*UNITS, "X9"])),
        "ts": pick(
            st.one_of(ts_valid, ts_valid.map(float), st.integers(TS_MAX - 2, TS_MAX + 2).map(lambda x: x * 1000))
        ),
        "lat": pick(st.one_of(st.just(lat), st.floats(-91.0, 91.0))),
        "lon": pick(st.one_of(st.just(lon), st.floats(-181.0, 181.0))),
        "speed_kmh": pick(st.one_of(st.sampled_from([0.0, 0.5, 0.99, 1.0, 36.0]), st.floats(-1.0, 301.0))),
    }
    missing = draw(st.sets(st.sampled_from(sorted(fields)), max_size=1))
    extra = draw(st.dictionaries(st.text(max_size=3), JSON_JUNK, max_size=2))
    obj = {**extra, **{k: v for k, v in fields.items() if k not in missing}}
    return json.dumps(obj, allow_nan=True).encode()


@st.composite
def corrupted_message(draw) -> bytes:
    """Валидное сообщение с испорченными байтами: замена, вставка или обрезка."""
    raw = bytearray(msg(draw(st.sampled_from(UNITS)), draw(st.integers(NOON, NOON + 300)), at(CFG, "S1", 0.0, 10.0)))
    i = draw(st.integers(0, len(raw)))
    junk = draw(st.binary(min_size=1, max_size=4))
    op = draw(st.sampled_from(["replace", "insert", "cut"]))
    if op == "replace":
        raw[i : i + len(junk)] = junk
    elif op == "insert":
        raw[i:i] = junk
    else:
        del raw[i:]
    return bytes(raw)


def check_step(e: Engine, raw: bytes | None) -> None:
    """Применить сообщение и проверить контракт: не бросает; игнорируемое не меняет ничего."""
    before = copy.deepcopy(e.state)
    r = e.apply(raw)
    ks = [ev.kind for ev in r.events]
    parsed = parse_telemetry(raw, CFG.assignment)
    if parsed is None:
        assert ks == ["invalid"] and r.outputs == () and e.state == before
    elif parsed.unit_uuid in before.vehicles and parsed.ts <= before.vehicles[parsed.unit_uuid].last_ts:
        assert ks == ["not_newer"] and r.outputs == () and e.state == before
    else:
        assert ks[0] == "accepted"
        assert e.state.now == (parsed.ts if before.now is None else max(before.now, parsed.ts))
        assert e.state.vehicles[parsed.unit_uuid].last_ts == parsed.ts


@settings(max_examples=300, deadline=None)
@given(st.binary(max_size=300))
def test_apply_never_raises_on_arbitrary_bytes(raw):
    check_step(prepared(), raw)


@settings(max_examples=200, deadline=None, suppress_health_check=[HealthCheck.too_slow])
@given(st.lists(st.one_of(near_valid_message(), corrupted_message(), st.none()), min_size=1, max_size=25))
def test_apply_never_raises_and_ignored_input_never_changes_state(raws):
    e = prepared()
    for raw in raws:
        check_step(e, raw)
