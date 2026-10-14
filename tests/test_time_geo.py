"""Время, округление и геометрия (§3.2): rhu, формат времени, гаверсинус, точка по азимуту, целые секунды.
Проверки конфигурации при загрузке (§3.1): константы, координаты, закрепление, структура site.toml.

Строки §3.11: п. 6 (округление половины вверх), п. 22 (опорное время примера).
"""

import dataclasses
import math
import tomllib

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from tests.helpers import BASE, NOON, two_stations
from vq.config import ConfigError, Constants, Point, StationCfg, build_config, validate
from vq.core.codec import TS_MAX, TS_MIN
from vq.geo import EARTH_RADIUS_M, bearing, destination, fmt_ts, haversine, parse_ts, rhu, step_toward


@pytest.mark.parametrize(
    ("x", "expected"),
    [(120.5, 121), (110.5, 111), (-0.5, 0), (2.5, 3), (0.49, 0), (0.5, 1), (119.4999, 119), (-1.5, -1), (300.0, 300)],
)
def test_rhu_rounds_half_up(x, expected):
    assert rhu(x) == expected
    assert isinstance(rhu(x), int)


def test_rhu_is_not_bankers_round():
    assert (rhu(110.5), round(110.5)) == (111, 110)
    assert (rhu(120.5), round(120.5)) == (121, 120)


def test_fmt_ts_iso_utc_with_z():
    assert fmt_ts(NOON) == "2026-09-15T12:00:00Z"  # опорное «сейчас» примера из задания
    assert fmt_ts(NOON - 60) == "2026-09-15T11:59:00Z"
    assert fmt_ts(NOON + 170) == "2026-09-15T12:02:50Z"
    assert fmt_ts(1_000_000_000) == "2001-09-09T01:46:40Z"
    assert fmt_ts(4_102_444_799) == "2099-12-31T23:59:59Z"


@given(st.integers(TS_MIN, TS_MAX - 1))
def test_parse_ts_is_the_inverse_of_fmt_ts(ts):
    assert parse_ts(fmt_ts(ts)) == ts


@pytest.mark.parametrize(
    "text",
    [
        "2026-09-15T12:00:00",  # без Z
        "2026-09-15T12:00:00.250Z",  # доли секунды
        "2026-09-15T12:00:00+00:00",  # смещение
        "2026-09-15T15:00:00+03:00",
        " 2026-09-15T12:00:00Z",  # пробелы
        "2026-09-15T12:00:00Z ",
        "2026-09-15 12:00:00Z",
        # strptime это терпит: одна цифра, день с пробелом, строчные t и z, не-ASCII цифры
        "2026-9-15T12:00:00Z",
        "2026-09- 5T12:00:00Z",
        "2026-09-15T1:2:3Z",
        "2026-09-15t12:00:00z",
        "２０２６-09-15T12:00:00Z",
        # нет такого момента
        "2026-02-30T00:00:00Z",
        "2026-09-15T24:00:00Z",
        "2026-09-15T12:00:60Z",
        "",
    ],
)
def test_parse_ts_rejects_near_misses(text):
    with pytest.raises(ValueError):
        parse_ts(text)


# почти время: поля из одной или двух цифр, день с пробелом, строчные t и z, доли секунды, без Z
NEAR_TS = r"2[01][0-9]{2}-[0-9]{1,2}-[ 0-9]{1,2}[Tt ][0-9]{1,2}:[0-9]{1,2}:[0-9]{1,2}(\.[0-9])?[Zz]?"


@settings(max_examples=500)
@given(st.from_regex(NEAR_TS, fullmatch=True))
def test_parse_ts_accepts_only_what_fmt_ts_writes(text):
    try:
        ts = parse_ts(text)
    except ValueError:
        return
    assert fmt_ts(ts) == text


@pytest.mark.parametrize("value", [None, 7, b"2026-09-15T12:00:00Z", ["2026-09-15T12:00:00Z"]])
def test_parse_ts_of_a_non_string_is_a_type_error(value):
    """Генератор ловит TypeError вместе с ValueError и считает такую запись decision.v1 невалидной."""
    with pytest.raises(TypeError):
        parse_ts(value)


def test_haversine_one_degree_of_latitude():
    one_degree = EARTH_RADIUS_M * math.pi / 180  # 111 195,08 м при R = 6 371 008,8 м
    assert EARTH_RADIUS_M == 6_371_008.8
    assert haversine(0.0, 0.0, 1.0, 0.0) == pytest.approx(one_degree, abs=1e-6)
    assert haversine(49.0, 142.0, 50.0, 142.0) == pytest.approx(111_195.08, abs=0.01)
    assert haversine(0.0, 0.0, 0.0, 1.0) == pytest.approx(one_degree, abs=1e-6)  # градус долготы на экваторе


def test_haversine_reference_points():
    assert haversine(*BASE, *BASE) == 0.0
    assert haversine(0.0, 0.0, 0.0, 180.0) == pytest.approx(math.pi * EARTH_RADIUS_M, abs=1e-6)  # антиподы
    assert haversine(90.0, 0.0, -90.0, 0.0) == pytest.approx(math.pi * EARTH_RADIUS_M, abs=1e-6)
    # две точки на одной параллели: d = 2R·asin(cos φ · sin(Δλ/2)) — независимая формула
    on_parallel = 2 * EARTH_RADIUS_M * math.asin(math.cos(math.radians(60.0)) * math.sin(math.radians(0.5)))
    assert haversine(60.0, 10.0, 60.0, 11.0) == pytest.approx(on_parallel, abs=1e-6)
    assert on_parallel == pytest.approx(55_597.01, abs=0.01)
    a, b = (49.1288, 142.6601), (49.2187, 142.7311)
    assert haversine(*a, *b) == haversine(*b, *a)


def test_haversine_propagates_nan_instead_of_plausible_distance():
    # min(1.0, nan) == 1.0: раньше nan превращался в π·R, и битая координата выглядела далёкой точкой
    assert math.isnan(haversine(math.nan, 0.0, 0.0, 0.0))
    assert math.isnan(haversine(*BASE, BASE[0], math.nan))


@pytest.mark.parametrize("dist", [1_200.0, 3_000.0, 900.0, 2_400.0, 50.0, 1_500.0, 10_000.0])
@pytest.mark.parametrize("brg", [0.0, 37.0, 90.0, 180.0, 270.0, 333.3])
def test_destination_haversine_round_trip(dist, brg):
    p = destination(*BASE, brg, dist)
    assert abs(haversine(*BASE, *p) - dist) < 1e-6


def test_bearing_matches_destination():
    for brg in (0.0, 10.0, 90.0, 135.0, 200.0, 315.0):
        p = destination(*BASE, brg, 2_000.0)
        assert (bearing(*BASE, *p) - brg + 180.0) % 360.0 - 180.0 == pytest.approx(0.0, abs=1e-9)


def test_step_toward_moves_exact_step_and_snaps_to_target():
    target = destination(*BASE, 30.0, 95.0)
    p = step_toward(*BASE, *target, 10.0)
    assert haversine(*BASE, *p) == pytest.approx(10.0, abs=1e-6)
    assert haversine(*p, *target) == pytest.approx(85.0, abs=1e-6)
    assert step_toward(*BASE, *target, 95.0) == target
    assert step_toward(*BASE, *target, 100.0) == target


@pytest.mark.parametrize(
    "bad",
    [{"service_s": 200.0}, {"fresh_s": True}, {"horizon_s": "1800"}, {"speed_mps": 0}, {"radius_m": math.nan}],
)
def test_time_constants_are_integer_seconds_and_constants_are_positive_numbers(bad):
    cfg = two_stations()
    with pytest.raises(ConfigError):
        validate(dataclasses.replace(cfg, constants=Constants(**bad)))


def test_default_constants_and_zero_min_gain_are_valid():
    cfg = two_stations()
    validate(cfg)
    validate(dataclasses.replace(cfg, constants=Constants(min_gain_s=0)))
    assert cfg.constants.occupy_s == 230


SITE_TOML = """
[site]
id = "t"
[dump]
lat = 49.1288
lon = 142.6601
[[stations]]
uuid = "S1"
lat = 49.218732036372444
lon = 142.66010000000006
[assignment]
S1 = ["T-01", "T-02"]
"""


def test_build_config_from_toml():
    cfg = build_config(tomllib.loads(SITE_TOML))
    assert (cfg.site_id, cfg.dump, cfg.stations) == (
        "t",
        Point(49.1288, 142.6601),
        (StationCfg("S1", 49.218732036372444, 142.66010000000006),),
    )
    assert (cfg.assignment, cfg.constants) == ({"T-01": "S1", "T-02": "S1"}, Constants())


BAD_SITE_TOML = {
    # координаты: TOML допускает nan/inf; nan давал «расстояние» π·R (станцию нельзя занять), inf — ValueError
    "station_lat_nan": ("lat = 49.218732036372444", "lat = nan"),
    "dump_lat_nan": ("lat = 49.1288", "lat = nan"),
    "dump_lat_inf": ("lat = 49.1288", "lat = inf"),
    "station_lon_181": ("lon = 142.66010000000006", "lon = 181.0"),
    "dump_lat_below_-90": ("lat = 49.1288", "lat = -90.5"),
    "dump_lat_string": ("lat = 49.1288", 'lat = "49.1288"'),
    "dump_lat_bool": ("lat = 49.1288", "lat = true"),
    # закрепление: строка вместо списка давала машины 'T', '-', '0', '1', и все сообщения были бы invalid
    "assignment_string": ('S1 = ["T-01", "T-02"]', 'S1 = "T-01"'),
    "assignment_not_string_unit": ('S1 = ["T-01", "T-02"]', 'S1 = ["T-01", 2]'),
    "assignment_twice": ('S1 = ["T-01", "T-02"]', 'S1 = ["T-01", "T-01"]'),
    "assignment_unknown_station": ('S1 = ["T-01", "T-02"]', 'S9 = ["T-01"]'),
    # структура: KeyError / TypeError вместо понятной ошибки
    "dump_missing": ("[dump]\nlat = 49.1288\nlon = 142.6601\n", ""),
    "station_uuid_missing": ('uuid = "S1"\n', ""),
    "site_id_not_string": ('id = "t"', "id = 1"),
    "constant_float_seconds": ("[assignment]", "[constants]\nservice_s = 200.0\n[assignment]"),
    "constant_unknown": ("[assignment]", "[constants]\nservice = 200\n[assignment]"),
}


@pytest.mark.parametrize(("old", "new"), BAD_SITE_TOML.values(), ids=BAD_SITE_TOML.keys())
def test_bad_site_toml_raises_config_error(old, new):
    assert SITE_TOML.count(old) == 1
    with pytest.raises(ConfigError):
        build_config(tomllib.loads(SITE_TOML.replace(old, new)))


def test_validate_rejects_non_finite_coordinates_of_config_built_in_code():
    cfg = two_stations()
    with pytest.raises(ConfigError):
        validate(dataclasses.replace(cfg, dump=Point(math.nan, 142.0)))
    bad = StationCfg("S1", cfg.stations[0].lat, math.inf)
    with pytest.raises(ConfigError):
        validate(dataclasses.replace(cfg, stations=(bad, cfg.stations[1])))
