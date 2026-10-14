"""Конфигурация config/site.toml (§3.1, §4.1): константы, проверки при загрузке, геометрия, зазор отсчётов (§4.2)."""

import copy
import importlib.util
import math
import tomllib
from collections import Counter
from pathlib import Path

import pytest

from tests.helpers import summary_line
from vq.config import ConfigError, Constants, build_config, config_hash, load_config
from vq.generator.sim import DRIVE, DUMP, MANEUVER, Sim
from vq.geo import destination, haversine, step_toward

ROOT = Path(__file__).resolve().parent.parent
SITE = ROOT / "config" / "site.toml"
GAP_M = 0.5  # отсчёт не ближе 0,5 м к порогу: шум float и libm не перекинет его через границу
RHU_GAP = 0.005  # дробная часть d/10 не ближе 0,005 к 0,5 (точка округления rhu)

# таблица констант ТЗ («Константы в конфигурации сервиса»); время — целые секунды, остальное — float
TASK_CONSTANTS = {
    "service_s": 200,
    "maneuver_s": 30,
    "speed_mps": 10.0,
    "radius_m": 50.0,
    "stop_kmh": 1.0,
    "decision_radius_m": 1500.0,
    "horizon_s": 1800,
    "fresh_s": 30,
    "min_gain_s": 60,
}


@pytest.fixture(scope="module")
def cfg():
    return load_config(SITE)


def raw_site() -> dict:
    with open(SITE, "rb") as f:
        return tomllib.load(f)


# --- константы в файле (§3.1, §7, ТЗ) ------------------------------------------------


def test_constants_are_in_site_toml_as_in_task_table(cfg):
    constants = raw_site()["constants"]
    assert constants == TASK_CONSTANTS
    assert {k: type(v) for k, v in constants.items()} == {k: type(v) for k, v in TASK_CONSTANTS.items()}
    assert cfg.constants == Constants()  # умолчания в коде совпадают с файлом
    assert cfg.constants.occupy_s == 230


def test_constants_come_from_file_and_change_config_hash(tmp_path):
    """Константа из файла меняет расчёт и config_hash: процессор на старом состоянии откажется стартовать (§2.5)."""
    text = SITE.read_text(encoding="utf-8")
    assert text.count("maneuver_s = 30") == 1
    changed = tmp_path / "site.toml"
    changed.write_text(text.replace("maneuver_s = 30", "maneuver_s = 40"), encoding="utf-8")
    assert load_config(changed).constants.occupy_s == 240
    assert config_hash(changed) != config_hash(SITE)


# --- проверки при загрузке (§3.1) ---------------------------------------------------


def with_station(raw: dict, uuid: str, pos: tuple[float, float]) -> dict:
    out = copy.deepcopy(raw)
    for s in out["stations"]:
        if s["uuid"] == uuid:
            s["lat"], s["lon"] = pos
    return out


def with_assignment(raw: dict, station: str, *units: str) -> dict:
    out = copy.deepcopy(raw)
    out["assignment"].setdefault(station, []).extend(units)
    return out


def with_constant(raw: dict, name: str, value: object) -> dict:
    out = copy.deepcopy(raw)
    out["constants"][name] = value
    return out


def near(raw: dict, uuid: str, bearing: float, dist: float) -> tuple[float, float]:
    s = next(s for s in raw["stations"] if s["uuid"] == uuid)
    return destination(s["lat"], s["lon"], bearing, dist)


def from_dump(raw: dict, dist: float) -> tuple[float, float]:
    return destination(raw["dump"]["lat"], raw["dump"]["lon"], 0.0, dist)


RAW = raw_site()


@pytest.mark.parametrize(
    ("raw", "ok"),
    [
        pytest.param(with_station(RAW, "S2", near(RAW, "S1", 90.0, 99.9)), False, id="radii_overlap_99.9m"),
        pytest.param(with_station(RAW, "S2", near(RAW, "S1", 90.0, 100.1)), True, id="radii_apart_100.1m"),
        pytest.param(with_station(RAW, "S1", from_dump(RAW, 1549.99)), False, id="dump_1549.99m"),
        pytest.param(with_station(RAW, "S1", from_dump(RAW, 1550.01)), True, id="dump_1550.01m"),
        pytest.param(with_assignment(RAW, "S2", "T-01"), False, id="vehicle_bound_twice"),
        pytest.param(with_assignment(RAW, "S9", "T-41"), False, id="unknown_station"),
        pytest.param({**RAW, "stations": [*RAW["stations"], dict(RAW["stations"][0])]}, False, id="duplicate_station"),
        pytest.param(with_constant(RAW, "warp_s", 1), False, id="unknown_constant"),
        pytest.param(with_constant(RAW, "service_s", 200.5), False, id="fractional_seconds"),
        pytest.param(with_constant(RAW, "radius_m", "50"), False, id="constant_not_a_number"),
    ],
)
def test_load_time_checks(raw, ok):
    if ok:
        build_config(raw)
    else:
        with pytest.raises(ConfigError):
            build_config(raw)


def test_real_site_passes_load_time_checks():
    assert build_config(RAW) == load_config(SITE)


def test_site_toml_is_what_make_site_generates():
    spec = importlib.util.spec_from_file_location("make_site", ROOT / "tools" / "make_site.py")
    assert spec is not None and spec.loader is not None
    make_site = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(make_site)
    assert SITE.read_text(encoding="utf-8") == make_site.render()


def test_radii_do_not_overlap_and_dump_is_far(cfg):
    c = cfg.constants
    points = [(s.uuid, s.lat, s.lon) for s in cfg.stations] + [("dump", cfg.dump.lat, cfg.dump.lon)]
    for i, (a, alat, alon) in enumerate(points):
        for b, blat, blon in points[i + 1 :]:
            assert haversine(alat, alon, blat, blon) > 2 * c.radius_m, (a, b)
    for s in cfg.stations:
        d = haversine(s.lat, s.lon, cfg.dump.lat, cfg.dump.lon)
        assert d > c.decision_radius_m + c.radius_m
        assert d == pytest.approx(10_000.0, abs=1e-6)  # §4.1: ровно 10 км
    assert [s.uuid for s in cfg.stations] == ["S1", "S2", "S3", "S4"]


def test_station_spacing_as_in_spec(cfg):
    """§4.1: соседние станции — около 1743 м друг от друга, S1–S4 — около 5176 м."""
    st = {s.uuid: s for s in cfg.stations}

    def dist(a: str, b: str) -> float:
        return haversine(st[a].lat, st[a].lon, st[b].lat, st[b].lon)

    for a, b in (("S1", "S2"), ("S2", "S3"), ("S3", "S4")):
        assert dist(a, b) == pytest.approx(1743.0, abs=1.0), (a, b)
    assert dist("S1", "S4") == pytest.approx(5176.0, abs=1.0)


def test_each_vehicle_bound_to_exactly_one_station(cfg):
    with open(SITE, "rb") as f:
        raw = tomllib.load(f)
    listed = [u for units in raw["assignment"].values() for u in units]
    assert sorted(listed) == [f"T-{i:02d}" for i in range(1, 41)]  # 40 машин, без повторов
    assert len(listed) == len(set(listed))
    assert Counter(cfg.assignment.values()) == {"S1": 13, "S2": 11, "S3": 9, "S4": 7}


def test_dump_station_routes_pass_far_from_other_stations(cfg):
    for s in cfg.stations:
        length = haversine(cfg.dump.lat, cfg.dump.lon, s.lat, s.lon)
        others = [o for o in cfg.stations if o.uuid != s.uuid]
        closest = math.inf
        for i in range(int(length) + 1):  # отсчёт через 1 м
            lat, lon = step_toward(cfg.dump.lat, cfg.dump.lon, s.lat, s.lon, float(i))
            closest = min(closest, *(haversine(lat, lon, o.lat, o.lon) for o in others))
        assert closest >= 100.0, (s.uuid, closest)


def frac_gap(x: float) -> float:
    """Расстояние дробной части x до 0,5 — точки, где rhu меняет значение."""
    return abs((x - math.floor(x)) - 0.5)


def test_generator_samples_keep_clear_of_thresholds_and_rounding_points(cfg, pytestconfig):
    """3 часа без решений и сбоев: каждый отсчёт в движении — не ближе 0,5 м к порогам 50 / 1500 м и без ничьих rhu."""
    c = cfg.constants
    sim = Sim(cfg, 42, 1_789_473_600)
    dump = cfg.dump
    leg = {s.uuid: haversine(dump.lat, dump.lon, s.lat, s.lon) for s in cfg.stations}
    worst = {"home50": math.inf, "home1500": math.inf, "dump50": math.inf, "rhu": math.inf}
    counts: Counter[str] = Counter()
    for _ in range(3 * 3600):
        for m in sim.tick():
            v = sim.vehicles[m.unit_uuid]
            if v.state not in (DRIVE, MANEUVER):
                continue
            home = cfg.station(v.home)
            d_home = haversine(m.lat, m.lon, home.lat, home.lon)
            d_dump = haversine(m.lat, m.lon, dump.lat, dump.lon)
            worst["home50"] = min(worst["home50"], abs(d_home - c.radius_m))
            if v.state == MANEUVER:
                counts["maneuver"] += 1
                continue
            worst["dump50"] = min(worst["dump50"], abs(d_dump - c.radius_m))
            if v.dest == DUMP:
                counts["to_dump"] += 1
                # ETA «к разгрузке»: оба плеча до округления; после захода в радиус разгрузки — прямо до станции
                worst["rhu"] = min(worst["rhu"], frac_gap((d_dump + leg[v.home]) / c.speed_mps))
                if d_dump < c.radius_m:
                    worst["rhu"] = min(worst["rhu"], frac_gap(d_home / c.speed_mps))
            else:
                assert v.dest == v.home  # без решений перенаправлений нет
                counts["to_station"] += 1
                worst["home1500"] = min(worst["home1500"], abs(d_home - c.decision_radius_m))
                worst["rhu"] = min(worst["rhu"], frac_gap(d_home / c.speed_mps))
    assert counts["to_station"] > 100_000 and counts["to_dump"] > 100_000 and counts["maneuver"] > 1_000
    assert min(v.cycles for v in sim.vehicles.values()) >= 1  # обратное плечо после разгрузки тоже проверено
    summary_line(
        pytestconfig,
        "наименьший зазор отсчётов генератора до порогов: 50 м станции — {home50:.3f} м, 1500 м — {home1500:.3f} м, "
        "50 м разгрузки — {dump50:.3f} м; до точки округления rhu — {rhu:.4f} с".format(**worst),
    )
    assert worst["home50"] >= GAP_M and worst["home1500"] >= GAP_M and worst["dump50"] >= GAP_M, worst
    assert worst["rhu"] >= RHU_GAP, worst
