"""Конфигурация площадки: site.toml → SiteConfig, с проверками."""

import hashlib
import json
import math
import tomllib
from dataclasses import dataclass, fields
from functools import cached_property
from pathlib import Path
from typing import Any

from vq.geo import haversine


@dataclass(frozen=True)
class Point:
    lat: float
    lon: float


@dataclass(frozen=True)
class StationCfg:
    uuid: str
    lat: float
    lon: float


@dataclass(frozen=True)
class Constants:
    service_s: int = 200
    maneuver_s: int = 30
    speed_mps: float = 10.0
    radius_m: float = 50.0
    stop_kmh: float = 1.0
    decision_radius_m: float = 1500.0
    horizon_s: int = 1800
    fresh_s: int = 30
    min_gain_s: int = 60

    @property
    def occupy_s(self) -> int:
        return self.service_s + self.maneuver_s


@dataclass(frozen=True)
class SiteConfig:
    site_id: str
    dump: Point
    stations: tuple[StationCfg, ...]  # отсортированы по uuid
    assignment: dict[str, str]  # unit_uuid -> station_uuid
    constants: Constants

    def station(self, uuid: str) -> StationCfg:
        return self._by_uuid[uuid]

    def dump_leg_m(self, station_uuid: str) -> float:
        """Расстояние разгрузка → станция, м (постоянное, считается один раз)."""
        return self._dump_legs[station_uuid]

    @cached_property
    def _by_uuid(self) -> dict[str, StationCfg]:
        return {s.uuid: s for s in self.stations}

    @cached_property
    def _dump_legs(self) -> dict[str, float]:
        return {s.uuid: haversine(self.dump.lat, self.dump.lon, s.lat, s.lon) for s in self.stations}


class ConfigError(ValueError):
    pass


def build_config(raw: dict[str, Any]) -> SiteConfig:
    """Разобранный site.toml → SiteConfig. Любая ошибка структуры или значения — ConfigError, а не KeyError."""
    site = _table(raw, "site", "site.toml")
    dump = _table(raw, "dump", "site.toml")
    raw_stations = raw.get("stations")
    if not isinstance(raw_stations, list):
        raise ConfigError(f"stations: нужен массив таблиц [[stations]], а не {raw_stations!r}")
    stations = tuple(sorted((_station(s) for s in raw_stations), key=lambda s: s.uuid))
    assignment: dict[str, str] = {}
    for station_uuid, units in _table(raw, "assignment", "site.toml").items():
        if not isinstance(units, list) or not all(isinstance(u, str) for u in units):
            raise ConfigError(f"assignment.{station_uuid}: нужен список машин-строк, а не {units!r}")
        for unit in units:
            if unit in assignment:
                raise ConfigError(f"машина {unit} закреплена дважды")
            assignment[unit] = station_uuid
    cfg = SiteConfig(
        site_id=_str(site, "id", "site"),
        dump=Point(_num(dump, "lat", "dump"), _num(dump, "lon", "dump")),
        stations=stations,
        assignment=dict(sorted(assignment.items())),
        constants=_constants(raw.get("constants", {})),
    )
    validate(cfg)
    return cfg


def _table(raw: dict[str, Any], key: str, where: str) -> dict[str, Any]:
    value = raw.get(key)
    if not isinstance(value, dict):
        raise ConfigError(f"{where}: нужна таблица [{key}], а не {value!r}")
    return value


def _str(table: dict[str, Any], key: str, where: str) -> str:
    value = table.get(key)
    if not isinstance(value, str):
        raise ConfigError(f"{where}.{key}: нужна строка, а не {value!r}")
    return value


def _num(table: dict[str, Any], key: str, where: str) -> float:
    """Число TOML (bool числом не считается) как float; диапазон проверяет validate()."""
    value = table.get(key)
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ConfigError(f"{where}.{key}: нужно число, а не {value!r}")
    return float(value)


def _station(raw: object) -> StationCfg:
    if not isinstance(raw, dict):
        raise ConfigError(f"stations: нужна таблица, а не {raw!r}")
    uuid = _str(raw, "uuid", "stations")
    return StationCfg(uuid, _num(raw, "lat", f"stations.{uuid}"), _num(raw, "lon", f"stations.{uuid}"))


def _constants(raw: dict[str, Any]) -> Constants:
    try:
        return Constants(**raw)
    except TypeError as exc:  # неизвестный ключ или не таблица
        raise ConfigError(f"секция constants: {exc}") from exc


def _point(name: str, lat: float, lon: float) -> None:
    """Координаты — конечные числа в диапазоне. Иначе nan (TOML его допускает) молча ломает все радиусы."""
    for x in (lat, lon):
        if isinstance(x, bool) or not isinstance(x, (int, float)) or not math.isfinite(x):
            raise ConfigError(f"{name}: координаты должны быть конечными числами, а не ({lat!r}, {lon!r})")
    if abs(lat) > 90 or abs(lon) > 180:
        raise ConfigError(f"{name}: координаты вне диапазона ({lat!r}, {lon!r})")


INT_CONSTANTS = ("service_s", "maneuver_s", "horizon_s", "fresh_s", "min_gain_s")  # время — целые секунды (§3.2)


def _validate_constants(c: Constants) -> None:
    for f in fields(c):
        value = getattr(c, f.name)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ConfigError(f"константа {f.name} должна быть числом, а не {value!r}")
        if f.name in INT_CONSTANTS and not isinstance(value, int):
            raise ConfigError(f"константа {f.name} — целые секунды, а не {value!r}")
        if value < 0 or (value == 0 and f.name != "min_gain_s"):
            raise ConfigError(f"константа {f.name} должна быть положительной, а не {value!r}")


def validate(cfg: SiteConfig) -> None:
    c = cfg.constants
    _validate_constants(c)
    uuids = [s.uuid for s in cfg.stations]
    if not uuids or len(set(uuids)) != len(uuids):
        raise ConfigError("станции пусты или uuid повторяются")
    _point("разгрузка", cfg.dump.lat, cfg.dump.lon)
    for s in cfg.stations:
        _point(f"станция {s.uuid}", s.lat, s.lon)
    for unit, st in cfg.assignment.items():
        if not isinstance(unit, str):
            raise ConfigError(f"машина {unit!r}: unit_uuid должен быть строкой")
        if st not in uuids:
            raise ConfigError(f"машина {unit} закреплена за неизвестной станцией {st!r}")
    for i, a in enumerate(cfg.stations):
        if haversine(a.lat, a.lon, cfg.dump.lat, cfg.dump.lon) <= c.decision_radius_m + c.radius_m:
            raise ConfigError(f"разгрузка ближе {c.decision_radius_m + c.radius_m} м к {a.uuid}")
        for b in cfg.stations[i + 1 :]:
            if haversine(a.lat, a.lon, b.lat, b.lon) < 2 * c.radius_m:
                raise ConfigError(f"радиусы {a.uuid} и {b.uuid} пересекаются")


def load_config(path: str | Path) -> SiteConfig:
    with open(path, "rb") as f:
        return build_config(tomllib.load(f))


def config_hash(path: str | Path) -> str:
    """sha256 канонизированного site.toml: порядок ключей и форматирование не влияют."""
    with open(path, "rb") as f:
        raw = tomllib.load(f)
    canon = json.dumps(raw, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return hashlib.sha256(canon.encode()).hexdigest()
