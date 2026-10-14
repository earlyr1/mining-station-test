"""Геометрия и время: гаверсинус, точки на сфере, округление, формат времени."""

import math
from datetime import UTC, datetime

EARTH_RADIUS_M = 6_371_008.8
_TS_FORMAT = "%Y-%m-%dT%H:%M:%SZ"


def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Расстояние по дуге большого круга в метрах."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp = p2 - p1
    dl = math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    # порядок аргументов min важен: min(nan, 1.0) — nan, а min(1.0, nan) — 1.0, то есть правдоподобные π·R
    return 2 * EARTH_RADIUS_M * math.asin(min(math.sqrt(a), 1.0))


def destination(lat: float, lon: float, bearing_deg: float, dist_m: float) -> tuple[float, float]:
    """Точка на расстоянии dist_m по азимуту bearing_deg."""
    d = dist_m / EARTH_RADIUS_M
    b = math.radians(bearing_deg)
    p1, l1 = math.radians(lat), math.radians(lon)
    p2 = math.asin(math.sin(p1) * math.cos(d) + math.cos(p1) * math.sin(d) * math.cos(b))
    l2 = l1 + math.atan2(math.sin(b) * math.sin(d) * math.cos(p1), math.cos(d) - math.sin(p1) * math.sin(p2))
    return math.degrees(p2), (math.degrees(l2) + 540.0) % 360.0 - 180.0


def bearing(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Начальный азимут от первой точки ко второй, градусы."""
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dl = math.radians(lon2 - lon1)
    y = math.sin(dl) * math.cos(p2)
    x = math.cos(p1) * math.sin(p2) - math.sin(p1) * math.cos(p2) * math.cos(dl)
    return math.degrees(math.atan2(y, x))


def step_toward(lat: float, lon: float, lat2: float, lon2: float, step_m: float) -> tuple[float, float]:
    """Сдвиг на step_m по дуге к цели; если цель ближе, возвращается цель."""
    if haversine(lat, lon, lat2, lon2) <= step_m:
        return lat2, lon2
    return destination(lat, lon, bearing(lat, lon, lat2, lon2), step_m)


def rhu(x: float) -> int:
    """Округление половины вверх. Встроенный round() банковский."""
    return math.floor(x + 0.5)


def fmt_ts(ts: int) -> str:
    """Секунды epoch → 'YYYY-MM-DDTHH:MM:SSZ'."""
    return datetime.fromtimestamp(ts, tz=UTC).strftime(_TS_FORMAT)


def parse_ts(text: str) -> int:
    """'YYYY-MM-DDTHH:MM:SSZ' → секунды epoch: строгая обратная fmt_ts, принимает ровно то, что она пишет.

    Другой вид (доли секунды, смещение, пробелы) или несуществующий момент — ValueError, не строка — TypeError.
    """
    ts = int(datetime.strptime(text, _TS_FORMAT).replace(tzinfo=UTC).timestamp())
    if fmt_ts(ts) != text:  # strptime терпит одну цифру, день с пробелом, строчные t и z, не-ASCII цифры
        raise ValueError(f"ожидается YYYY-MM-DDTHH:MM:SSZ, получено {text[:40]!r}")
    return ts
