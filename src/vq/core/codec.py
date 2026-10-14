"""Разбор телеметрии, сериализация выходов и их заголовок vq-src-offset (§2.3)."""

import json
import math
from collections.abc import Container, Iterable, Mapping
from dataclasses import dataclass
from typing import Any

from vq.geo import fmt_ts

TS_MIN = 1_000_000_000  # 2001-09-09
TS_MAX = 4_102_444_800  # 2100-01-01, не включая
SPEED_MAX_KMH = 300.0
SRC_OFFSET_HEADER = "vq-src-offset"

# Заголовки записи Kafka: пары (имя, значение), как у confluent_kafka, или словарь; None — заголовков нет.
Headers = Mapping[str, str | bytes | None] | Iterable[tuple[str, str | bytes | None]] | None


@dataclass(frozen=True)
class Telemetry:
    unit_uuid: str
    ts: int
    lat: float
    lon: float
    speed_kmh: float


def _reject_constant(name: str) -> Any:
    raise ValueError(f"запрещённая константа {name}")


def _number(v: Any) -> float | None:
    """Конечное число как float, иначе None; bool числом не считается."""
    if isinstance(v, bool) or not isinstance(v, (int, float)):
        return None
    try:
        f = float(v)
    except OverflowError:  # целое за пределами float
        return None
    return f if math.isfinite(f) else None


def parse_telemetry(raw: bytes | None, units: Container[str]) -> Telemetry | None:
    """Сообщение telemetry.v1 → Telemetry, или None, если оно невалидно. Никогда не бросает.

    Вход — JSON-объект в UTF-8 (строгое декодирование: UTF-16/32 и битые байты невалидны).
    """
    if raw is None:
        return None
    try:
        obj = json.loads(str(raw, "utf-8"), parse_constant=_reject_constant)
    except (ValueError, TypeError, RecursionError):  # UnicodeDecodeError и JSONDecodeError — подклассы ValueError
        return None
    if not isinstance(obj, dict):
        return None
    unit = obj.get("unit_uuid")
    if not isinstance(unit, str) or unit not in units:
        return None
    ts = obj.get("ts")
    if isinstance(ts, bool):
        return None
    if isinstance(ts, float):
        if not math.isfinite(ts) or not ts.is_integer():
            return None
        ts = int(ts)
    if not isinstance(ts, int) or not TS_MIN <= ts < TS_MAX:
        return None
    lat, lon, speed = _number(obj.get("lat")), _number(obj.get("lon")), _number(obj.get("speed_kmh"))
    if lat is None or lon is None or speed is None:
        return None
    if abs(lat) > 90 or abs(lon) > 180 or not 0 <= speed < SPEED_MAX_KMH:
        return None
    return Telemetry(unit, ts, lat, lon, speed)


def dumps(obj: Any) -> str:
    """Компактный JSON выходов: без пробелов, не-ASCII как есть."""
    return json.dumps(obj, separators=(",", ":"), ensure_ascii=False)


def encode_telemetry(t: Telemetry) -> bytes:
    return dumps({"unit_uuid": t.unit_uuid, "ts": t.ts, "lat": t.lat, "lon": t.lon, "speed_kmh": t.speed_kmh}).encode()


def queue_message(station_uuid: str, at: int, rows_json: str) -> bytes:
    """rows_json — уже сериализованные строки очереди (то же, что хранится в last_published_queue)."""
    return f'{{"station_uuid":{dumps(station_uuid)},"at":"{fmt_ts(at)}","queue":{rows_json}}}'.encode()


def decision_recommended(unit: str, at: int, from_station: str, to_station: str, gain: int) -> bytes:
    return dumps(
        {
            "unit_uuid": unit,
            "at": fmt_ts(at),
            "result": "recommended",
            "from_station": from_station,
            "to_station": to_station,
            "gain_seconds": gain,
        }
    ).encode()


def decision_rejected(unit: str, at: int, reason: str) -> bytes:
    return dumps({"unit_uuid": unit, "at": fmt_ts(at), "result": "rejected", "reason": reason}).encode()


def src_headers(offset: int) -> list[tuple[str, bytes]]:
    """Заголовки выходной записи: offset сообщения telemetry.v1, обработка которого её породила, десятичной строкой."""
    return [(SRC_OFFSET_HEADER, str(offset).encode())]


def parse_src_offset(headers: Headers) -> int | None:
    """vq-src-offset из заголовков записи или None. Никогда не бросает.

    Решает первый заголовок с этим именем. Значение — только ASCII-цифры, как пишет src_headers: int() принял бы
    и ' 12', '+12', '1_000', '١٢', и потребители (verify, витрина) считали бы своим то, чего dispatcher не пишет.
    """
    items = headers.items() if isinstance(headers, Mapping) else headers or ()
    value = next((v for name, v in items if name == SRC_OFFSET_HEADER), None)
    raw = value.encode() if isinstance(value, str) and value.isascii() else value
    if not isinstance(raw, bytes) or not raw.isdigit():  # bytes.isdigit — только непустые ASCII 0-9
        return None
    try:
        return int(raw)
    except ValueError:  # больше 4300 цифр int() не разбирает (sys.get_int_max_str_digits)
        return None
