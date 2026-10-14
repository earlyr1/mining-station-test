"""Генерирует config/site.toml: площадка по §4.1 спеки и константы ТЗ (§3.1).

Разгрузка — точка из образца ТЗ; станции S1–S4 — ровно 10 000 м от неё по азимутам 0°, 10°, 20°, 30°.
Координаты пишутся с полной точностью float (repr). Константы — значения vq.config.Constants по умолчанию;
они входят в файл, поэтому их смена меняет config_hash и требует make reset (§2.5).

    uv run python tools/make_site.py            # перезаписать config/site.toml
    uv run python tools/make_site.py --check    # проверить, что файл совпадает с генерацией
"""

import argparse
import sys
from dataclasses import fields
from pathlib import Path

from vq.config import Constants
from vq.geo import destination

SITE_ID = "site-1"
DUMP = (49.1288, 142.6601)
STATION_DIST_M = 10_000.0
BEARINGS = {"S1": 0.0, "S2": 10.0, "S3": 20.0, "S4": 30.0}
FLEET = {"S1": 13, "S2": 11, "S3": 9, "S4": 7}  # T-01…T-40 по порядку
DEFAULT_OUT = Path(__file__).resolve().parent.parent / "config" / "site.toml"

# пояснения к константам в файле; ключи — поля Constants
CONSTANT_NOTES = {
    "service_s": "обслуживание одной машины, с",
    "maneuver_s": "манёвр между машинами, с (занятие = обслуживание + манёвр)",
    "speed_mps": "расчётная скорость, м/с (36 км/ч)",
    "radius_m": "радиус станции и точки разгрузки, м",
    "stop_kmh": "порог «машина стоит»: скорость меньше, км/ч",
    "decision_radius_m": "радиус принятия решения до своей станции, м",
    "horizon_s": "горизонт очереди, с",
    "fresh_s": "позиция свежая, если не старше, с",
    "min_gain_s": "минимальный выигрыш для рекомендации, с",
}


def assignment() -> dict[str, list[str]]:
    out: dict[str, list[str]] = {}
    n = 0
    for station, count in FLEET.items():
        out[station] = [f"T-{i:02d}" for i in range(n + 1, n + count + 1)]
        n += count
    return out


def constant_lines() -> list[str]:
    """Секция [constants]: целые — целыми, остальные — float (repr), как их ждёт проверка vq.config."""
    defaults = Constants()
    lines = ["[constants]"]
    for f in fields(defaults):
        note = CONSTANT_NOTES.get(f.name)
        lines.append(f"{f.name} = {getattr(defaults, f.name)!r}" + (f"  # {note}" if note else ""))
    return lines


def render() -> str:
    """Текст site.toml; детерминирован."""
    lines = [
        "# Сгенерировано tools/make_site.py — не править руками.",
        "",
        "[site]",
        f'id = "{SITE_ID}"',
        "",
        "[dump]",
        f"lat = {DUMP[0]!r}",
        f"lon = {DUMP[1]!r}",
    ]
    for uuid, brg in BEARINGS.items():
        lat, lon = destination(DUMP[0], DUMP[1], brg, STATION_DIST_M)
        lines += ["", "[[stations]]", f'uuid = "{uuid}"', f"lat = {lat!r}", f"lon = {lon!r}"]
    lines += ["", "[assignment]"]
    for station, units in assignment().items():
        quoted = ", ".join(f'"{u}"' for u in units)
        lines.append(f"{station} = [{quoted}]")
    lines += ["", *constant_lines()]
    return "\n".join(lines) + "\n"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", type=Path, default=DEFAULT_OUT)
    p.add_argument("--check", action="store_true", help="не писать, а сверить файл с генерацией")
    args = p.parse_args(argv)
    text = render()
    if args.check:
        ok = args.out.exists() and args.out.read_text(encoding="utf-8") == text
        print(f"{args.out}: {'совпадает' if ok else 'РАСХОДИТСЯ с генерацией'}")
        return 0 if ok else 1
    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text(text, encoding="utf-8")
    print(f"записан {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
