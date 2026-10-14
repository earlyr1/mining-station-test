"""Состояние ядра: машины, станции, площадка. Поля один к одному совпадают с таблицами Postgres."""

from dataclasses import dataclass, field
from typing import Literal

TO_STATION = "to_station"
WAITING = "waiting"
SERVICE = "service"
SERVED = "served"
TO_DUMP = "to_dump"


@dataclass
class Vehicle:
    unit_uuid: str
    home_station: str
    last_ts: int
    lat: float
    lon: float
    speed_kmh: float
    phase: str
    target_station: str
    radius_station: str | None = None  # станция, в радиусе которой машина; ведётся в фазе to_station (§3.5)
    radius_entered_at: int | None = None  # вход в радиус radius_station
    arrived_at: int | None = None
    dump_visited: bool = False
    decided: bool = False
    predicted_start: int | None = None


@dataclass
class Station:
    station_uuid: str
    occupant_uuid: str | None = None
    occupied_at: int | None = None
    last_release_at: int | None = None
    last_published_queue: str | None = None  # канонический JSON строк; None — ещё не публиковалась


@dataclass
class SiteState:
    now: int | None = None
    vehicles: dict[str, Vehicle] = field(default_factory=dict)
    stations: dict[str, Station] = field(default_factory=dict)


# события ядра (§3.10); опечатку mypy ловит и в event("…"), и в e.kind == "…" (strict_equality)
EventKind = Literal[
    "accepted",
    "invalid",
    "not_newer",
    "fsm_resync",
    "occupied",
    "released",
    "decision",
    "prediction_error",
    "queue_published",
]


@dataclass(frozen=True)
class Event:
    kind: EventKind
    attrs: tuple[tuple[str, str | int], ...] = ()

    def get(self, key: str) -> str | int | None:
        return dict(self.attrs).get(key)


def event(kind: EventKind, **attrs: str | int) -> Event:
    return Event(kind, tuple(sorted(attrs.items())))


@dataclass(frozen=True)
class Output:
    topic: str
    key: bytes
    value: bytes


@dataclass(frozen=True)
class StepResult:
    outputs: tuple[Output, ...]
    events: tuple[Event, ...]
