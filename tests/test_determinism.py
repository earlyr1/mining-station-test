"""Детерминизм (§2.7): выходы — чистая функция потока; пачки, чекпоинт и загрузка состояния их не меняют.

Ядро применяет сообщения по одному, поэтому «пачка» здесь — как у dispatcher: после каждой пачки состояние
сохраняется строками таблиц (dataclasses.asdict → json.dumps, как колонки §2.5), читается в перемешанном
порядке строк и собирается в новый Engine. Выходы и события обязаны совпасть с непрерывным прогоном байт в байт.

Строки §3.11: п. 18 (повтор хвоста после падения даёт те же записи байт в байт — часть ядра; заголовок
vq-src-offset и at-least-once — в тестах dispatcher); п. 21 — только косвенно: ядро не знает о брокере и I/O
(test_pure_modules_have_no_clock_io_or_free_randomness), сама работа на Redpanda проверяется сквозным make e2e.
"""

import ast
import copy
import dataclasses
import json
import random
from collections import Counter
from functools import cache
from pathlib import Path
from typing import get_args

import pytest
from hypothesis import given, settings
from hypothesis import strategies as st

from tests.helpers import NOON, at, fleet_stream, msg, two_stations
from vq.core.engine import Engine
from vq.core.model import EventKind, SiteState, Station, StepResult, Vehicle

CFG = two_stations()

Record = tuple[tuple[tuple[str, bytes, bytes], ...], tuple[tuple[str, tuple], ...]]


@cache
def stream() -> tuple[bytes, ...]:
    """Машины едут к S1/S2, ждут, обслуживаются, выезжают, едут к разгрузке и обратно; дубли, опоздания, обрывы."""
    starts = {"T1": 1_603.0, "T2": 1_653.0, "T3": 1_803.0, "T4": 2_203.0, "U1": 1_703.0, "U2": 2_003.0}
    raws = fleet_stream(
        CFG,
        starts,
        ticks=1_000,
        seed=5,
        dup_rate=0.02,
        late_rate=0.01,
        outage_rate=0.003,
        leg_m=100.0,
        service_s={"U2": 120},  # U2 уезжает раньше таймера: освобождение по выезду
    )
    raws.insert(150, b'{"unit_uuid":"T1","ts":NaN}')
    raws.insert(3_000, msg("X9", NOON + 400, at(CFG, "S1", 0.0, 10.0)))  # неизвестная машина
    return tuple(raws)


def record(r: StepResult) -> Record:
    return (
        tuple((o.topic, o.key, o.value) for o in r.outputs),
        tuple((ev.kind, ev.attrs) for ev in r.events),
    )


@cache
def baseline() -> tuple[Record, ...]:
    e = Engine(CFG)
    return tuple(record(e.apply(raw)) for raw in stream())


def save(state: SiteState, drop: tuple[str, ...] = ()) -> dict[str, list[str]]:
    """Чекпоинт: строки processor / stations / vehicles, по JSON на строку, как колонки таблиц.

    drop — колонки vehicles, которые «забыли» сохранить (при загрузке у них значение по умолчанию): контроль, что
    проверка их замечает.
    """
    return {
        "processor": [json.dumps({"now_ts": state.now})],
        "stations": [json.dumps(dataclasses.asdict(s)) for s in state.stations.values()],
        "vehicles": [
            json.dumps({k: x for k, x in dataclasses.asdict(v).items() if k not in drop})
            for v in state.vehicles.values()
        ],
    }


def load(rows: dict[str, list[str]], rng: random.Random) -> SiteState:
    """Загрузка из строк в перемешанном порядке."""
    stations = [json.loads(r) for r in rows["stations"]]
    vehicles = [json.loads(r) for r in rows["vehicles"]]
    rng.shuffle(stations)
    rng.shuffle(vehicles)
    (processor,) = [json.loads(r) for r in rows["processor"]]
    return SiteState(
        now=processor["now_ts"],
        vehicles={v["unit_uuid"]: Vehicle(**v) for v in vehicles},
        stations={s["station_uuid"]: Station(**s) for s in stations},
    )


def run_with_restarts(cuts: list[int], seed: int, drop: tuple[str, ...] = ()) -> list[Record]:
    """Прогон потока; перед каждым индексом из cuts — чекпоинт и рестарт из строк."""
    rng = random.Random(seed)
    e = Engine(CFG)
    got: list[Record] = []
    cut_set = set(cuts)
    for i, raw in enumerate(stream()):
        if i in cut_set:
            e = Engine(CFG, load(save(e.state, drop), rng))
        got.append(record(e.apply(raw)))
    return got


def test_stream_covers_the_rules():
    seen = Counter()
    for outs, events in baseline():
        for kind, attrs in events:
            a = dict(attrs)
            seen[(kind, a.get("how") or a.get("reason") or a.get("result") or "")] += 1
        for topic, _, _ in outs:
            seen[(topic, "")] += 1
    for key in [
        ("invalid", ""),
        ("not_newer", ""),
        ("fsm_resync", ""),
        ("prediction_error", ""),
        ("occupied", "arrival"),
        ("occupied", "handover"),
        ("released", "timer"),
        ("released", "exit"),
        ("decision", "recommended"),
        ("decision", "no_gain"),
        ("decision", "stale_telemetry"),
        ("decision.v1", ""),
    ]:
        assert seen[key] > 0, key
    assert {kind for kind, _ in seen if "." not in kind} == set(get_args(EventKind))  # словарь событий §3.10 весь


def test_same_stream_gives_same_records():
    e = Engine(CFG)
    assert tuple(record(e.apply(raw)) for raw in stream()) == baseline()


@pytest.mark.parametrize("batch", [1, 7, 1_000])
def test_batches_with_checkpoint_and_restart_give_identical_records(batch):
    got = run_with_restarts(list(range(batch, len(stream()), batch)), seed=batch)
    assert got == list(baseline())


@settings(max_examples=10, deadline=None)
@given(
    st.lists(st.integers(1, len(stream()) - 1), min_size=1, max_size=4, unique=True),
    st.integers(0, 2**32 - 1),
)
def test_restart_from_rows_at_random_points_gives_identical_records(cuts, seed):
    assert run_with_restarts(sorted(cuts), seed) == list(baseline())


def test_restart_between_radius_entry_and_self_heal_keeps_entry():
    """Чекпоинт, пока машина в радиусе нецелевой станции (вошла, ещё не встала): radius_station и radius_entered_at
    переживают рестарт, и самолечение считает приезд от входа в радиус, как в непрерывном прогоне (§3.5)."""
    windows = []  # индексы сообщений, перед которыми какая-то машина «к станции» в радиусе не своей цели
    e = Engine(CFG)
    for i, raw in enumerate(stream()):
        if any(v.radius_station not in (None, v.target_station) for v in e.state.vehicles.values()):
            windows.append(i)
        e.apply(raw)
    assert len(windows) >= 5
    assert {"radius_station", "radius_entered_at"} <= json.loads(save(e.state)["vehicles"][0]).keys()
    assert run_with_restarts(windows, seed=11) == list(baseline())
    # контроль: без этих колонок вход в радиус после рестарта — следующее сообщение, приезд ждущей сдвигается
    assert run_with_restarts(windows, seed=11, drop=("radius_station", "radius_entered_at")) != list(baseline())


def test_state_copy_is_independent_and_continues_identically():
    cut = len(stream()) // 2
    e = Engine(CFG)
    for raw in stream()[:cut]:
        e.apply(raw)
    snap = copy.deepcopy(e.state)
    assert snap == e.state and snap is not e.state
    tail_original = [record(e.apply(raw)) for raw in stream()[cut:]]
    assert snap != e.state  # оригинал ушёл вперёд, снимок не тронут
    resumed = Engine(CFG, snap)
    assert [record(resumed.apply(raw)) for raw in stream()[cut:]] == tail_original == list(baseline()[cut:])


def test_replay_after_crash_reproduces_tail_byte_for_byte():
    checkpoint_at, crash_at = 2_000, 2_450
    e = Engine(CFG)
    for raw in stream()[:checkpoint_at]:
        e.apply(raw)
    rows = save(e.state)
    first = [record(e.apply(raw)) for raw in stream()[checkpoint_at:crash_at]]  # выдано до падения
    assert any(outs for outs, _ in first)
    restarted = Engine(CFG, load(rows, random.Random(1)))
    replay = [record(restarted.apply(raw)) for raw in stream()[checkpoint_at:crash_at]]
    assert replay == first


def test_rows_round_trip_is_exact():
    e = Engine(CFG)
    for raw in stream()[:2_500]:
        e.apply(raw)
    restored = load(save(e.state), random.Random(3))
    assert restored == e.state
    for unit, v in e.state.vehicles.items():
        r = restored.vehicles[unit]
        assert (r.lat.hex(), r.lon.hex(), r.speed_kmh.hex()) == (v.lat.hex(), v.lon.hex(), v.speed_kmh.hex())
    assert [s.last_published_queue for s in restored.stations.values()] != [None] * len(restored.stations)


SRC = Path(__file__).resolve().parent.parent / "src" / "vq"
# Чистые модули (§2.7, §3, §4.2) и что им можно импортировать: ни часов, ни I/O, ни сторонних зависимостей (брокера
# и БД в том числе). Ядру случайность запрещена вовсе; симулятору, инжектору сбоев и замкнутому контуру (из него
# vq-bench строит поток с воспроизводимым хэшем) — только экземпляры random.Random(seed)
CORE_IMPORTS = {"collections", "dataclasses", "datetime", "json", "math", "typing", "vq.config", "vq.core", "vq.geo"}
SIM_IMPORTS = CORE_IMPORTS | {"bisect", "heapq", "random", "vq.generator.sim", "vq.generator.faults"}
PURE = {
    **{p: CORE_IMPORTS for p in sorted((SRC / "core").glob("*.py"))},
    SRC / "geo.py": CORE_IMPORTS,
    **{SRC / "generator" / f: SIM_IMPORTS for f in ("sim.py", "faults.py", "closed_loop.py")},
}
FORBIDDEN_CALLS = {"now", "today", "utcnow", "time", "time_ns", "monotonic", "perf_counter", "sleep", "getenv", "open"}


def impurities(source: str, allowed: set[str]) -> list[str]:
    """Нарушения чистоты в исходнике: импорт вне allowed (модуль или его подмодуль), вызов часов или I/O по имени,
    random — только random.Random(...) без псевдонима (глобальный генератор и seed() — это недетерминизм)."""
    found = []

    def check_import(name: str, line: int) -> None:
        if not any(name == a or name.startswith(a + ".") for a in allowed):
            found.append(f"{line}: import {name}")

    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.Import):
            for a in node.names:
                check_import(a.name, node.lineno)
                if a.name == "random" and a.asname is not None:
                    found.append(f"{node.lineno}: import random as {a.asname}")
        elif isinstance(node, ast.ImportFrom):
            check_import(node.module or "", node.lineno)
            if node.module == "random" and [a.name for a in node.names] != ["Random"]:
                found.append(f"{node.lineno}: from random import {', '.join(a.name for a in node.names)}")
        elif isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "random":
            if node.attr != "Random":
                found.append(f"{node.lineno}: random.{node.attr}")
        elif isinstance(node, ast.Call):
            f = node.func
            name = f.attr if isinstance(f, ast.Attribute) else f.id if isinstance(f, ast.Name) else ""
            if name in FORBIDDEN_CALLS:
                found.append(f"{node.lineno}: {name}()")
    return found


@pytest.mark.parametrize("path", PURE, ids=lambda p: f"{p.parent.name}/{p.name}")
def test_pure_modules_have_no_clock_io_or_free_randomness(path):
    assert impurities(path.read_text(), PURE[path]) == []


@pytest.mark.parametrize(
    "source",
    [
        "import time\ntime.time()",
        "from time import monotonic",
        "from datetime import datetime\ndatetime.now()",
        "import random\nrandom.random()",
        "import random\nrandom.seed(1)",
        "import random\nf = random.randint",
        "from random import randint",
        "import random as r",
        "import os",
        "from vq.dispatcher import store",
        "open('site.toml')",
    ],
)
def test_purity_check_catches_clock_io_and_free_randomness(source):
    assert impurities(source, SIM_IMPORTS) != []


def test_purity_check_allows_seeded_random_instance_outside_core_only():
    source = "import random\nfrom random import Random\nrng = random.Random(f'faults:{42}')\nrng.randint(2, 60)"
    assert impurities(source, SIM_IMPORTS) == []
    assert impurities(source, CORE_IMPORTS) != []  # ядру случайность запрещена вовсе
