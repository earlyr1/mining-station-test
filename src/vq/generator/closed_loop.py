"""Замкнутый контур в памяти (§6.1): симулятор → сбои → ядро → решения обратно в симулятор.

Решения, выданные при обработке тика t, попадают в симулятор перед тиком t + 1 + decision_delay
(с тика obedient_from — перед тиком t + 1: генератор снова послушен, так проверяется, что модель сходится).
Нужен тесту замкнутого контура и vq-bench (офлайн-телеметрия с обратной связью); критерии приёмки по
LoopResult — в tests/closed_loop_checks.py.
"""

import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field, replace
from typing import NamedTuple

from vq.config import SiteConfig
from vq.core.codec import encode_telemetry
from vq.core.decision import DECISION_TOPIC
from vq.core.engine import QUEUE_TOPIC, Engine
from vq.core.model import StepResult
from vq.core.queue import station_rows
from vq.generator.faults import FaultInjector
from vq.generator.sim import Sim


@dataclass(frozen=True)
class FaultRates:
    dup: float = 0.005
    late: float = 0.002
    outage: float = 0.0005


class LoopOutput(NamedTuple):
    topic: str
    key: bytes
    value: bytes
    src: int  # индекс породившего сообщения в telemetry — аналог vq-src-offset


class Mismatch(NamedTuple):
    tick: int
    station: str
    sim: str | None  # занявшая в симуляторе
    kernel: str | None  # занявшая в ядре


@dataclass(frozen=True)
class Occupation:
    """Занятие станции по ядру; error — prediction_error, если у машины был прогноз."""

    tick: int
    unit: str
    station: str
    how: str  # arrival | handover
    error: int | None = None


@dataclass(frozen=True)
class Prediction:
    """Решение ядра и снимок станции, куда машина поедет, сразу после решения."""

    tick: int
    unit: str
    result: str  # recommended | rejected
    reason: str  # "" | no_gain | stale_telemetry
    station: str  # целевая станция по модели ядра
    predicted_start: int | None
    queue: tuple[str, ...] = ()  # очередь станции по порядку, вместе с этой машиной
    etas: tuple[int | None, ...] = ()  # eta строк очереди в том же порядке; None — занявшая и ждущие в радиусе
    occupant: str | None = None  # занявшая станцию в момент решения
    stale: tuple[str, ...] = ()  # несвежие машины: их нет ни в одной очереди
    trigger_ts: int = 0  # ts позиции, по которой считались ETA (сообщение-триггер)
    deliver_tick: int | None = None  # тик, перед которым решение попадает в симулятор; None — не доставляется

    @property
    def turn_lag(self) -> int:
        """На сколько тиков позже позиции-триггера машина поворачивает к рекомендованной станции; 0 — поворота нет."""
        if self.result != "recommended" or self.deliver_tick is None:
            return 0
        return max(0, self.deliver_tick - 1 - self.trigger_ts)


@dataclass
class LoopResult:
    engine: Engine
    sim: Sim
    injector: FaultInjector | None
    telemetry: list[bytes] = field(default_factory=list)  # в порядке отправки (если record_streams)
    outputs: list[LoopOutput] = field(default_factory=list)  # выходы ядра (если record_streams)
    messages: int = 0  # сообщений отправлено в ядро
    occupant_mismatches: list[Mismatch] = field(default_factory=list)
    double_occupations: int = 0
    row_violations: list[tuple[int, str]] = field(default_factory=list)  # (индекс сообщения, unit_uuid)
    occupations: list[Occupation] = field(default_factory=list)
    predictions: list[Prediction] = field(default_factory=list)
    decisions: Counter[tuple[str, str]] = field(default_factory=Counter)  # (result, reason)
    releases: Counter[str] = field(default_factory=Counter)  # timer | exit
    resync_ticks: list[int] = field(default_factory=list)  # тики симуляции, на которых ядро сработало самолечением
    not_newer: int = 0
    trips: dict[str, list[int]] = field(default_factory=dict)  # начала заездов машины в симуляторе: старт, разгрузки
    end_tick: int = 0  # первый тик после прогона

    @property
    def prediction_errors(self) -> list[int]:
        return [o.error for o in self.occupations if o.error is not None]

    @property
    def cycles(self) -> dict[str, int]:
        return {u: v.cycles for u, v in self.sim.vehicles.items()}

    @property
    def resyncs(self) -> int:
        return len(self.resync_ticks)


class _Loop:
    def __init__(
        self,
        res: LoopResult,
        decision_delay: int,
        check: bool,
        obedient_from: int | None = None,
        record_streams: bool = True,
    ):
        self.res = res
        self.delay = decision_delay
        self.obedient_from = obedient_from
        self.check = check
        self.record = record_streams
        self.due: defaultdict[int, list[bytes]] = defaultdict(list)
        self.current_rows: dict[str, list[str]] = {}  # станция → машины последней опубликованной очереди

    def deliver_tick(self, tick: int) -> int:
        late = self.obedient_from is None or tick < self.obedient_from
        return tick + 1 + (self.delay if late else 0)

    def apply(self, raw: bytes, tick: int, deliver: bool) -> None:
        res = self.res
        src = res.messages
        res.messages += 1
        if self.record:
            res.telemetry.append(raw)
        step = res.engine.apply(raw)
        queue_changed = False
        for o in step.outputs:
            if self.record:
                res.outputs.append(LoopOutput(o.topic, o.key, o.value, src))
            if o.topic == DECISION_TOPIC:
                when = self.deliver_tick(tick) if deliver else None
                if when is not None:
                    self.due[when].append(o.value)
                self._record_prediction(o.value, tick, when)
            elif o.topic == QUEUE_TOPIC and self.check:
                q = json.loads(o.value)
                self.current_rows[q["station_uuid"]] = [r["unit_uuid"] for r in q["queue"]]
                queue_changed = True
        self._events(step, tick)
        if self.check:
            if queue_changed:  # проверка после всего шага: внутри шага очереди станций публикуются по одной
                seen: Counter[str] = Counter(u for units in self.current_rows.values() for u in units)
                res.row_violations += [(src, u) for u, n in sorted(seen.items()) if n > 1]
            self._check_occupation()

    def _events(self, step: StepResult, tick: int) -> None:
        res = self.res
        for ev in step.events:
            kind = ev.kind
            if kind == "not_newer":
                res.not_newer += 1
            elif kind == "fsm_resync":
                res.resync_ticks.append(tick)
            elif kind == "decision":
                res.decisions[(str(ev.get("result")), str(ev.get("reason")))] += 1
            elif kind == "released":
                res.releases[str(ev.get("reason"))] += 1
            elif kind == "occupied":
                unit, station, how = str(ev.get("unit")), str(ev.get("station")), str(ev.get("how"))
                res.occupations.append(Occupation(tick, unit, station, how))
            elif kind == "prediction_error":
                seconds = ev.get("seconds")
                assert isinstance(seconds, int)
                res.occupations[-1] = replace(res.occupations[-1], error=seconds)  # идёт сразу за своим occupied

    def _record_prediction(self, value: bytes, tick: int, deliver_tick: int | None) -> None:
        d = json.loads(value)
        engine = self.res.engine
        state = engine.state
        now = state.now
        assert now is not None
        v = state.vehicles[d["unit_uuid"]]  # решение принято по сообщению, которое ядро только что применило
        fresh_s = engine.cfg.constants.fresh_s
        rows = station_rows(state, engine.cfg, v.target_station)
        self.res.predictions.append(
            Prediction(
                tick,
                v.unit_uuid,
                d["result"],
                d.get("reason", ""),
                v.target_station,
                v.predicted_start,
                queue=tuple(r.unit_uuid for r in rows),
                etas=tuple(r.eta for r in rows),
                occupant=state.stations[v.target_station].occupant_uuid,
                stale=tuple(u for u, x in state.vehicles.items() if now - x.last_ts > fresh_s),
                trigger_ts=v.last_ts,
                deliver_tick=deliver_tick,
            )
        )

    def _check_occupation(self) -> None:
        """Двойное занятие в ядре: машина занимает две станции или занятие раньше прошлого освобождения."""
        stations = self.res.engine.state.stations.values()
        occ = [st.occupant_uuid for st in stations if st.occupant_uuid is not None]
        bad = len(occ) != len(set(occ))
        for st in stations:
            if st.occupant_uuid is not None and st.last_release_at is not None:
                assert st.occupied_at is not None
                bad = bad or st.occupied_at < st.last_release_at
        if bad:
            self.res.double_occupations += 1

    def compare_occupants(self, tick: int) -> None:
        res = self.res
        sim_occ = res.sim.occupants()
        busy = [u for u in sim_occ.values() if u is not None]
        if len(busy) != len(set(busy)):
            res.double_occupations += 1
        for uuid, st in res.engine.state.stations.items():
            if sim_occ[uuid] != st.occupant_uuid:
                res.occupant_mismatches.append(Mismatch(tick, uuid, sim_occ[uuid], st.occupant_uuid))

    def note_trips(self) -> None:
        """Новый заезд машины в симуляторе начинается с разгрузки (§4.3: trip_started_at)."""
        for unit, v in self.res.sim.vehicles.items():
            starts = self.res.trips.setdefault(unit, [])
            if not starts or starts[-1] != v.trip_started_at:
                starts.append(v.trip_started_at)


def run(
    cfg: SiteConfig,
    seed: int,
    start_ts: int,
    ticks: int,
    decision_delay: int = 0,
    fault_rates: FaultRates | None = None,
    check_each_tick: bool = True,
    obedient_from: int | None = None,
    record_streams: bool = True,
) -> LoopResult:
    """ticks секунд симуляции; по окончании досылается отложенное инжектором (решения из досылки не доставляются).

    obedient_from — тик, с которого решения доставляются без задержки decision_delay (None — задержка всё время).
    record_streams=False не хранит телеметрию и выходы: проверкам они не нужны, а это сотни МБ на прогон.
    """
    sim = Sim(cfg, seed, start_ts)
    injector = None
    if fault_rates is not None:
        injector = FaultInjector(cfg, seed, fault_rates.dup, fault_rates.late, fault_rates.outage)
    loop = _Loop(LoopResult(Engine(cfg), sim, injector), decision_delay, check_each_tick, obedient_from, record_streams)
    loop.note_trips()
    for _ in range(ticks):
        t = sim.t
        for raw in loop.due.pop(t, ()):
            sim.offer_decision(raw)
        msgs = sim.tick()
        loop.note_trips()
        for m in injector.push(t, msgs) if injector is not None else msgs:
            loop.apply(encode_telemetry(m), t, deliver=True)
        if check_each_tick:
            loop.compare_occupants(t)
    loop.res.end_tick = sim.t
    if injector is not None:
        for m in injector.drain():
            loop.apply(encode_telemetry(m), sim.t, deliver=False)
    return loop.res
