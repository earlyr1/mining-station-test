"""Замкнутый контур (§6.1): симулятор и ядро в памяти, решения возвращаются в симулятор.

Прогоны по 2 часа симуляции с задержкой решений 0, 6 и 200 с и сбоями по умолчанию, плюс прогон самолечения HEAL:
первый час решения опаздывают на 200 с, потом генератор снова послушен — проверяется, что модель сходится.
Прогоны независимы и считаются параллельно в отдельных процессах (~290 тыс. сообщений за 2 часа).
"""

import multiprocessing
import os
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from pathlib import Path

import pytest

from tests.closed_loop_checks import (
    INSERTED_BY_LATER_REDIRECT,
    INSERTED_WHILE_STALE,
    OVERTAKEN,
    REDIRECTED_AWAY,
    check_predictions,
    trip_decisions,
)
from tests.helpers import summary_line
from vq.config import load_config
from vq.generator.closed_loop import FaultRates, LoopResult, Prediction, run

SITE = Path(__file__).resolve().parent.parent / "config" / "site.toml"
SEED = 42  # порог доли точных прогнозов (ACCURACY_FLOOR) — для этого seed; остальные проверки от seed не зависят
START = 1_789_473_600  # 2026-09-15T12:00:00Z
TICKS = 2 * 3600
OBEDIENT = (0, 6)  # решение успевает до начала обслуживания
LATE = 200  # решение опаздывает: машина уже у своей станции, модель сходится самолечением
HEAL = "heal"  # LATE первый час, затем без задержки
HEAL_OBEDIENT_FROM = START + 3600
HEAL_TICKS = 9000
LAST_LATE_DELIVERY = HEAL_OBEDIENT_FROM + LATE  # решение тика HEAL_OBEDIENT_FROM − 1 приходит перед этим тиком
REDIRECT_MAX_S = 600  # самый длинный переезд от своей станции к чужой: S1↔S4 ≈ 5176 м — 518 с, плюс тик остановки
DUMP_LEG_S = 1000  # плечо станция ↔ разгрузка: 10 км на 10 м/с
CONVERGED_WINDOW_S = 1800  # последние 30 минут HEAL — через 3400 с после последнего опоздавшего решения
ACCURACY_FLOOR = {0: 0.85, 6: 0.80}  # доля |ошибка| ≤ 5 с по всем занятиям — только порог регрессии (seed 42)

SCENARIOS: dict[int | str, tuple[int, int | None, int]] = {  # задержка, obedient_from, тиков
    0: (0, None, TICKS),
    6: (6, None, TICKS),
    LATE: (LATE, None, TICKS),
    HEAL: (LATE, HEAL_OBEDIENT_FROM, HEAL_TICKS),
}
ALL = tuple(SCENARIOS)


@pytest.fixture(scope="module")
def runs() -> dict[int | str, LoopResult]:
    cfg = load_config(SITE)
    ctx = multiprocessing.get_context("spawn")
    workers = min(len(SCENARIOS), os.cpu_count() or 1)
    with ProcessPoolExecutor(max_workers=workers, mp_context=ctx) as pool:
        futures = {
            name: pool.submit(
                run,
                cfg,
                SEED,
                START,
                ticks,
                decision_delay=delay,
                fault_rates=FaultRates(),
                check_each_tick=True,
                obedient_from=obedient_from,
                record_streams=False,
            )
            for name, (delay, obedient_from, ticks) in SCENARIOS.items()
        }
        return {name: f.result() for name, f in futures.items()}


def delivered(res: LoopResult) -> list[Prediction]:
    """Решения, которые успели вернуться в симулятор до конца прогона."""
    return [p for p in res.predictions if p.deliver_tick is not None and p.deliver_tick < res.end_tick]


# --- инварианты всех прогонов -------------------------------------------------


@pytest.mark.parametrize("name", ALL)
def test_sim_and_kernel_agree_on_occupant_every_tick(runs, name):
    """Занявшая каждой станции у симулятора и ядра совпадает на каждом тике — и при опоздавших решениях: самолечение
    считает приезд от входа в радиус (тот же ключ FIFO, что у симулятора), а заход «к станции» в радиус разгрузки
    заканчивает заезд (§3.5)."""
    res = runs[name]
    assert res.occupant_mismatches == []
    assert set(res.releases) == {"timer"}  # таймер и выезд из радиуса — в одну секунду: выезд не освобождает


@pytest.mark.parametrize("name", ALL)
def test_no_double_occupation_and_at_most_one_row_per_vehicle(runs, name):
    res = runs[name]
    assert res.double_occupations == 0
    assert res.row_violations == []


@pytest.mark.parametrize("name", ALL)
def test_every_vehicle_completes_a_cycle(runs, name):
    cycles = runs[name].cycles
    assert len(cycles) == 40 and min(cycles.values()) >= 1, cycles


@pytest.mark.parametrize("name", ALL)
def test_both_recommended_and_no_gain(runs, name):
    decisions = runs[name].decisions
    assert decisions[("recommended", "")] > 0 and decisions[("rejected", "no_gain")] > 0, decisions


@pytest.mark.parametrize("name", ALL)
def test_kernel_drops_exactly_what_generator_expects(runs, name):
    res = runs[name]
    assert res.injector is not None
    assert res.injector.faults["dup"] > 0 and res.injector.faults["late"] > 0 and res.injector.faults["outage"] > 0
    assert res.not_newer == res.injector.expected_drops > 0


@pytest.mark.parametrize("name", ALL)
def test_one_decision_per_trip(runs, name):
    """ТЗ: рекомендация считается один раз за заезд. Заезд — от разгрузки (или старта) до следующей разгрузки."""
    trips = trip_decisions(runs[name])
    assert len(trips) > 80
    wrong = [(t.unit, t.start - START, t.end - START, t.decisions) for t in trips if t.decisions != 1]
    assert wrong == []


# --- послушный генератор: 0 и 6 с ---------------------------------------------


@pytest.mark.parametrize("delay", OBEDIENT)
def test_decisions_all_executed(runs, delay):
    res = runs[delay]
    stats = res.sim.stats
    sent = delivered(res)
    assert stats.ignored == Counter()
    assert stats.received == len(sent)
    assert stats.applied == sum(1 for p in sent if p.result == "recommended")
    # задержка поворота от входа в зону решения (§4.2 п. 4): решение тика t исполняется на тике t + 1 + delay;
    # больше — если сообщение-триггер опоздало или попало в обрыв (до 60 с)
    assert len(stats.delays) == stats.applied
    assert min(stats.delays) == delay + 1 and max(stats.delays) <= delay + 1 + 60
    assert Counter(stats.delays).most_common(1)[0][0] == delay + 1


@pytest.mark.parametrize("delay", OBEDIENT)
def test_no_self_healing_with_obedient_generator(runs, delay):
    assert runs[delay].resyncs == 0


LEGIT = {INSERTED_BY_LATER_REDIRECT, INSERTED_WHILE_STALE, REDIRECTED_AWAY, OVERTAKEN}


@pytest.mark.parametrize("delay", OBEDIENT)
def test_prediction_accuracy(runs, delay, pytestconfig):
    """Точность прогноза (§6.1): прогноз точен, пока у станции те же предшественники, что в момент решения.

    1. Спокойное занятие: 0 ≤ ошибка ≤ 2 + 2·L. 1–2 с — округление ETA и тик остановки; L — на сколько тиков позже
       позиции-триггера повернула эта машина или ещё не повернувшая предшественница (задержка решения, триггер
       в обрыве).
    2. Возмущённое занятие — только по причине из LEGIT, то есть из-за сведений, которых при решении не было: позже
       перенаправленная сюда машина приезжает раньше (дальние рекомендации едут 350–500 с), предшественницу позже
       уводят, несвежая на момент решения машина (обрыв) появляется впереди, стоявшая позади въезжает в радиус раньше
       (ETA отстаёт не больше чем на 1 + 2·L с). Без причины из списка — ошибка.
    3. Доля |ошибка| ≤ 5 с по всем занятиям выводится в итог pytest и проверяется только как порог регрессии:
       правило ТЗ считает ожидание одной решающей машины, поэтому «95 % занятий» недостижимо (8 seed: 84–92 %).
    """
    res = runs[delay]
    checks = check_predictions(res)
    calm = [c for c in checks if not c.disturbed]
    disturbed = [c for c in checks if c.disturbed]
    assert len(checks) > 100
    off = [
        (c.occupation.tick - START, c.occupation.unit, c.occupation.error, c.turn_lag)
        for c in calm
        if not 0 <= (c.occupation.error or 0) <= 2 + 2 * c.turn_lag
    ]
    assert off == []
    unexplained = [
        (c.occupation.tick - START, c.occupation.unit, c.causes) for c in disturbed if not set(c.causes) <= LEGIT
    ]
    assert unexplained == []
    errors = res.prediction_errors
    exact = sum(abs(e) <= 5 for e in errors)
    calm_exact = sum(abs(c.occupation.error or 0) <= 5 for c in calm)
    summary_line(
        pytestconfig,
        f"точность прогноза, задержка {delay} с: |ошибка| ≤ 5 с у {exact}/{len(errors)} = {exact / len(errors):.3f} "
        f"всех занятий (порог {ACCURACY_FLOOR[delay]}), у {calm_exact}/{len(calm)} спокойных; "
        f"возмущённых {len(disturbed)}",
    )
    assert exact >= ACCURACY_FLOOR[delay] * len(errors)


# --- самолечение: 200 с ---------------------------------------------------------


@pytest.mark.parametrize("name", (LATE, HEAL))
def test_late_decisions_trigger_self_healing(runs, name):
    """Задержка 200 с: рекомендация приходит, когда машина уже у своей станции; ядро сходится самолечением.

    Совпадение занявших по тикам для этих прогонов — в test_sim_and_kernel_agree_on_occupant_every_tick.
    """
    res = runs[name]
    stats = res.sim.stats
    sent = delivered(res)
    assert res.resyncs > 0
    assert stats.applied > 0  # часть рекомендаций застаёт машину ещё в очереди своей станции
    assert set(stats.ignored) <= {"already_serving"}
    assert stats.received == len(sent)
    assert stats.applied + stats.ignored["already_serving"] == sum(p.result == "recommended" for p in sent)
    assert max(stats.delays) >= LATE + 1
    if name == LATE:
        assert min(stats.delays) >= LATE + 1


def test_heal_scenario_switches_to_obedient(runs):
    """HEAL: решения до HEAL_OBEDIENT_FROM опаздывают на LATE, после — приходят на следующем тике."""
    res = runs[HEAL]
    late = [p for p in res.predictions if p.tick < HEAL_OBEDIENT_FROM]
    obedient = [p for p in res.predictions if HEAL_OBEDIENT_FROM <= p.tick < res.end_tick]
    assert late and all(p.deliver_tick == p.tick + 1 + LATE for p in late)
    assert obedient and all(p.deliver_tick == p.tick + 1 for p in obedient)
    assert 1 in res.sim.stats.delays
    # окно проверки сходимости — заведомо позже переезда к рекомендованной станции и цикла через разгрузку
    assert res.end_tick - CONVERGED_WINDOW_S >= LAST_LATE_DELIVERY + REDIRECT_MAX_S + 2 * DUMP_LEG_S


def test_heal_no_self_healing_after_late_decisions_are_done(runs):
    """Самолечение — только у машин, поздно повернувших к рекомендованной станции: не позже переезда к ней."""
    late = [t - START for t in runs[HEAL].resync_ticks if t > LAST_LATE_DELIVERY + REDIRECT_MAX_S]
    assert late == []


def test_heal_model_converges_after_late_decisions_stop(runs):
    """Через 3400 с после последнего опоздавшего решения занявшие станций у ядра и симулятора снова совпадают."""
    res = runs[HEAL]
    window = res.end_tick - CONVERGED_WINDOW_S
    tail = [(m.tick - START, m.station, m.sim, m.kernel) for m in res.occupant_mismatches if m.tick >= window]
    assert tail == []


# --- записанные потоки (vq-bench, офлайн-перепрогон) ----------------------------


def test_recorded_streams_replay_identically_offline():
    """§2.7: телеметрия замкнутого контура, прогнанная через новое ядро офлайн, даёт те же выходы байт в байт."""
    from vq.core.engine import Engine

    cfg = load_config(SITE)
    ticks = 600  # первые решения — в первую минуту: старт в 1603 + 10·k м от станции
    res = run(cfg, 7, START, ticks, 0, FaultRates(dup=0.02, late=0.01, outage=0.002), check_each_tick=False)
    assert len(res.telemetry) == res.messages > ticks * 40
    assert any(o.topic == "decision.v1" for o in res.outputs)
    engine = Engine(cfg)
    replay = [(o.topic, o.key, o.value, i) for i, raw in enumerate(res.telemetry) for o in engine.apply(raw).outputs]
    assert replay == [tuple(o) for o in res.outputs]
