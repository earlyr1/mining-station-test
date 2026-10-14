"""Оракулы теста замкнутого контура (§6.1, решение лида 3): один расчёт на заезд и разбор ошибок прогноза.

Работают по LoopResult из vq.generator.closed_loop; в образ приложения не входят — это критерии приёмки, а не код
сервиса.
"""

from collections import defaultdict
from dataclasses import dataclass
from itertools import pairwise
from typing import NamedTuple

from vq.generator.closed_loop import LoopResult, Occupation, Prediction

# --- один расчёт на заезд -----------------------------------------------------


class Trip(NamedTuple):
    unit: str
    start: int  # тик старта или разгрузки
    end: int  # тик следующей разгрузки
    decisions: int  # решений ядра (decision.v1) по машине за заезд


def trip_decisions(res: LoopResult) -> list[Trip]:
    """Завершённые в прогоне заезды симулятора и число решений ядра в каждом: по ТЗ — ровно одно."""
    ticks: defaultdict[str, list[int]] = defaultdict(list)
    for p in res.predictions:
        ticks[p.unit].append(p.tick)
    return [
        Trip(unit, a, b, sum(a <= x < b for x in ticks[unit]))
        for unit, starts in sorted(res.trips.items())
        for a, b in pairwise(starts)
    ]


# --- разбор ошибок прогноза ---------------------------------------------------

# Причины, по которым фактические предшественники машины на станции разошлись с прогнозом. Все — следствия
# решений и сведений, которых в момент прогноза ещё не было; «unexplained» — повод искать ошибку.
INSERTED_BY_LATER_REDIRECT = "inserted_by_later_redirect"  # перенаправлена сюда позже и приехала раньше
INSERTED_WHILE_STALE = "inserted_while_stale"  # была несвежей (обрыв) и потому не стояла в очереди
# стояла в очереди позади, а въехала в радиус раньше: ETA отстаёт от нашего не больше чем на 1 + 2·L с
# (въезд в радиус — за 4–5 тиков до точки станции в зависимости от дробной части пути; L — запаздывание поворота)
OVERTAKEN = "overtaken"
REDIRECTED_AWAY = "redirected_away"  # предшественница позже перенаправлена на другую станцию
STATION_CHANGED = "station_changed"  # заняла не ту станцию, что в прогнозе (самолечение)
REORDERED = "reordered"  # те же предшественники в другом порядке
UNEXPLAINED = "unexplained"


@dataclass(frozen=True)
class PredictionCheck:
    occupation: Occupation
    prediction: Prediction
    expected: tuple[str, ...]  # предшественники по прогнозу, кроме занявшей в момент решения
    actual: tuple[str, ...]  # фактически занимавшие станцию между решением и занятием
    causes: tuple[str, ...]  # пусто, если expected == actual
    turn_lag: int = 0  # наибольшее запаздывание поворота у машины и её ожидаемых предшественниц, тиков

    @property
    def disturbed(self) -> bool:
        return bool(self.causes)


def _redirected(decisions: list[tuple[int, Prediction]], after: int, o: Occupation, to_here: bool) -> bool:
    """Была ли рекомендация после решения №after и до занятия o — на станцию o (to_here) или с неё."""
    return any(
        i > after and x.tick <= o.tick and x.result == "recommended" and (x.station == o.station) == to_here
        for i, x in decisions
    )


def _overtake_explained(p: Prediction, me: int, unit: str) -> bool:
    """Обгон стоявшей позади объясним округлением ETA до секунды и запаздыванием поворота этой машины."""
    mine, theirs = p.etas[me], p.etas[p.queue.index(unit)]
    return mine is not None and theirs is not None and theirs - mine <= 1 + 2 * p.turn_lag


def _pending_turn_lag(decisions: list[tuple[int, Prediction]], station: str, tick: int, fresh_s: int) -> int:
    """Запаздывание поворота предшественницы, ещё не повернувшей к станции на момент решения tick.

    Её ETA в очереди считался от последней позиции (не старше fresh_s); рекомендация, доставленная раньше
    tick − fresh_s, к тому моменту уже исполнена, и предшественница ехала к станции прямо.
    """
    return max(
        (
            x.turn_lag
            for _, x in decisions
            if x.result == "recommended"
            and x.station == station
            and x.tick <= tick
            and x.deliver_tick is not None
            and x.deliver_tick >= tick - fresh_s
        ),
        default=0,
    )


def check_predictions(res: LoopResult) -> list[PredictionCheck]:
    """Для каждого занятия с прогнозом — совпали ли фактические предшественники с прогнозом и почему нет.

    turn_lag ограничивает ошибку спокойного прогноза: 0 ≤ ошибка ≤ 2 + 2·turn_lag. 1–2 с — округление ETA
    и тик остановки в точке станции; машина, повернувшая на L тиков позже позиции, по которой считался ETA,
    теряет не больше 2·L с, и это запаздывание передаётся по цепочке тем, кто ждёт за ней.
    """
    fresh_s = res.engine.cfg.constants.fresh_s
    by_station: defaultdict[str, list[Occupation]] = defaultdict(list)
    for o in res.occupations:
        by_station[o.station].append(o)
    preds: defaultdict[str, list[tuple[int, Prediction]]] = defaultdict(list)  # по порядку решений в логе
    for i, p in enumerate(res.predictions):
        preds[p.unit].append((i, p))
    out: list[PredictionCheck] = []
    for o in res.occupations:
        if o.error is None:
            continue
        n, p = [(i, x) for i, x in preds[o.unit] if x.tick <= o.tick][-1]
        if p.station != o.station:
            out.append(PredictionCheck(o, p, (), (), (STATION_CHANGED,)))
            continue
        if o.unit not in p.queue:  # прогноз без строки в очереди — так быть не должно
            out.append(PredictionCheck(o, p, (), (), (UNEXPLAINED,)))
            continue
        me = p.queue.index(o.unit)
        expected = tuple(u for u in p.queue[:me] if u != p.occupant)
        actual = tuple(x.unit for x in by_station[o.station] if p.tick <= x.tick < o.tick and x.unit != p.occupant)
        causes: list[str] = []
        for u in actual:
            if u in expected:
                continue
            if _redirected(preds[u], n, o, to_here=True):
                causes.append(INSERTED_BY_LATER_REDIRECT)
            elif u in p.stale:
                causes.append(INSERTED_WHILE_STALE)
            elif u in p.queue[me + 1 :]:
                causes.append(OVERTAKEN if _overtake_explained(p, me, u) else UNEXPLAINED)
            else:
                causes.append(UNEXPLAINED)
        for u in expected:
            if u not in actual:
                causes.append(REDIRECTED_AWAY if _redirected(preds[u], n, o, to_here=False) else UNEXPLAINED)
        if not causes and expected != actual:
            causes.append(REORDERED)
        lag = max([p.turn_lag, *(_pending_turn_lag(preds[u], o.station, p.tick, fresh_s) for u in expected)])
        out.append(PredictionCheck(o, p, expected, actual, tuple(causes), lag))
    return out
