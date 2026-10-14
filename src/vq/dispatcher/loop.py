"""Оболочка процессора вокруг ядра (§2.4): пачка → выходы → flush → чекпоинт. Без Kafka: тестируется подделками."""

import logging
import os
import threading
import time
from collections.abc import Callable, Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import TYPE_CHECKING, Protocol

from vq.config import SiteConfig
from vq.core.decision import DECISION_TOPIC, options
from vq.core.engine import LOGIC_VERSION, Engine
from vq.core.model import Event, SiteState, StepResult
from vq.dispatcher.store import Claim, Counters, StartRefused
from vq.geo import fmt_ts

if TYPE_CHECKING:
    from vq.dispatcher.metrics import DispatcherMetrics

FLUSH_TIMEOUT_S = 10.0
# Срок вызова Postgres: больше statement_timeout (5 с) и tcp_user_timeout (10 с) — штатные ошибки приходят раньше.
PG_DEADLINE_S = 15.0

# Коды выхода процесса (§2.4).
EXIT_OK = 0
EXIT_FAILURE = 1  # сбой: контейнер перезапустится (crash-only)
EXIT_REFUSED = 2  # отказ старта: лечится make reset
CRASH_EXIT_CODE = 137  # точка аварии make e2e-chaos, как у kill -9

log = logging.getLogger("vq.dispatcher")

CrashHook = Callable[[Mapping[str, object]], None]
ExitFn = Callable[[int], object]


@dataclass(frozen=True)
class InMsg:
    offset: int
    value: bytes | None


class Sink(Protocol):
    """Выходной продюсер: send ставит записи заголовки src_headers(src_offset) (§2.3). failed — первая ошибка
    доставки (из delivery callback) или None."""

    failed: str | None

    def send(self, topic: str, key: bytes, value: bytes, src_offset: int) -> None: ...

    def flush(self, timeout: float) -> int: ...


class Store(Protocol):
    def claim(self, site_id: str, logic_version: str, config_hash: str) -> Claim: ...

    def checkpoint(self, site_id: str, epoch: int, next_offset: int, counters: Counters, state: SiteState) -> bool: ...


class Fenced(Exception):
    """Чекпоинт не записан: площадку забрал экземпляр с большим epoch."""


class DeliveryFailed(Exception):
    """Брокер не подтвердил выходы пачки: чекпоинт писать нельзя."""


def _no_crash(ctx: Mapping[str, object]) -> None:
    return None


class Deadline:
    """Сторож вызовов Postgres (§2.4, crash-only): не вернулся за seconds — строка exit_postgres_stalled и выход 1.

    statement_timeout и lock_timeout исполняет сервер, а TCP-сегменты за замёрзший сервер подтверждает ядро ОС:
    без сторожа поток висел бы в запросе, heartbeat librdkafka держал бы партицию, а /metrics отвечал бы.
    Выход — os._exit из потока таймера: состояние в Postgres атомарно, рестарт продолжит с последнего чекпоинта.
    """

    def __init__(self, seconds: float = PG_DEADLINE_S, exit_fn: ExitFn = os._exit) -> None:
        self.seconds = seconds
        self._exit = exit_fn

    @contextmanager
    def guard(self, op: str, fields: Mapping[str, object]) -> Iterator[None]:
        timer = threading.Timer(self.seconds, self._expire, (op, dict(fields)))
        timer.daemon = True
        timer.start()
        try:
            yield
        finally:
            timer.cancel()

    def _expire(self, op: str, fields: dict[str, object]) -> None:
        log.error("exit_postgres_stalled", extra={"fields": {**fields, "op": op, "deadline_s": self.seconds}})
        self._exit(EXIT_FAILURE)


def chaos_hook(path: str | None, exit_fn: ExitFn = os._exit) -> CrashHook:
    """Точка аварии make e2e-chaos (VQ_CRASH_FILE): файл есть — удалить, записать chaos_crash, выйти с кодом 137.

    Processor вызывает её после flush пачки, отправившей хотя бы одну запись, и до чекпоинта: перезапуск
    повторяет эту пачку и детерминированно отправляет те же записи ещё раз (повтор, который считает verify).
    """
    if not path:
        return _no_crash
    crash_file = Path(path)

    def hook(ctx: Mapping[str, object]) -> None:
        try:
            crash_file.unlink()
        except FileNotFoundError:
            return
        log.error("chaos_crash", extra={"fields": {**ctx, "file": str(crash_file)}})
        exit_fn(CRASH_EXIT_CODE)

    return hook


def count_messages(events: Iterable[Event]) -> Counters:
    c = Counters()
    for e in events:
        if e.kind == "accepted":
            c.accepted += 1
        elif e.kind == "not_newer":
            c.not_newer += 1
        elif e.kind == "invalid":
            c.invalid += 1
    return c


def decision_stations(state: SiteState, cfg: SiteConfig, unit_uuid: str) -> dict[str, dict[str, object]]:
    """ETA и ожидание машины у каждой станции — расчёт options() ядра, по которому decide() выбрал станцию (§3.8).

    Вызывать сразу после шага с решением: после решения шаг меняет только эту машину, а options() её исключает.
    """
    v = state.vehicles[unit_uuid]
    return {o.station: {"eta": fmt_ts(o.eta), "wait_s": o.wait} for o in options(state, cfg, v)}


class Processor:
    """Владение партицией и обработка пачек. Один поток; Kafka и Postgres — за протоколами Sink и Store."""

    def __init__(
        self,
        cfg: SiteConfig,
        config_hash: str,
        store: Store,
        sink: Sink,
        metrics: "DispatcherMetrics | None" = None,
        crash_hook: CrashHook | None = None,
        deadline: Deadline | None = None,
    ) -> None:
        self.cfg = cfg
        self.config_hash = config_hash
        self.store = store
        self.sink = sink
        self.metrics = metrics
        self._crash_hook = crash_hook or _no_crash
        self.deadline = deadline or Deadline()
        self.engine: Engine | None = None
        self.epoch: int | None = None
        self.expected: int | None = None  # offset первого сообщения следующей пачки
        self.counters = Counters()

    @property
    def assigned(self) -> bool:
        return self.engine is not None

    def context(self, **fields: object) -> dict[str, object]:
        return {"site": self.cfg.site_id, "epoch": self.epoch, "offset": self.expected, **fields}

    def log(self, level: int, event: str, **fields: object) -> None:
        log.log(level, event, extra={"fields": self.context(**fields)})

    # --- назначение -------------------------------------------------------

    def on_assign(self, low: int, high: int) -> int:
        """claim + проверка водяных знаков; возвращает offset, с которого читать партицию."""
        self._drop()
        with self.deadline.guard("claim", self.context()):
            claim = self.store.claim(self.cfg.site_id, LOGIC_VERSION, self.config_hash)
        nxt = claim.next_offset
        if nxt is not None and not low <= nxt <= high:
            reason = "state_older_than_log" if nxt < low else "log_recreated"
            self.epoch = claim.epoch  # только для лога отказа; ядра нет, пачки не обрабатываются
            raise StartRefused(reason, f"next_offset={nxt} вне водяных знаков [{low}, {high}]")
        self.engine = Engine(self.cfg, claim.state)
        self.epoch = claim.epoch
        self.counters = claim.counters
        self.expected = nxt if nxt is not None else low
        self.log(
            logging.INFO,
            "claim",
            next_offset=nxt,
            low=low,
            high=high,
            now=claim.now_ts,
            empty_state=claim.state is None,
            accepted=claim.counters.accepted,
            not_newer=claim.counters.not_newer,
            invalid=claim.counters.invalid,
        )
        if self.metrics is not None:
            self.metrics.on_claim(claim.epoch, self.engine.state)
        return self.expected

    def on_revoke(self, event: str = "revoke") -> None:
        """Потеря партиции (revoke или lost): ядро выбрасывается, сообщения не обрабатываются до нового claim."""
        if self.engine is not None:
            self.log(logging.INFO, event)
        self._drop()

    def _drop(self) -> None:
        self.engine = None
        self.epoch = None
        self.expected = None

    # --- пачка ------------------------------------------------------------

    def handle_batch(self, msgs: Sequence[InMsg]) -> int | None:
        """None — пачка обработана (или партиции нет); int — пачка отброшена, читать заново с этого offset."""
        engine = self.engine
        if engine is None or self.epoch is None or self.expected is None or not msgs:
            return None
        if msgs[0].offset != self.expected:
            self.log(logging.WARNING, "batch_dropped", got=msgs[0].offset, size=len(msgs))
            return self.expected
        try:
            self._process(engine, self.epoch, msgs)
        except BaseException:
            # Состояние в памяти могло уйти вперёд Postgres: ядро выбрасывается, дальше только через новый claim.
            # epoch и offset (начало упавшей пачки) остаются: по ним строка выхода в логе называет, что умерло.
            self.engine = None
            raise
        return None

    def _process(self, engine: Engine, epoch: int, msgs: Sequence[InMsg]) -> None:
        t0 = time.perf_counter()
        events: list[Event] = []
        sent = 0
        for m in msgs:
            step = engine.apply(m.value)
            for out in step.outputs:
                self.sink.send(out.topic, out.key, out.value, m.offset)
                if out.topic == DECISION_TOPIC:
                    self._log_decision(engine.state, step, out.key.decode(), m.offset)
            sent += len(step.outputs)
            events.extend(step.events)
        remaining = self.sink.flush(FLUSH_TIMEOUT_S)
        if remaining > 0 or self.sink.failed is not None:
            raise DeliveryFailed(f"не доставлено {remaining}, ошибка доставки: {self.sink.failed}")
        next_offset = msgs[-1].offset + 1
        if sent:  # авария только после пачки с записями: иначе повтор хвоста не даёт ни одной повторной записи
            self._crash_hook(self.context(next_offset=next_offset, records=sent))
        counters = self.counters.plus(count_messages(events))
        with self.deadline.guard("checkpoint", self.context(next_offset=next_offset)):
            t1 = time.perf_counter()
            written = self.store.checkpoint(self.cfg.site_id, epoch, next_offset, counters, engine.state)
            t2 = time.perf_counter()
        if not written:
            self.log(logging.ERROR, "fenced", next_offset=next_offset)
            raise Fenced(f"epoch {epoch} больше не владеет площадкой {self.cfg.site_id}")
        self.counters = counters
        self.expected = next_offset
        if self.metrics is not None:
            self.metrics.observe_batch(events, t2 - t0, t2 - t1, engine.state.now)

    def _log_decision(self, state: SiteState, step: StepResult, unit: str, offset: int) -> None:
        """INFO на каждое решение (§2.4): результат, выигрыш, ETA и ожидание по всем станциям — из события решения
        и расчёта ядра, без разбора опубликованного JSON."""
        ev = next(e for e in step.events if e.kind == "decision")
        v = state.vehicles[unit]
        result, reason = ev.get("result"), ev.get("reason")
        self.log(
            logging.INFO,
            "decision",
            offset=offset,
            unit=unit,
            at=None if state.now is None else fmt_ts(state.now),
            result=result,
            reason=reason,
            home=v.home_station,
            to=v.target_station if result == "recommended" else None,
            gain_s=ev.get("gain"),
            stations={} if reason == "stale_telemetry" else decision_stations(state, self.cfg, unit),
        )
