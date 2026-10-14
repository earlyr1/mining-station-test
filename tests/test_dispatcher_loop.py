"""Processor с поддельными Kafka и store (§6.1): порядок выходов и чекпоинта, фенсинг, водяные знаки, точка
аварии, сторож вызовов Postgres, лог решений и метрики."""

import copy
import logging
import threading
import time
from collections import Counter
from datetime import datetime

import pytest
from prometheus_client import CollectorRegistry

from tests.dispatcher_helpers import FakeStore, batch, empty_claim, example, line_site, make, offline, scenario
from tests.helpers import NOON, at, msg, two_stations
from vq.config import SiteConfig
from vq.core.engine import LOGIC_VERSION, Engine
from vq.core.model import SiteState, event
from vq.dispatcher.loop import (
    CRASH_EXIT_CODE,
    EXIT_FAILURE,
    Deadline,
    DeliveryFailed,
    Fenced,
    chaos_hook,
    count_messages,
    decision_stations,
)
from vq.dispatcher.metrics import DispatcherMetrics
from vq.dispatcher.store import Claim, Counters, StartRefused

# --- порядок: выходы, flush, чекпоинт ------------------------------------------------


def test_outputs_are_sent_in_order_with_src_offset_before_checkpoint():
    cfg = two_stations()
    p, store, sink, journal = make(cfg)
    assert p.on_assign(100, 200) == 100
    raws = example(cfg)
    assert p.handle_batch(batch(raws, 100)) is None
    expected = offline(cfg, raws, 100)
    assert sink.sent == expected  # те же байты, в порядке появления, каждый со своим src_offset
    assert {s[0] for s in sink.sent} == {"queue.v1", "decision.v1"}
    assert journal == ["claim", *["send"] * len(expected), "flush", "checkpoint"]
    ((epoch, next_offset, counters, state),) = store.checkpoints
    assert (epoch, next_offset, p.expected) == (7, 105, 105)
    assert counters == Counters(accepted=5)
    assert store.claims == [("test", LOGIC_VERSION, "cfg-hash")]
    ref = Engine(cfg)
    for r in raws:
        ref.apply(r)
    assert state == ref.state


# --- отказы: фенсинг и доставка ----------------------------------------------------------


def test_fencing_raises_and_stops_processing_until_new_claim():
    cfg = two_stations()
    p, _, sink, journal = make(cfg, accept=False)
    p.on_assign(40, 50)
    with pytest.raises(Fenced):
        p.handle_batch(batch(example(cfg), 40))
    assert journal[-2:] == ["flush", "checkpoint"]
    assert not p.assigned
    # строка выхода в логе называет epoch и начало упавшей пачки (контекст не стирается вместе с ядром)
    assert p.context() == {"site": "test", "epoch": 7, "offset": 40}
    sent = len(sink.sent)
    assert p.handle_batch(batch(example(cfg), 40)) is None  # без нового claim ничего не обрабатывается
    assert p.handle_batch(batch(example(cfg), 45)) is None
    assert len(sink.sent) == sent and journal.count("checkpoint") == 1


@pytest.mark.parametrize(("remaining", "failed"), [(2, None), (0, "decision.v1: Local: Message timed out")])
def test_delivery_failure_raises_without_checkpoint(remaining, failed):
    cfg = two_stations()
    p, store, _, journal = make(cfg, remaining=remaining, failed=failed)
    p.on_assign(40, 50)
    with pytest.raises(DeliveryFailed):
        p.handle_batch(batch(example(cfg), 40))
    assert journal[-1] == "flush" and "checkpoint" not in journal
    assert store.checkpoints == [] and not p.assigned
    assert p.context() == {"site": "test", "epoch": 7, "offset": 40}
    p.on_assign(40, 50)  # новый claim: состояние — из store, пачка проходит заново
    assert p.assigned and p.expected == 40


# --- offset: отбрасывание пачки, старт с начала, водяные знаки -----------------------------


@pytest.mark.parametrize("first", [0, 4, 6, 50])
def test_batch_with_unexpected_first_offset_is_dropped(first):
    cfg = two_stations()
    p, store, sink, journal = make(cfg, claim=Claim(3, 5, None, Counters(), SiteState()))
    assert p.on_assign(0, 10) == 5
    assert p.engine is not None
    before = copy.deepcopy(p.engine.state)
    assert p.handle_batch(batch(example(cfg), first)) == 5
    assert sink.sent == [] and "flush" not in journal and store.checkpoints == []
    assert p.engine.state == before and p.expected == 5
    assert p.handle_batch(batch(example(cfg), 5)) is None
    assert p.expected == 10


def test_null_next_offset_starts_from_low_watermark():
    cfg = two_stations()
    p, store, _, _ = make(cfg)
    assert p.on_assign(40, 90) == 40
    assert p.engine is not None and p.engine.state.vehicles == {}
    assert p.handle_batch(batch(example(cfg), 0)) == 40
    assert p.handle_batch(batch(example(cfg), 40)) is None
    assert store.checkpoints[-1][1] == 45


@pytest.mark.parametrize(
    ("next_offset", "low", "high", "reason"),
    [(4, 5, 20, "state_older_than_log"), (21, 5, 20, "log_recreated"), (3, 0, 0, "log_recreated")],
)
def test_next_offset_outside_watermarks_refuses_start(next_offset, low, high, reason):
    p, *_ = make(claim=Claim(2, next_offset, NOON, Counters(), SiteState(now=NOON)))
    with pytest.raises(StartRefused) as refused:
        p.on_assign(low, high)
    assert refused.value.reason == reason and refused.value.hint == "make reset"
    assert not p.assigned


@pytest.mark.parametrize("next_offset", [5, 12, 20])
def test_next_offset_within_watermarks_inclusive_is_accepted(next_offset):
    p, *_ = make(claim=Claim(2, next_offset, NOON, Counters(), SiteState(now=NOON)))
    assert p.on_assign(5, 20) == next_offset


def test_batches_are_ignored_when_not_assigned():
    cfg = two_stations()
    p, store, sink, journal = make(cfg)
    assert p.handle_batch(batch(example(cfg), 0)) is None
    p.on_assign(0, 10)
    p.on_revoke()
    assert p.handle_batch(batch(example(cfg), 0)) is None
    assert sink.sent == [] and store.checkpoints == [] and journal == ["claim"]


# --- счётчики и восстановление ---------------------------------------------------------


def test_counters_accumulate_from_claim_across_batches():
    cfg = two_stations()
    p, store, _, _ = make(cfg, claim=empty_claim(counters=Counters(10, 2, 1)))
    p.on_assign(0, 100)
    pos = at(cfg, "S1", 180.0, 3_000.0)
    first = [msg("T1", NOON, pos), msg("T1", NOON, pos), b"{not json", msg("T2", NOON, pos)]
    p.handle_batch(batch(first, 0))
    assert store.checkpoints[-1][2] == Counters(accepted=12, not_newer=3, invalid=2)
    second = [msg("T1", NOON - 5, pos), None, msg("T1", NOON + 1, pos)]
    p.handle_batch(batch(second, 4))
    assert store.checkpoints[-1][2] == Counters(accepted=13, not_newer=4, invalid=3)
    assert p.counters == Counters(13, 4, 3)


def test_count_messages_by_event_kind():
    e = Engine(two_stations())
    pos = at(e.cfg, "S1", 180.0, 3_000.0)
    events = [ev for r in [msg("T1", NOON, pos), msg("T1", NOON, pos), None] for ev in e.apply(r).events]
    assert count_messages(events) == Counters(accepted=1, not_newer=1, invalid=1)


def test_scenario_covers_kernel_paths():
    cfg = line_site()
    e = Engine(cfg)
    events = [ev for r in scenario(cfg) for ev in e.apply(r).events]
    kinds = Counter(ev.kind for ev in events)
    assert kinds["not_newer"] > 0 and kinds["invalid"] > 0 and kinds["fsm_resync"] > 0
    assert kinds["prediction_error"] > 0
    assert {(ev.get("result"), ev.get("reason")) for ev in events if ev.kind == "decision"} == {
        ("recommended", ""),
        ("rejected", "no_gain"),
    }
    assert {ev.get("reason") for ev in events if ev.kind == "released"} == {"timer", "exit"}
    assert {ev.get("how") for ev in events if ev.kind == "occupied"} == {"arrival", "handover"}


def test_processor_resumes_from_claimed_state_byte_identical():
    """Состояние из claim и чтение с next_offset дают те же записи, что непрерывный прогон (Postgres — в §6.2)."""
    cfg = line_site()
    stream = scenario(cfg)
    ref = Engine(cfg)
    ref_outputs = [ref.apply(r).outputs for r in stream]
    for k in (len(stream) // 4, len(stream) // 2, 3 * len(stream) // 4):
        first = Engine(cfg)
        for r in stream[:k]:
            first.apply(r)
        claim = Claim(9, k, first.state.now, Counters(), copy.deepcopy(first.state))
        p, _, sink, _ = make(cfg, claim=claim)
        assert p.on_assign(0, len(stream)) == k
        for start in range(k, len(stream), 500):  # пачки по 500, как consume()
            assert p.handle_batch(batch(stream[start : start + 500], start)) is None
        expected = [(o.topic, o.key, o.value, k + i) for i, outs in enumerate(ref_outputs[k:]) for o in outs]
        assert sink.sent == expected
        assert p.engine is not None and p.engine.state == ref.state


# --- точка аварии ---------------------------------------------------------------


def test_crash_hook_runs_after_flush_and_before_checkpoint():
    cfg = two_stations()
    journal: list[str] = []
    seen: list[dict] = []

    def hook(ctx):
        journal.append("crash")
        seen.append(dict(ctx))

    p, _, sink, _ = make(cfg, crash_hook=hook, journal=journal)
    p.on_assign(0, 10)
    p.handle_batch(batch(example(cfg), 0))
    assert journal[-3:] == ["flush", "crash", "checkpoint"]
    assert seen == [{"site": "test", "epoch": 7, "offset": 0, "next_offset": 5, "records": len(sink.sent)}]
    assert len(sink.sent) > 0


def test_chaos_file_crashes_once_after_flush_without_checkpoint(tmp_path, caplog):
    cfg = two_stations()
    crash_file = tmp_path / "crash"
    crash_file.touch()
    exits: list[int] = []

    def fake_exit(code: int) -> None:
        exits.append(code)
        raise SystemExit(code)

    p, store, _, journal = make(cfg, crash_hook=chaos_hook(str(crash_file), exit_fn=fake_exit))
    p.on_assign(0, 10)
    with caplog.at_level(logging.ERROR, logger="vq.dispatcher"), pytest.raises(SystemExit):
        p.handle_batch(batch(example(cfg), 0))
    assert exits == [CRASH_EXIT_CODE] == [137]
    assert not crash_file.exists()
    assert journal[-1] == "flush" and store.checkpoints == []
    (rec,) = [r for r in caplog.records if r.getMessage() == "chaos_crash"]
    assert rec.fields["next_offset"] == 5 and rec.fields["file"] == str(crash_file)
    p.on_assign(0, 10)  # перезапуск: файла нет, тот же хвост проходит
    assert p.handle_batch(batch(example(cfg), 0)) is None
    assert exits == [137] and store.checkpoints[-1][1] == 5


def test_chaos_crash_waits_for_batch_with_records_and_replay_repeats_them(tmp_path):
    """Взведённая авария не срабатывает на пачке без записей (повтор такой пачки не дал бы ни одного повтора записи);
    срабатывает на первой пачке с записями, и повтор после рестарта отправляет их ещё раз байт в байт."""
    cfg = two_stations()
    raws = example(cfg)
    crash_file = tmp_path / "crash"
    exits: list[int] = []

    def fake_exit(code: int) -> None:
        exits.append(code)
        raise SystemExit(code)

    p, store, sink, _ = make(cfg, crash_hook=chaos_hook(str(crash_file), exit_fn=fake_exit))
    p.on_assign(0, 100)
    assert p.handle_batch(batch(raws[:1], 0)) is None and sink.sent  # T1 занимает S1: записи есть, файла нет
    crash_file.touch()  # e2e-chaos взводит аварию
    quiet = [raws[1], raws[0], b"{bad", None]  # принятое без выходов, дубль, невалидные: записей нет
    assert p.handle_batch(batch(quiet, 1)) is None
    assert crash_file.exists() and exits == [] and store.checkpoints[-1][1] == 5
    before = len(sink.sent)
    with pytest.raises(SystemExit):
        p.handle_batch(batch(raws[2:], 5))
    crashed = sink.sent[before:]
    assert exits == [CRASH_EXIT_CODE] and not crash_file.exists() and len(crashed) == 5
    assert store.checkpoints[-1][1] == 5  # пачка с записями не зафиксирована
    epoch, next_offset, counters, state = store.checkpoints[-1]
    store.claim_result = Claim(epoch + 1, next_offset, state.now, counters, state)  # рестарт: последний чекпоинт
    assert p.on_assign(0, 100) == 5
    assert p.handle_batch(batch(raws[2:], 5)) is None
    assert sink.sent[before + len(crashed) :] == crashed  # повтор: те же записи с теми же src_offset
    assert exits == [CRASH_EXIT_CODE] and store.checkpoints[-1][1] == 8


def test_chaos_hook_is_noop_without_env_or_file(tmp_path):
    exits: list[int] = []
    chaos_hook(None, exit_fn=exits.append)({})
    chaos_hook("", exit_fn=exits.append)({})
    chaos_hook(str(tmp_path / "missing"), exit_fn=exits.append)({})
    assert exits == []


# --- сторож вызовов Postgres -------------------------------------------------------------------


class StuckStore(FakeStore):
    """Вызов op висит, как запрос в замёрзший Postgres, пока сторож не «завершит процесс» (release)."""

    def __init__(self, op: str, release: threading.Event):
        super().__init__([], empty_claim())
        self.op = op
        self.release = release

    def claim(self, site_id: str, logic_version: str, config_hash: str) -> Claim:
        if self.op == "claim":
            assert self.release.wait(5)
        return super().claim(site_id, logic_version, config_hash)

    def checkpoint(self, site_id: str, epoch: int, next_offset: int, counters: Counters, state: SiteState) -> bool:
        if self.op == "checkpoint":
            assert self.release.wait(5)
        return super().checkpoint(site_id, epoch, next_offset, counters, state)


def stalled(op: str) -> tuple[StuckStore, Deadline, list[int]]:
    """Store, у которого висит op, и сторож на 0,05 с; его «выход» записывает код и отпускает висящий вызов."""
    release = threading.Event()
    exits: list[int] = []

    def fake_exit(code: int) -> None:
        exits.append(code)
        release.set()  # в проде процесса уже нет; в тесте висящий вызов отпускается, чтобы тест закончился

    return StuckStore(op, release), Deadline(0.05, exit_fn=fake_exit), exits


def test_stalled_checkpoint_exits_1_and_exit_line_names_the_batch(caplog):
    """Замёрзший Postgres не исполняет statement_timeout (§2.4): чекпоинт, не вернувшийся за срок сторожа, —
    строка exit_postgres_stalled с epoch и пачкой и выход 1, а не вечное ожидание с живым /metrics."""
    cfg = two_stations()
    store, deadline, exits = stalled("checkpoint")
    p, *_ = make(cfg, store=store, deadline=deadline)
    p.on_assign(0, 10)
    t0 = time.monotonic()
    with caplog.at_level(logging.ERROR, logger="vq.dispatcher"):
        p.handle_batch(batch(example(cfg), 0))
    assert exits == [EXIT_FAILURE] == [1] and time.monotonic() - t0 < 2
    (rec,) = [r for r in caplog.records if r.getMessage() == "exit_postgres_stalled"]
    assert rec.levelname == "ERROR"
    assert rec.fields == {
        "site": "test",
        "epoch": 7,
        "offset": 0,
        "next_offset": 5,
        "op": "checkpoint",
        "deadline_s": 0.05,
    }


def test_stalled_claim_exits_1(caplog):
    store, deadline, exits = stalled("claim")
    p, *_ = make(store=store, deadline=deadline)
    with caplog.at_level(logging.ERROR, logger="vq.dispatcher"):
        p.on_assign(0, 10)
    assert exits == [1]
    (rec,) = [r for r in caplog.records if r.getMessage() == "exit_postgres_stalled"]
    assert rec.fields == {"site": "test", "epoch": None, "offset": None, "op": "claim", "deadline_s": 0.05}


def test_deadline_is_disarmed_when_postgres_answers_in_time():
    cfg = two_stations()
    exits: list[int] = []
    p, store, *_ = make(cfg, deadline=Deadline(0.05, exit_fn=exits.append))
    p.on_assign(0, 10)
    p.handle_batch(batch(example(cfg), 0))
    time.sleep(0.2)  # втрое дольше срока: взведённый и не снятый сторож уже сработал бы
    assert exits == [] and store.checkpoints[-1][1] == 5


def test_default_deadline_is_15_s():
    """Срок сторожа больше statement_timeout (5 с) и tcp_user_timeout (10 с): штатные ошибки приходят раньше."""
    p, *_ = make()
    assert p.deadline.seconds == 15.0


# --- лог решений и метрики ------------------------------------------------------------


def test_decision_log_has_eta_and_wait_for_every_station(caplog):
    cfg = two_stations()
    p, *_ = make(cfg)
    p.on_assign(0, 10)
    with caplog.at_level(logging.INFO, logger="vq.dispatcher"):
        p.handle_batch(batch(example(cfg), 0))
    logged = {r.fields["unit"]: r.fields for r in caplog.records if r.getMessage() == "decision"}
    assert set(logged) == {"T2", "T4"}
    t4 = logged["T4"]
    assert (t4["result"], t4["to"], t4["gain_s"], t4["offset"], t4["epoch"], t4["site"]) == (
        "recommended",
        "S2",
        80,
        4,
        7,
        "test",
    )
    assert t4["stations"] == {  # таблица T4 из ТЗ
        "S1": {"eta": "2026-09-15T12:01:30Z", "wait_s": 80},
        "S2": {"eta": "2026-09-15T12:04:00Z", "wait_s": 0},
    }
    t2 = logged["T2"]
    assert (t2["result"], t2["reason"], t2["gain_s"]) == ("rejected", "no_gain", 50)
    assert t2["stations"]["S1"] == {"eta": "2026-09-15T12:02:00Z", "wait_s": 50}


def test_decision_table_agrees_with_every_decision():
    """Таблица станций строки decision (decision_stations, расчёт options() ядра после шага) на всех решениях
    сценария сходится с решением: выигрыш, выбранная станция и predicted_start. Так проверяется, что между
    decide() и логом шаг не меняет того, от чего зависит расчёт."""
    cfg = line_site()
    e = Engine(cfg)
    results: Counter[str | int | None] = Counter()
    for raw in scenario(cfg):
        step = e.apply(raw)
        ev = next((x for x in step.events if x.kind == "decision"), None)
        if ev is None:
            continue
        (unit,) = [o.key.decode() for o in step.outputs if o.topic == "decision.v1"]
        v = e.state.vehicles[unit]
        table = decision_stations(e.state, cfg, unit)
        waits = {s: int(str(row["wait_s"])) for s, row in table.items()}
        etas = {s: int(datetime.fromisoformat(str(row["eta"])).timestamp()) for s, row in table.items()}
        best = min(waits.values())
        assert ev.get("gain") == waits[v.home_station] - best
        chosen = v.target_station if ev.get("result") == "recommended" else v.home_station
        assert waits[chosen] == best or ev.get("result") == "rejected"
        assert v.predicted_start == etas[chosen] + waits[chosen]
        results[ev.get("result")] += 1
    assert results["recommended"] > 0 and results["rejected"] > 0


SPEC_METRICS = {  # §5.2: имя → (тип, метки)
    "vq_messages_total": ("counter", {"result"}),
    "vq_event_time_seconds": ("gauge", set()),
    "vq_event_time_lag_seconds": ("gauge", set()),
    "vq_queue_publications_total": ("counter", {"station"}),
    "vq_queue_length": ("gauge", {"station"}),
    "vq_decisions_total": ("counter", {"result", "reason"}),
    "vq_decision_gain_seconds": ("histogram", set()),
    "vq_prediction_error_seconds": ("histogram", set()),
    "vq_prediction_abs_error_seconds": ("histogram", set()),
    "vq_fsm_resync_total": ("counter", {"station"}),
    "vq_last_checkpoint_unixtime": ("gauge", set()),
    "vq_station_releases_total": ("counter", {"station", "reason"}),
    "vq_batch_seconds": ("histogram", set()),
    "vq_checkpoint_seconds": ("histogram", set()),
    "vq_consumer_lag_messages": ("gauge", set()),
    "vq_epoch": ("gauge", set()),
}


def metrics_for(cfg: SiteConfig) -> tuple[DispatcherMetrics, CollectorRegistry]:
    reg = CollectorRegistry()
    return DispatcherMetrics(reg, stations=[s.uuid for s in cfg.stations], clock=lambda: NOON + 100.0), reg


def test_metric_names_types_and_labels_match_spec():
    cfg = two_stations()
    m, reg = metrics_for(cfg)
    p, *_ = make(cfg, metrics=m)
    p.on_assign(0, 10)
    p.handle_batch(batch(example(cfg), 0))
    found = {}
    for fam in reg.collect():
        name = fam.name + "_total" if fam.type == "counter" else fam.name
        labels = {k for s in fam.samples for k in s.labels} - {"le"}
        found[name] = (fam.type, labels)
    assert found == SPEC_METRICS


def test_prediction_error_buckets_match_spec():
    m, reg = metrics_for(two_stations())
    m.observe_events([event("prediction_error", seconds=s) for s in (-301, -7, 0, 4, 250)])
    bounds = [
        float(s.labels["le"])
        for fam in reg.collect()
        if fam.name == "vq_prediction_error_seconds"
        for s in fam.samples
        if s.name.endswith("_bucket")
    ]
    assert bounds[:-1] == [-300, -120, -60, -30, -10, -5, -1, 0, 1, 5, 10, 30, 60, 120, 300]
    cumulative = {
        le: reg.get_sample_value("vq_prediction_error_seconds_bucket", {"le": le})
        for le in ("-300.0", "-5.0", "0.0", "5.0", "+Inf")
    }
    assert cumulative == {"-300.0": 1, "-5.0": 2, "0.0": 3, "5.0": 4, "+Inf": 5}
    assert reg.get_sample_value("vq_prediction_abs_error_seconds_sum") == 301 + 7 + 0 + 4 + 250


def test_metrics_follow_kernel_events_after_checkpoint():
    cfg = two_stations()
    m, reg = metrics_for(cfg)
    p, *_ = make(cfg, metrics=m)
    p.on_assign(0, 10)
    g = reg.get_sample_value
    assert g("vq_epoch") == 7
    assert g("vq_fsm_resync_total", {"station": "S1"}) == 0  # серия есть с нулём: e2e сравнивает с 0
    p.handle_batch(batch(example(cfg), 0))
    assert g("vq_messages_total", {"result": "accepted"}) == 5
    assert g("vq_messages_total", {"result": "not_newer"}) == 0
    assert g("vq_decisions_total", {"result": "recommended", "reason": ""}) == 1
    assert g("vq_decisions_total", {"result": "rejected", "reason": "no_gain"}) == 1
    assert (g("vq_decision_gain_seconds_count"), g("vq_decision_gain_seconds_sum")) == (1, 80)
    assert g("vq_queue_length", {"station": "S1"}) == 3  # T1, T2, T3
    assert g("vq_queue_length", {"station": "S2"}) == 1  # T4
    assert g("vq_queue_publications_total", {"station": "S1"}) >= 1
    assert g("vq_event_time_seconds") == NOON
    assert g("vq_event_time_lag_seconds") == 100.0
    assert g("vq_last_checkpoint_unixtime") == NOON + 100.0
    assert g("vq_batch_seconds_count") == 1 and g("vq_checkpoint_seconds_count") == 1
    m.observe_lag(17)
    assert g("vq_consumer_lag_messages") == 17


def test_metrics_untouched_when_checkpoint_is_fenced():
    cfg = two_stations()
    m, reg = metrics_for(cfg)
    p, *_ = make(cfg, metrics=m, accept=False)
    p.on_assign(0, 10)
    with pytest.raises(Fenced):
        p.handle_batch(batch(example(cfg), 0))
    assert reg.get_sample_value("vq_messages_total", {"result": "accepted"}) == 0
    assert reg.get_sample_value("vq_batch_seconds_count") == 0


def test_claim_restores_queue_length_gauges():
    cfg = two_stations()
    e = Engine(cfg)
    for r in example(cfg):
        e.apply(r)
    m, reg = metrics_for(cfg)
    p, *_ = make(cfg, metrics=m, claim=Claim(4, 5, e.state.now, Counters(5, 0, 0), copy.deepcopy(e.state)))
    p.on_assign(0, 5)
    assert reg.get_sample_value("vq_queue_length", {"station": "S1"}) == 3
    assert reg.get_sample_value("vq_event_time_seconds") == NOON
