"""vq-bench (§6.4): поток замкнутого контура, проходы ядра с новым Engine, хэш выходов и формат вывода."""

import json
from collections.abc import Iterator
from pathlib import Path
from types import SimpleNamespace

import pytest

import vq.tools.bench as bench
from vq.config import SiteConfig, load_config
from vq.core.codec import parse_telemetry
from vq.core.model import Output
from vq.generator.closed_loop import FaultRates
from vq.tools.bench import generate, main, outputs_sha256, run_bench

SITE = Path(__file__).resolve().parent.parent / "config" / "site.toml"


@pytest.fixture(scope="module")
def cfg() -> SiteConfig:
    return load_config(SITE)


@pytest.fixture(scope="module")
def stream(cfg: SiteConfig) -> list[bytes]:
    return generate(cfg, ticks=30, start_ts=1_800_000_000)


def test_stream_is_recorded_closed_loop_telemetry_from_given_clock(cfg: SiteConfig, stream: list[bytes]) -> None:
    msgs = [parse_telemetry(raw, cfg.assignment) for raw in stream]
    assert all(m is not None for m in msgs)
    assert len(stream) >= 30 * 40  # каждая машина на каждом тике, плюс дубли
    first_tick = [m for m in msgs[:40] if m is not None]
    assert {m.ts for m in first_tick} == {1_800_000_000}  # часы — аргумент, не часы компьютера
    assert {m.unit_uuid for m in first_tick} == set(cfg.assignment)


def test_stream_does_not_depend_on_the_run(cfg: SiteConfig, stream: list[bytes]) -> None:
    assert generate(cfg, ticks=30, start_ts=1_800_000_000) == stream


def test_stream_has_injected_faults(cfg: SiteConfig, stream: list[bytes]) -> None:
    """Сбои по умолчанию: дубли (тот же unit_uuid и ts) уходят в поток сверх 40 сообщений на тик."""
    msgs = [m for raw in stream if (m := parse_telemetry(raw, cfg.assignment)) is not None]
    assert len({(m.unit_uuid, m.ts) for m in msgs}) == 30 * 40 < len(stream)


def test_generation_is_closed_loop_seed_42_without_delay_with_default_faults(
    cfg: SiteConfig, monkeypatch: pytest.MonkeyPatch
) -> None:
    calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    def fake_run(*args: object, **kwargs: object) -> SimpleNamespace:
        calls.append((args, kwargs))
        return SimpleNamespace(telemetry=[b"x"])

    monkeypatch.setattr(bench, "run", fake_run)
    assert generate(cfg) == [b"x"]
    ((args, kwargs),) = calls
    assert args == (cfg, 42, bench.START_TS, 3600)
    assert kwargs == {
        "decision_delay": 0,
        "fault_rates": FaultRates(),
        "check_each_tick": False,
        "record_streams": True,
    }
    assert FaultRates() == FaultRates(dup=0.005, late=0.002, outage=0.0005)  # умолчания §4.5


def test_outputs_hash_covers_order_step_and_bytes() -> None:
    a, b = Output("queue.v1", b"S1", b"x"), Output("decision.v1", b"T-01", b"y")
    base = outputs_sha256([(a, b), ()])
    assert outputs_sha256([(a, b), ()]) == base
    assert outputs_sha256([(b, a), ()]) != base  # порядок выходов в шаге
    assert outputs_sha256([(a,), (b,)]) != base  # выход другого сообщения
    assert outputs_sha256([(a, Output("decision.v1", b"T-01", b"z")), ()]) != base  # байты значения
    assert outputs_sha256([(Output("queue.v1", b"S2", b"x"), b), ()]) != base  # ключ
    assert outputs_sha256([(Output("queue.v1", b"S1x", b""), b), ()]) != base  # граница ключа и значения


def test_each_pass_starts_from_a_fresh_engine(cfg: SiteConfig, stream: list[bytes]) -> None:
    """Второй проход по тому же Engine отбросил бы всё как not_newer: выходы проходов обязаны совпасть."""
    one = run_bench(cfg, stream, passes=1)
    three = run_bench(cfg, stream, passes=3)
    assert three["outputs_sha256"] == one["outputs_sha256"]
    assert three["messages"] == one["messages"] == len(stream)


def test_time_per_message_is_best_and_median_of_passes(cfg: SiteConfig, stream: list[bytes]) -> None:
    ticks: Iterator[float] = iter([0.0, 4.0, 10.0, 11.0, 20.0, 21.5])  # проходы 4, 1 и 1.5 с: медиана ≠ среднему
    res = run_bench(cfg, stream, passes=3, clock=lambda: next(ticks))
    n = len(stream)
    assert res["us_per_msg"] == {"best": round(1.0 / n * 1e6, 2), "median": round(1.5 / n * 1e6, 2)}


def test_main_prints_generation_time_then_json(capsys: pytest.CaptureFixture[str]) -> None:
    assert main(["--ticks", "5", "--passes", "1"]) == 0
    lines = capsys.readouterr().out.splitlines()
    res = json.loads(lines[-1])
    assert set(res) == {"messages", "us_per_msg", "outputs_sha256", "python", "platform"}
    assert res["messages"] >= 5 * 40 and set(res["us_per_msg"]) == {"best", "median"}
    assert len(res["outputs_sha256"]) == 64
    assert any("генерация" in line for line in lines[:-1])


@pytest.mark.parametrize("args", [["--ticks", "0"], ["--passes", "0"], ["--ticks", "x"]])
def test_main_rejects_bad_arguments(args: list[str]) -> None:
    with pytest.raises(SystemExit) as e:
        main(args)
    assert e.value.code == 2
