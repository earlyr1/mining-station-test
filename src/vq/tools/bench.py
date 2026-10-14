"""vq-bench (§6.4): время Engine.apply на сообщение — цифра для ответа на вопрос 4 ARCHITECTURE.md.

Час телеметрии генерируется в процессе замкнутым контуром (vq.generator.closed_loop: seed 42, 3600 тиков,
решения без задержки, сбои по умолчанию), поток записывается. Затем несколько полных проходов ядра по записанному
потоку, каждый с новым Engine: время — лучший и медианный проход. outputs_sha256 — хэш всех выходов прохода;
проходы обязаны совпасть. Часы потока — константа START_TS, не часы компьютера: хэш воспроизводим.

Вывод: строки для человека (время генерации отдельно), затем один JSON последней строкой stdout.
Запускается в образе приложения (`make bench` = `docker compose run --rm --no-deps dispatcher vq-bench`):
от версии Python и libm зависит последний бит гаверсинуса (§2.7).
"""

import argparse
import hashlib
import json
import os
import platform
import statistics
import time
from collections.abc import Callable, Iterable, Sequence
from typing import Any

from vq.config import SiteConfig, load_config
from vq.core.engine import Engine
from vq.core.model import Output
from vq.generator.closed_loop import FaultRates, run

DEFAULT_CONFIG = "config/site.toml"
START_TS = 1_789_473_600  # 2026-09-15T12:00:00Z
SEED = 42
TICKS = 3600  # час симуляции
PASSES = 3


def generate(cfg: SiteConfig, ticks: int = TICKS, start_ts: int = START_TS, seed: int = SEED) -> list[bytes]:
    """Телеметрия замкнутого контура в порядке отправки: решения возвращаются без задержки, сбои по умолчанию."""
    res = run(
        cfg,
        seed,
        start_ts,
        ticks,
        decision_delay=0,
        fault_rates=FaultRates(),
        check_each_tick=False,
        record_streams=True,
    )
    return list(res.telemetry)


def _put(h: Any, part: bytes) -> None:
    h.update(len(part).to_bytes(8, "big"))
    h.update(part)


def outputs_sha256(steps: Iterable[Sequence[Output]]) -> str:
    """sha256 выходов по шагам: число выходов шага, затем топик, ключ и значение каждого с длиной впереди.

    Длины делают запись однозначной: ни граница шага, ни граница ключа и значения не сдвигаются незаметно.
    """
    h = hashlib.sha256()
    for outs in steps:
        h.update(len(outs).to_bytes(8, "big"))
        for o in outs:
            _put(h, o.topic.encode())
            _put(h, o.key)
            _put(h, o.value)
    return h.hexdigest()


def run_bench(
    cfg: SiteConfig,
    stream: Sequence[bytes],
    passes: int = PASSES,
    clock: Callable[[], float] = time.perf_counter,
) -> dict[str, Any]:
    """passes полных проходов ядра по потоку, каждый с новым Engine; время — лучший и медианный проход.

    Второй проход по тому же Engine отбросил бы всё как not_newer, поэтому ядро каждый раз новое.
    Хэш выходов считается вне замера; разные хэши проходов — ошибка (ядро недетерминировано).
    """
    n = len(stream)
    times: list[float] = []
    digest: str | None = None
    for i in range(passes):
        apply = Engine(cfg).apply
        t0 = clock()
        steps = [apply(raw).outputs for raw in stream]
        times.append(clock() - t0)
        d = outputs_sha256(steps)
        if digest is not None and d != digest:
            raise RuntimeError(f"проход {i + 1} дал другие выходы: {d} ≠ {digest}")
        digest = d
    assert digest is not None

    def us(seconds: float) -> float:
        return round(seconds / n * 1e6, 2) if n else 0.0

    return {
        "messages": n,
        "us_per_msg": {"best": us(min(times)), "median": us(statistics.median(times))},
        "outputs_sha256": digest,
    }


def _positive(value: str) -> int:
    try:
        n = int(value)
    except ValueError:
        raise argparse.ArgumentTypeError(f"ожидается целое число, получено {value!r}") from None
    if n <= 0:
        raise argparse.ArgumentTypeError(f"ожидается положительное число, получено {value!r}")
    return n


def main(argv: Sequence[str] | None = None) -> int:
    p = argparse.ArgumentParser(prog="vq-bench", description="Время Engine.apply на сообщение (§6.4)")
    p.add_argument("--ticks", type=_positive, default=TICKS, help="секунд симуляции в потоке")
    p.add_argument("--passes", type=_positive, default=PASSES, help="полных проходов ядра")
    p.add_argument("--seed", type=int, default=SEED)
    args = p.parse_args(argv)
    cfg = load_config(os.environ.get("VQ_CONFIG", DEFAULT_CONFIG))
    t0 = time.perf_counter()
    stream = generate(cfg, ticks=args.ticks, seed=args.seed)
    took = time.perf_counter() - t0
    print(
        f"генерация: {args.ticks} с симуляции замкнутым контуром (seed {args.seed}, решения без задержки, "
        f"сбои по умолчанию) — {len(stream)} сообщений за {took:.1f} с"
    )
    res = run_bench(cfg, stream, passes=args.passes)
    best, median = res["us_per_msg"]["best"], res["us_per_msg"]["median"]
    print(f"ядро: проходов с новым Engine — {args.passes}; на сообщение лучший {best} мкс, медиана {median} мкс")
    res.update(python=platform.python_version(), platform=platform.platform())
    print(json.dumps(res, ensure_ascii=False), flush=True)
    return 0
