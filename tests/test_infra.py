"""Стенд без демона Docker: compose, Makefile, Redpanda, Prometheus, дашборд Grafana; чистые части tools/e2e.py.

Проверкам compose нужен `docker compose` (только `config`, демон не нужен), проверкам Makefile — `make`;
без них они пропускаются. Живой стенд проверяют make e2e и make e2e-chaos.
"""

from __future__ import annotations

import importlib.util
import inspect
import json
import math
import operator
import os
import re
import shutil
import socket
import stat
import subprocess
import sys
import threading
import tomllib
from collections.abc import Callable, Iterator
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from types import ModuleType
from typing import Any

import pytest
from prometheus_client import CollectorRegistry, generate_latest

from vq.core.decision import DECISION_TOPIC
from vq.core.engine import QUEUE_TOPIC
from vq.core.model import SiteState, event
from vq.dispatcher.main import TELEMETRY_TOPIC
from vq.dispatcher.metrics import DispatcherMetrics
from vq.generator.main import FLUSH_TIMEOUT_S, DecisionFeed
from vq.generator.metrics import GeneratorMetrics

ROOT = Path(__file__).resolve().parents[1]
APP_SERVICES = ("dispatcher", "dispatcher-b", "generator", "viewer")
COUNTER_FUNCS = {"rate", "increase", "resets", "irate"}

needs_docker = pytest.mark.skipif(shutil.which("docker") is None, reason="нужен docker CLI")
needs_make = pytest.mark.skipif(shutil.which("make") is None, reason="нужен make")
needs_bash = pytest.mark.skipif(shutil.which("bash") is None, reason="нужен bash")


def load_tool(name: str) -> ModuleType:
    """Скрипт из tools/ (не пакет); модуль регистрируется в sys.modules, как того ждут dataclasses."""
    mod_name = f"vq_tool_{name}"
    if mod_name in sys.modules:
        return sys.modules[mod_name]
    spec = importlib.util.spec_from_file_location(mod_name, ROOT / "tools" / f"{name}.py")
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = mod
    spec.loader.exec_module(mod)
    return mod


def compose_config(
    env_extra: dict[str, str] | None = None, drop: tuple[str, ...] = (), env_file: str | None = None
) -> dict[str, Any]:
    """`docker compose config` всех профилей; env_file="/dev/null" — без .env прототипа."""
    env = {k: v for k, v in os.environ.items() if k not in drop}
    env.update(env_extra or {})
    args = ["docker", "compose", *(["--env-file", env_file] if env_file else []), "--profile", "*", "config"]
    out = subprocess.run(
        [*args, "--format", "json"], cwd=ROOT, env=env, capture_output=True, text=True, check=True
    ).stdout
    return json.loads(out)


@pytest.fixture(scope="module")
def cfg() -> dict[str, Any]:
    return compose_config({"VQ_UID": "4242", "VQ_GID": "4343"})


@pytest.fixture(scope="module")
def defaults() -> dict[str, Any]:
    """Как в чистом клоне: без .env и без переменных VQ_* и COMPOSE_PROJECT_NAME в окружении."""
    drop = tuple(k for k in os.environ if k.startswith("VQ_") or k == "COMPOSE_PROJECT_NAME")
    return compose_config(drop=drop, env_file="/dev/null")


# --- один образ приложения: собирает только dispatcher, остальные берут готовый -----------------------------


@needs_docker
def test_only_dispatcher_builds_the_app_image(cfg: dict[str, Any]) -> None:
    """build у нескольких сервисов: тег достаётся последней сборке, и каждый `up --build` пересоздаёт всё."""
    services = cfg["services"]
    builders = sorted(name for name, s in services.items() if "build" in s)
    assert builders == ["dispatcher"]
    images = {services[n]["image"] for n in APP_SERVICES}
    assert len(images) == 1, images


@needs_docker
def test_app_services_never_pull(cfg: dict[str, Any]) -> None:
    """Образа нет в реестре: попытка pull на чистой машине печатает «pull access denied»."""
    for name in APP_SERVICES:
        assert cfg["services"][name].get("pull_policy") == "never", name


# --- сервисы приложения работают от пользователя хоста ------------------------------------------------------


@needs_docker
def test_app_services_run_as_host_user(cfg: dict[str, Any]) -> None:
    for name in APP_SERVICES:
        assert cfg["services"][name]["user"] == "4242:4343", name
    plain = compose_config(drop=("VQ_UID", "VQ_GID"))
    assert plain["services"]["dispatcher"]["user"] == "1000:1000"  # без make — пользователь образа


FAKE_TOOL = (
    "#!/bin/sh\n"
    'printf \'%s|%s|UP_PROFILES=%s|VQ_UID=%s|VQ_GID=%s|VQ_KAFKA=%s|VQ_PG_DSN=%s\\n\' "$(basename "$0")" "$*" '
    '"${UP_PROFILES-<unset>}" "${VQ_UID-<unset>}" "${VQ_GID-<unset>}" "${VQ_KAFKA-<unset>}" '
    '"${VQ_PG_DSN-<unset>}" >> "$FAKE_LOG"\n'
)


def write_tool(path: Path, script: str) -> None:
    path.write_text(script)
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


@pytest.fixture()
def fake_bin(tmp_path: Path) -> Iterator[tuple[Path, Path]]:
    """Подделки docker и uv: пишут argv и нужное окружение в лог, ничего не запускают."""
    bindir, log = tmp_path / "bin", tmp_path / "calls.log"
    bindir.mkdir()
    for tool in ("docker", "uv"):
        write_tool(bindir / tool, FAKE_TOOL)
    yield bindir, log


def run_make(
    target: str,
    fake: tuple[Path, Path],
    *args: str,
    env_extra: dict[str, str] | None = None,
    cwd: Path = ROOT,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    bindir, log = fake
    env = {k: v for k, v in os.environ.items() if not k.startswith("VQ_") and k not in ("UP_PROFILES", "MAKEFLAGS")}
    env.update({"PATH": f"{bindir}:{env['PATH']}", "FAKE_LOG": str(log)}, **(env_extra or {}))
    return subprocess.run(["make", target, *args], cwd=cwd, env=env, capture_output=True, text=True, check=check)


def make_calls(target: str, fake: tuple[Path, Path], *args: str, env_extra: dict[str, str] | None = None) -> list[str]:
    """Вызовы подделок за один `make target`: «инструмент|argv|окружение»."""
    log = fake[1]
    log.unlink(missing_ok=True)
    run_make(target, fake, *args, env_extra=env_extra)
    return log.read_text().splitlines() if log.exists() else []


@needs_make
def test_makefile_passes_host_uid(fake_bin: tuple[Path, Path]) -> None:
    calls = make_calls("up", fake_bin)
    assert len(calls) == 1 and calls[0].startswith("docker|compose --profile demo up -d --build --wait")
    assert f"VQ_UID={os.getuid()}|VQ_GID={os.getgid()}" in calls[0]
    calls = make_calls("up", fake_bin, env_extra={"VQ_UID": "0", "VQ_GID": "0"})  # sudo make up
    assert "VQ_UID=0|VQ_GID=0" in calls[0]


# --- без .env порты, имя проекта и адреса — умолчания §2.2 --------------------------------------------------

SPEC_PORTS = {
    ("redpanda", 19092): "19092",
    ("redpanda", 9644): "19644",
    ("postgres", 5432): "5432",
    ("dispatcher", 9100): "9100",
    ("generator", 9101): "9101",
    ("viewer", 8000): "8000",
    ("console", 8080): "8080",
    ("prometheus", 9090): "9090",
    ("grafana", 3000): "3000",
}


def published(conf: dict[str, Any]) -> dict[tuple[str, int], str]:
    return {(name, p["target"]): p["published"] for name, s in conf["services"].items() for p in s.get("ports", [])}


@needs_docker
def test_without_env_file_ports_and_project_are_spec_defaults(defaults: dict[str, Any]) -> None:
    """.env прототипа (порты 2xxxx, проект vqproto) в репозиторий не переносится: умолчания живут в compose."""
    assert published(defaults) == SPEC_PORTS
    assert defaults["name"] == "vq"
    assert {defaults["services"][n]["image"] for n in APP_SERVICES} == {"vq-app:latest"}


@needs_docker
def test_ports_are_published_on_loopback_only(cfg: dict[str, Any], defaults: dict[str, Any]) -> None:
    for conf in (cfg, defaults):
        for name, s in conf["services"].items():
            for p in s.get("ports", []):
                assert p.get("host_ip") == "127.0.0.1", (name, p)
    assert set(published(cfg)) == set(SPEC_PORTS)  # .env меняет номера, но не набор портов


@needs_make
def test_makefile_without_env_file_prints_spec_addresses(tmp_path: Path, fake_bin: tuple[Path, Path]) -> None:
    work = tmp_path / "clone"
    work.mkdir()
    shutil.copy(ROOT / "Makefile", work / "Makefile")
    out = run_make("up", fake_bin, cwd=work).stdout
    for url in ("localhost:8000", "localhost:8080", "localhost:3000/d/vq", "localhost:9090"):
        assert f"http://{url}\n" in out, out


# --- брокер и топики (§2.2, §2.3) ---------------------------------------------------------------------------

SPEC_TOPICS = {
    TELEMETRY_TOPIC: ("1", {"cleanup.policy": "delete", "retention.ms": "604800000"}),
    QUEUE_TOPIC: ("1", {"cleanup.policy": "compact", "min.compaction.lag.ms": "86400000"}),  # история для verify
    DECISION_TOPIC: ("1", {"cleanup.policy": "delete", "retention.ms": "604800000"}),
}


def init_script() -> str:
    return (ROOT / "deploy" / "redpanda-init.sh").read_text(encoding="utf-8")


def test_redpanda_init_creates_spec_topics() -> None:
    m = re.search(r"^TOPICS=\((.*?)^\)", init_script(), re.S | re.M)
    assert m is not None, "нет массива TOPICS"
    got = {
        name: (parts, dict(kv.split("=", 1) for kv in configs.split()))
        for name, parts, configs in re.findall(r'"([^|"]+)\|(\d+)\|([^"]*)"', m.group(1))
    }
    assert got == SPEC_TOPICS
    assert re.search(r"^REPLICAS=1$", init_script(), re.M)


def test_redpanda_init_sets_cluster_options() -> None:
    """Без автосоздания топиков (опечатка в имени не создаёт пустой топик); первый join группы без задержки."""
    got = re.findall(r"^set_cluster (\S+) (\S+)$", init_script(), re.M)
    assert got == [("auto_create_topics_enabled", "false"), ("group_initial_rebalance_delay", "0")]


# Поддельный rpk: состояние кластера — JSON-файл; вывод — в формате rpk v26.2.3 (снят со стенда).
FAKE_RPK = """#!{python}
import json, os, sys

path = os.environ["FAKE_RPK_STATE"]
with open(path) as fh:
    st = json.load(fh)
args = sys.argv[1:]
while args[:1] == ["-X"]:
    args = args[2:]


def save():
    with open(path, "w") as fh:
        json.dump(st, fh)


if args[:2] == ["cluster", "info"]:
    print("CLUSTER\\n=======\\nredpanda.fake")
elif args[:3] == ["cluster", "config", "get"]:
    print(st["cluster"].get(args[3], "-1"))
elif args[:3] == ["cluster", "config", "set"]:
    st["cluster"][args[3]] = args[4]
    save()
elif args[:2] == ["topic", "list"]:
    print("NAME          PARTITIONS  REPLICAS")
    for name, t in sorted(st["topics"].items()):
        print(f"{{name:<13}} {{t['partitions']:<11}} {{t['replicas']}}")
elif args[:2] == ["topic", "create"]:
    name, rest = args[2], args[3:]
    t = {{"partitions": None, "replicas": None, "configs": {{}}}}
    for flag, value in zip(rest[::2], rest[1::2]):
        if flag == "-p":
            t["partitions"] = value
        elif flag == "-r":
            t["replicas"] = value
        elif flag == "-c":
            key, _, val = value.partition("=")
            t["configs"][key] = val
    st["topics"][name] = t
    st["created"].append(name)
    save()
elif args[:2] == ["topic", "describe"] and args[3] == "-s":
    t = st["topics"][args[2]]
    print(f"NAME        {{args[2]}}\\nPARTITIONS  {{t['partitions']}}\\nREPLICAS    {{t['replicas']}}")
elif args[:2] == ["topic", "describe"] and args[3] == "-c":
    t = st["topics"][args[2]]
    every = {{"cleanup.policy": "delete", "min.compaction.lag.ms": "0", "retention.ms": "604800000", **t["configs"]}}
    print("KEY                    VALUE      SOURCE")
    for key, val in sorted(every.items()):
        source = "DYNAMIC_TOPIC_CONFIG" if key in t["configs"] else "DEFAULT_CONFIG"
        print(f"{{key:<22}} {{val:<10}} {{source}}")
else:
    sys.exit("fake rpk: " + " ".join(args))
"""


def run_init(tmp_path: Path, topics: dict[str, Any] | None = None) -> tuple[subprocess.CompletedProcess[str], Any]:
    """deploy/redpanda-init.sh против поддельного rpk; topics — уже существующие топики."""
    bindir, state = tmp_path / "bin", tmp_path / "rpk-state.json"
    bindir.mkdir(exist_ok=True)
    write_tool(bindir / "rpk", FAKE_RPK.format(python=sys.executable))
    if topics is not None or not state.exists():
        state.write_text(json.dumps({"cluster": {}, "topics": topics or {}, "created": []}))
    env = {**os.environ, "PATH": f"{bindir}:{os.environ['PATH']}", "FAKE_RPK_STATE": str(state)}
    bash = shutil.which("bash") or "bash"
    r = subprocess.run([bash, str(ROOT / "deploy" / "redpanda-init.sh")], env=env, capture_output=True, text=True)
    return r, json.loads(state.read_text())


def spec_topic(name: str, **change: Any) -> dict[str, Any]:
    parts, configs = SPEC_TOPICS[name]
    return {"partitions": parts, "replicas": "1", "configs": dict(configs), **change}


@needs_bash
def test_redpanda_init_creates_missing_topics_and_is_idempotent(tmp_path: Path) -> None:
    r, st = run_init(tmp_path, topics={})
    assert r.returncode == 0, r.stderr
    assert st["cluster"] == {"auto_create_topics_enabled": "false", "group_initial_rebalance_delay": "0"}
    assert st["topics"] == {name: spec_topic(name) for name in SPEC_TOPICS}
    again, st = run_init(tmp_path)  # повторный `make up` после `make down`: топики уже есть
    assert again.returncode == 0, again.stderr
    assert sorted(st["created"]) == sorted(SPEC_TOPICS)  # ничего не создано второй раз
    assert again.stdout.count("уже есть, не создаю") == 3


@needs_bash
@pytest.mark.parametrize(
    ("topic", "change", "message"),
    [
        (QUEUE_TOPIC, {"configs": {"cleanup.policy": "delete"}}, "cleanup.policy = 'delete'"),
        (QUEUE_TOPIC, {"configs": {"cleanup.policy": "compact"}}, "min.compaction.lag.ms = '0'"),
        (TELEMETRY_TOPIC, {"partitions": "3"}, "партиций '3'"),
    ],
)
def test_redpanda_init_refuses_a_topic_that_differs_from_the_spec(
    tmp_path: Path, topic: str, change: dict[str, Any], message: str
) -> None:
    """Чужой или старый топик не исправляется молча: init падает, и стенд не стартует до make reset."""
    r, st = run_init(tmp_path, topics={topic: spec_topic(topic, **change)})
    assert r.returncode != 0
    assert message in r.stderr and "make reset" in r.stderr, r.stderr
    assert topic not in st["created"]


@needs_docker
def test_redpanda_is_one_dev_node_advertising_its_published_port(cfg: dict[str, Any], defaults: dict[str, Any]) -> None:
    """Внешний адрес — 127.0.0.1 и опубликованный порт: иначе клиенты на хосте (e2e, тесты) не подключатся."""
    for conf in (cfg, defaults):
        cmd = conf["services"]["redpanda"]["command"]
        assert {"--mode=dev-container", "--smp=1", "--memory=1G"} <= set(cmd)
        port = published(conf)[("redpanda", 19092)]
        assert f"--advertise-kafka-addr=internal://redpanda:9092,external://127.0.0.1:{port}" in cmd


# --- порядок старта, остановка, демо-генератор (§2.2, §4.4) -------------------------------------------------

APP_DEPS = {"redpanda-init": "service_completed_successfully", "postgres": "service_healthy"}


@needs_docker
def test_start_order(cfg: dict[str, Any]) -> None:
    want = {
        "redpanda": {},
        "redpanda-init": {"redpanda": "service_healthy"},
        "postgres": {},
        "dispatcher": APP_DEPS,
        "dispatcher-b": APP_DEPS,
        "viewer": APP_DEPS,
        "generator": {"redpanda-init": "service_completed_successfully"},
        "console": {"redpanda": "service_healthy"},
        "prometheus": {},
        "grafana": {"prometheus": "service_started"},
    }
    got = {n: {d: v["condition"] for d, v in (s.get("depends_on") or {}).items()} for n, s in cfg["services"].items()}
    assert got == want


@needs_docker
def test_dispatchers_get_ten_seconds_to_stop(cfg: dict[str, Any]) -> None:
    for name in ("dispatcher", "dispatcher-b"):
        assert cfg["services"][name]["stop_grace_period"] == "10s", name


@needs_docker
def test_generator_gets_time_to_flush_and_write_the_summary_on_stop(cfg: dict[str, Any]) -> None:
    """SIGTERM от `make down` и `docker compose stop` (§4.4): flush ждёт до FLUSH_TIMEOUT_S, затем стоп ленты решений.

    Без stop_grace_period Docker шлёт SIGKILL через 10 с, а это меньше FLUSH_TIMEOUT_S: при недоступном или
    медленном брокере summary.json не пишется, а код выхода 137 вместо 1.
    """
    grace = cfg["services"]["generator"].get("stop_grace_period")
    assert grace == "40s", grace
    need = FLUSH_TIMEOUT_S + inspect.signature(DecisionFeed.stop).parameters["timeout"].default
    assert float(grace.removesuffix("s")) >= need, f"{grace} меньше flush + стоп ленты решений = {need} с"


@needs_docker
def test_demo_generator_runs_on_a_dirty_topic(defaults: dict[str, Any]) -> None:
    """`make down` сохраняет тома: повторный `make up` идёт по непустому telemetry.v1 (§4.4)."""
    gen = defaults["services"]["generator"]
    assert gen["command"] == ["vq-generator", "--speed", "1", "--allow-dirty"]
    assert gen["profiles"] == ["demo"]


# --- healthcheck: dispatcher healthy только после claim -----------------------------------------------------


@pytest.fixture()
def metrics_endpoint() -> Iterator[tuple[int, Callable[[bytes], None]]]:
    """HTTP /metrics на свободном порту; тело задаёт тест."""
    body = [b""]

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
            self.end_headers()
            self.wfile.write(body[0])

        def log_message(self, *args: Any) -> None:
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    def serve(text: bytes) -> None:
        body[0] = text

    yield server.server_address[1], serve
    server.shutdown()
    server.server_close()


def healthcheck(conf: dict[str, Any], service: str, port: int) -> int:
    """Код выхода healthcheck сервиса против /metrics на port (вместо 9100 внутри контейнера)."""
    test = conf["services"][service]["healthcheck"]["test"]
    assert test[:3] == ["CMD", "python", "-c"], test
    code = test[3].replace("127.0.0.1:9100", f"127.0.0.1:{port}")
    assert str(port) in code
    return subprocess.run([sys.executable, "-c", code], capture_output=True, timeout=30).returncode


def dispatcher_metrics(epoch: int | None) -> bytes:
    """Настоящий /metrics dispatcher: до claim (None) или после claim с этим epoch."""
    registry = CollectorRegistry()
    metrics = DispatcherMetrics(registry=registry)
    if epoch is not None:
        metrics.on_claim(epoch, SiteState())
    return generate_latest(registry)


@needs_docker
def test_dispatcher_is_healthy_only_after_claim(
    cfg: dict[str, Any], metrics_endpoint: tuple[int, Callable[[bytes], None]]
) -> None:
    """/metrics поднимается до Kafka и claim; при отказе старта (exit 2) vq_epoch так и остаётся 0.

    Поэтому `make up --wait` ждёт claim, а не только HTTP-сервер.
    """
    port, serve = metrics_endpoint
    serve(dispatcher_metrics(None))
    assert healthcheck(cfg, "dispatcher", port) != 0
    serve(dispatcher_metrics(3))
    assert healthcheck(cfg, "dispatcher", port) == 0
    serve(b"vq_epoch_other 5.0\n")
    assert healthcheck(cfg, "dispatcher", port) != 0


@needs_docker
def test_standby_is_healthy_without_a_claim(
    cfg: dict[str, Any], metrics_endpoint: tuple[int, Callable[[bytes], None]]
) -> None:
    """Standby партицию не забирает, пока жив активный: ему достаточно отвечающего /metrics."""
    port, serve = metrics_endpoint
    serve(dispatcher_metrics(None))
    assert healthcheck(cfg, "dispatcher-b", port) == 0


def free_port() -> int:
    """Порт 127.0.0.1, на котором никто не слушает: занят и тут же освобождён."""
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        port: int = s.getsockname()[1]
    return port


@needs_docker
def test_healthcheck_fails_without_metrics_server(cfg: dict[str, Any]) -> None:
    port = free_port()
    assert healthcheck(cfg, "dispatcher", port) != 0
    assert healthcheck(cfg, "dispatcher-b", port) != 0


# --- make e2e, verify, bench, lint и отказ старта -----------------------------------------------------------


@needs_make
@pytest.mark.parametrize("exported", [False, True], ids=["shell-clean", "shell-exports-ports"])
def test_e2e_resets_without_demo_generator(
    fake_bin: tuple[Path, Path], monkeypatch: pytest.MonkeyPatch, exported: bool
) -> None:
    """make e2e начинается со свежих томов без демо-генератора и передаёт инструментам адреса на хосте.

    run_make не передаёт make переменные VQ_* оболочки: порты в адресах — из .env или умолчания §2.2, и
    экспортированные VQ_KAFKA_PORT и VQ_PG_PORT (документированное переопределение) тест не роняют.
    """
    for name, port in (("VQ_KAFKA_PORT", "29092"), ("VQ_PG_PORT", "25432")):
        if exported:
            monkeypatch.setenv(name, port)
        else:
            monkeypatch.delenv(name, raising=False)
    calls = make_calls("e2e", fake_bin)
    assert [c.split("|")[:2] for c in calls] == [
        ["docker", "compose --profile * down -v --remove-orphans"],
        ["docker", "compose up -d --build --wait --wait-timeout 180"],
        ["uv", "run python tools/e2e.py"],
    ]
    uv = calls[-1]
    assert "UP_PROFILES=|" in uv  # вложенный make reset из e2e.py тоже без демо
    kafka_port = _dotenv().get("VQ_KAFKA_PORT", "19092")
    pg_port = _dotenv().get("VQ_PG_PORT", "5432")
    assert f"VQ_KAFKA=127.0.0.1:{kafka_port}|" in uv
    assert uv.endswith(f"VQ_PG_DSN=postgresql://vq:vq@127.0.0.1:{pg_port}/vq")
    chaos = make_calls("e2e-chaos", fake_bin)
    assert chaos[-1].split("|")[:2] == ["uv", "run python tools/e2e.py --chaos"]
    assert "--profile demo" not in "".join(chaos)


@needs_make
def test_verify_and_bench_run_in_the_app_image(fake_bin: tuple[Path, Path]) -> None:
    """§2.7: сверка и замер — в образе приложения; bench не нужны брокер и БД, стенд он не поднимает."""
    assert [c.split("|")[:2] for c in make_calls("verify", fake_bin)] == [
        ["docker", "compose run --rm dispatcher vq-verify"]
    ]
    assert [c.split("|")[:2] for c in make_calls("bench", fake_bin)] == [
        ["docker", "compose run --rm --no-deps dispatcher vq-bench"]
    ]


@needs_make
def test_lint_runs_ruff_and_strict_mypy_on_src_and_tools(fake_bin: tuple[Path, Path]) -> None:
    assert [c.split("|")[:2] for c in make_calls("lint", fake_bin)] == [
        ["uv", "run ruff check ."],
        ["uv", "run ruff format --check ."],
        ["uv", "run mypy src tools"],  # tools/e2e.py и генераторы — тоже код проекта
    ]
    mypy = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))["tool"]["mypy"]
    assert mypy["strict"] is True


# Лог dispatcher в форме vq.logs: голова строки, поля log_defaults, затем поля события.
REFUSED = (
    '{"ts": "2026-10-14T10:00:00.100Z", "level": "INFO", "logger": "vq.dispatcher", "event": "start", '
    '"site": "site-1", "epoch": null, "offset": null, "config": "/app/config/site.toml", "config_hash": "bb…", '
    '"kafka": "redpanda:9092"}\n'
    '{"ts": "2026-10-14T10:00:00.900Z", "level": "ERROR", "logger": "vq.dispatcher", "event": "start_refused", '
    '"site": "site-1", "epoch": null, "offset": null, "reason": "config_changed", '
    '"detail": "в Postgres logic_version=1 config_hash=aa…, в коде logic_version=1 config_hash=bb…", '
    '"hint": "make reset"}\n'
)
FAILING_UP = (
    "#!/bin/sh\n"
    'case "$*" in\n'
    '  *" up "*) echo "container vq-dispatcher-1 is unhealthy" >&2; exit 1 ;;\n'
    f"  *\" logs \"*) printf '%s' '{REFUSED}' ;;\n"
    "esac\n"
)


@needs_make
def test_failed_up_shows_why_the_dispatcher_refused_to_start(fake_bin: tuple[Path, Path]) -> None:
    """`up --wait` сообщает только «container … is unhealthy»: причина отказа старта — из лога dispatcher."""
    write_tool(fake_bin[0] / "docker", FAILING_UP)
    r = run_make("up", fake_bin, check=False)
    assert r.returncode != 0
    shown = [json.loads(line) for line in r.stdout.splitlines() if line.startswith("{")]
    assert [(d["event"], d.get("reason")) for d in shown] == [("start_refused", "config_changed")]  # не весь лог
    assert "make reset" in r.stderr and "make logs" in r.stderr
    assert "Витрина" not in r.stdout  # адреса не печатаются, раз стенд не поднялся


def _dotenv() -> dict[str, str]:
    path = ROOT / ".env"
    if not path.exists():
        return {}
    pairs = (line.split("=", 1) for line in path.read_text().splitlines() if "=" in line and not line.startswith("#"))
    return {k.strip(): v.strip() for k, v in pairs}


# --- standby не отбирает партицию у активного ---------------------------------------------------------------


def _kafka_str_cmp(a: str, b: str) -> int:
    """Как rd_kafkap_str_cmp в librdkafka: memcmp общей длины, затем длина."""
    ab, bb = a.encode(), b.encode()
    n = min(len(ab), len(bb))
    if ab[:n] != bb[:n]:
        return -1 if ab[:n] < bb[:n] else 1
    return (len(ab) > len(bb)) - (len(ab) < len(bb))


@needs_docker
def test_standby_instance_id_sorts_after_active(cfg: dict[str, Any]) -> None:
    """range-ассайнер сортирует static-участников по group.instance.id и отдаёт единственную партицию первому."""
    site = tomllib.loads((ROOT / "config/site.toml").read_text())["site"]["id"]
    active = f"dispatcher-{site}"  # умолчание vq.dispatcher.main.consumer_config
    assert "VQ_GROUP_INSTANCE_ID" not in cfg["services"]["dispatcher"].get("environment", {})
    standby = cfg["services"]["dispatcher-b"]["environment"]["VQ_GROUP_INSTANCE_ID"]
    assert standby != active, "одинаковый group.instance.id: участники вытесняют друг друга"
    assert re.fullmatch(r"[A-Za-z0-9._-]+", standby), "недопустимые для Kafka символы"
    assert _kafka_str_cmp(active, standby) < 0, f"range отдаст партицию standby: {standby!r} < {active!r}"


@needs_docker
def test_standby_has_no_published_metrics_port(cfg: dict[str, Any]) -> None:
    assert not cfg["services"]["dispatcher-b"].get("ports")  # §2.2: без проброса порта метрик


def prometheus_targets() -> dict[str, list[str]]:
    text = (ROOT / "deploy/prometheus.yml").read_text()
    jobs = re.findall(r"job_name:\s*(\S+)\s+static_configs:\s+- targets:\s*\[([^\]]*)\]", text)
    return {job: [t.strip().strip('"') for t in tl.split(",")] for job, tl in jobs}


def test_prometheus_scrapes_dispatcher_and_standby() -> None:
    targets = prometheus_targets()
    assert targets["dispatcher"] == ["dispatcher:9100", "dispatcher-b:9100"]
    assert targets["generator"] == ["generator:9101"]


@needs_docker
def test_viewer_reads_metrics_of_dispatcher_and_standby(cfg: dict[str, Any]) -> None:
    """Демо перехвата: витрина опрашивает оба экземпляра и показывает владельца площадки (наибольший vq_epoch)."""
    env = cfg["services"]["viewer"]["environment"]
    urls = [u.strip() for u in env["VQ_DISPATCHER_METRICS"].split(",")]
    assert urls == [f"http://{t}/metrics" for t in prometheus_targets()["dispatcher"]]


# --- авария изнутри контейнера: перезапуск политикой работает только с init ---------------------------------


@needs_docker
def test_app_services_have_init(cfg: dict[str, Any]) -> None:
    """PID 1 — tini: `kill -KILL -1` изнутри убивает процесс, и restart: unless-stopped поднимает контейнер."""
    for name in APP_SERVICES:
        assert cfg["services"][name].get("init") is True, name


@needs_docker
def test_dispatcher_has_crash_file_for_chaos(cfg: dict[str, Any]) -> None:
    """make e2e-chaos взводит точку аварии файлом внутри контейнера dispatcher."""
    env = cfg["services"]["dispatcher"]["environment"]
    assert env["VQ_CRASH_FILE"] == E2E.CRASH_FILE


# --- политики перезапуска -----------------------------------------------------------------------------------


@needs_docker
def test_restart_policies(cfg: dict[str, Any]) -> None:
    """После рестарта Docker поднимаются брокер, БД и все долгоживущие сервисы; генератор — никогда (§4.4)."""
    want = {
        "redpanda": "unless-stopped",
        "postgres": "unless-stopped",
        "dispatcher": "unless-stopped",
        "dispatcher-b": "unless-stopped",
        "viewer": "unless-stopped",
        "console": "unless-stopped",
        "prometheus": "unless-stopped",
        "grafana": "unless-stopped",
        "generator": "no",  # §2.2, §4.4
        "redpanda-init": "no",
    }
    got = {name: s.get("restart", "no") for name, s in cfg["services"].items()}
    assert got == want


# --- дашборд Grafana ----------------------------------------------------------------------------------------

DASHBOARD = ROOT / "deploy" / "grafana" / "dashboards" / "vq.json"


@pytest.fixture(scope="module")
def dashboard() -> dict[str, Any]:
    return json.loads(DASHBOARD.read_text(encoding="utf-8"))


def test_dashboard_is_what_gen_dashboard_renders() -> None:
    """vq.json правится только через tools/gen_dashboard.py: ручная правка JSON разойдётся с генерацией."""
    assert DASHBOARD.read_text(encoding="utf-8") == load_tool("gen_dashboard").render()


def exprs(dash: dict[str, Any]) -> Iterator[tuple[str, str]]:
    for p in dash["panels"]:
        for t in p.get("targets", []):
            yield p["title"], t["expr"]


def enclosing_funcs(expr: str) -> Iterator[tuple[str, str | None]]:
    """(метрика, ближайшая охватывающая функция) для каждого упоминания vq_* в выражении."""
    stack: list[str | None] = []
    i = 0
    while i < len(expr):
        m = re.match(r"[A-Za-z_:][A-Za-z0-9_:]*", expr[i:])
        if m:
            word = m.group(0)
            j = i + len(word)
            k = j
            while k < len(expr) and expr[k] == " ":
                k += 1
            if k < len(expr) and expr[k] == "(":
                stack.append(word)
                i = k + 1
                continue
            if word.startswith("vq_"):
                yield word, next((f for f in reversed(stack) if f is not None), None)
            i = j
            continue
        if expr[i] == "(":
            stack.append(None)
        elif expr[i] == ")":
            stack.pop()
        elif expr[i] in "{[":  # метки и диапазоны пропускаются целиком
            close = "}" if expr[i] == "{" else "]"
            i = expr.index(close, i)
        i += 1


def test_enclosing_funcs_parser() -> None:
    got = list(enclosing_funcs('round(sum(increase(vq_x_total{a="b"}[$__range]))) or vector(0)'))
    assert got == [("vq_x_total", "increase")]
    assert list(enclosing_funcs('sum(vq_x_total{result="not_newer"}) - sum(vq_gen_y_total)')) == [
        ("vq_x_total", "sum"),
        ("vq_gen_y_total", "sum"),
    ]


def test_dispatcher_counters_only_through_increase_or_rate(dashboard: dict[str, Any]) -> None:
    """§5.2: счётчики dispatcher сбрасываются при рестарте — графики только на increase()/rate()."""
    bad = []
    for title, expr in exprs(dashboard):
        for metric, func in enclosing_funcs(expr):
            counter = metric.endswith(("_total", "_bucket", "_sum", "_count"))
            if counter and not metric.startswith("vq_gen_") and func not in COUNTER_FUNCS:
                bad.append((title, metric, func))
    assert not bad, bad


def test_per_instance_gauges_follow_partition_owner(dashboard: dict[str, Any]) -> None:
    """Замершие датчики standby, потерявшего партицию, и нули до claim (vq_epoch = 0) не попадают на панели."""
    owner = "and on(instance) ((vq_epoch == scalar(max(vq_epoch))) > 0)"
    for title, expr in exprs(dashboard):
        for gauge in ("vq_queue_length", "vq_consumer_lag_messages", "vq_event_time_lag_seconds"):
            if gauge in expr:
                assert owner in expr, (title, expr)


# Датчик без меток prometheus_client отдаёт нулём с запуска /metrics, задолго до первого значения: у dispatcher —
# до claim (standby — всё время, пока жив активный) и до первой пачки на пустом состоянии, у генератора — до первого
# тика. Такой ноль — не данные: в окне перехвата (session.timeout.ms после остановки активного) панели пусты, а не
# «сейчас» 1970-01-01, 57 лет с чекпоинта, лаг 0 и отставание 1,8·10⁹ с, растягивающее ось на весь период.
# Панели считает Instant — подмножество PromQL; его ответы на этих сценариях сверены с promtool 3.15.0.

PROMQL_TOKEN = re.compile(r"\d+(?:\.\d+)?|[A-Za-z_:][A-Za-z0-9_:]*|==|[-*(),>]")
Labels = frozenset[tuple[str, str]]
Vector = dict[Labels, float]
STATE_GAUGES = {
    "vq_event_time_seconds",
    "vq_event_time_lag_seconds",
    "vq_last_checkpoint_unixtime",
    "vq_consumer_lag_messages",
    "vq_queue_length",
    "vq_gen_sim_time_seconds",
}
GAUGE_PANELS = (
    "«Сейчас» по времени событий A",
    "С последнего чекпоинта A",
    "Длина очереди A",
    "Лаг консьюмера, сообщений A",
    "Отставание по времени событий, с A",
    "Отставание по времени событий, с B",
)
EVENT_TS = 1_789_473_600  # «сейчас» последней пачки: 2026-09-15T12:00:00Z
SCRAPED, QUERIED = EVENT_TS + 20.0, EVENT_TS + 30.0  # часы при опросе /metrics и при запросе панели


def _no_name(labels: Labels) -> Labels:
    return frozenset(p for p in labels if p[0] != "__name__")


def _arith(lhs: float | Vector, rhs: float | Vector, op: Callable[[float, float], float]) -> float | Vector:
    """Арифметика PromQL: имя метрики уходит, вектор с вектором — один к одному по остальным меткам."""
    if isinstance(lhs, dict):
        if isinstance(rhs, dict):
            right = {_no_name(k): v for k, v in rhs.items()}
            return {_no_name(k): op(v, right[_no_name(k)]) for k, v in lhs.items() if _no_name(k) in right}
        return {_no_name(k): op(v, rhs) for k, v in lhs.items()}
    if isinstance(rhs, dict):
        return {_no_name(k): op(lhs, v) for k, v in rhs.items()}
    return op(lhs, rhs)


class Instant:
    """Мгновенный запрос на подмножестве PromQL панелей с датчиками: метрика без меток, число, time(), scalar(),
    max [by (…)], * и -, == и > без bool, and on (…). Как в Prometheus: сравнение с числом фильтрует вектор, max
    пропускает NaN, scalar() не одного элемента — NaN. Пустой вектор — «No data» в Grafana."""

    LEVELS = (("and",), ("==", ">"), ("-",), ("*",))  # приоритет растёт, как в PromQL

    def __init__(self, series: Vector, now: float) -> None:
        self.series, self.now = series, now
        self.toks: list[str] = []

    def __call__(self, expr: str) -> float | Vector:
        self.toks = PROMQL_TOKEN.findall(expr)
        assert "".join(self.toks) == "".join(expr.split()), f"вне подмножества: {expr}"
        value = self.binary(0)
        assert not self.toks, f"не разобрано {self.toks}: {expr}"
        return value

    def take(self, want: str | None = None) -> str:
        tok = self.toks.pop(0)
        assert want in (None, tok), f"ждали {want!r}, а не {tok!r}"
        return tok

    def labels(self) -> set[str]:
        self.take("(")
        names = set()
        while (tok := self.take()) != ")":
            if tok != ",":
                names.add(tok)
        return names

    def vector(self) -> Vector:
        """Аргумент функции в скобках."""
        self.take("(")
        value = self.binary(0)
        self.take(")")
        assert isinstance(value, dict)
        return value

    def binary(self, level: int) -> float | Vector:
        if level == len(self.LEVELS):
            return self.operand()
        lhs = self.binary(level + 1)
        while self.toks and self.toks[0] in self.LEVELS[level]:
            op = self.take()
            if op == "and":
                self.take("on")
                on = self.labels()
                rhs = self.binary(level + 1)
                assert isinstance(lhs, dict) and isinstance(rhs, dict)
                keys = {frozenset(p for p in k if p[0] in on) for k in rhs}
                lhs = {k: v for k, v in lhs.items() if frozenset(p for p in k if p[0] in on) in keys}
            elif op in ("==", ">"):
                rhs = self.binary(level + 1)
                assert isinstance(lhs, dict) and isinstance(rhs, float)  # вектор с числом, без bool — фильтр
                lhs = {k: v for k, v in lhs.items() if (v == rhs if op == "==" else v > rhs)}
            else:
                lhs = _arith(lhs, self.binary(level + 1), operator.sub if op == "-" else operator.mul)
        return lhs

    def operand(self) -> float | Vector:
        tok = self.take()
        if tok == "(":
            value = self.binary(0)
            self.take(")")
            return value
        if tok[0].isdigit():
            return float(tok)
        if tok == "time":
            self.take("(")
            self.take(")")
            return self.now
        if tok == "scalar":
            arg = self.vector()
            return next(iter(arg.values())) if len(arg) == 1 else math.nan
        if tok == "max":
            by: set[str] = set()
            if self.toks[0] == "by":
                self.take()
                by = self.labels()
            out: Vector = {}
            for k, v in self.vector().items():
                group = frozenset(p for p in k if p[0] in by)
                if group not in out or math.isnan(out[group]) or v > out[group]:
                    out[group] = v
            return out
        return {k: v for k, v in self.series.items() if ("__name__", tok) in k}


def test_instant_query_subset() -> None:
    a, b = frozenset({("__name__", "m"), ("instance", "a")}), frozenset({("__name__", "m"), ("instance", "b")})
    q = Instant({a: 1.0, b: math.nan}, now=100.0)
    assert q("max(m)") == {frozenset(): 1.0}  # NaN не побеждает
    assert math.isnan(q("scalar(m)"))  # не один элемент
    assert q("m > 0") == {a: 1.0}  # фильтр, метки на месте
    assert q("time() - max(m > 5)") == {}  # пустой вектор в арифметике — пусто, «No data»
    assert q("(m * 2) and on(instance) (m == scalar(max(m)))") == {_no_name(a): 2.0}
    with pytest.raises(AssertionError, match="вне подмножества"):
        q('m{instance="a"}')


def scrape(registry: CollectorRegistry, job: str, instance: str) -> Vector:
    """Серии одного /metrics, как их хранит Prometheus: с метками job и instance."""
    return {
        frozenset({**s.labels, "__name__": s.name, "job": job, "instance": instance}.items()): s.value
        for family in registry.collect()
        for s in family.samples
    }


def dispatcher_at(epoch: int | None, batch: bool = True) -> CollectorRegistry:
    """Настоящие метрики dispatcher: до claim (epoch=None), после claim пустого состояния (batch=False — пачки ещё
    не было) или после пачки: «сейчас» EVENT_TS, чекпоинт в SCRAPED, лаг 7, очередь S1 из двух машин."""
    registry = CollectorRegistry()
    metrics = DispatcherMetrics(registry=registry, clock=lambda: SCRAPED)
    if epoch is not None:
        metrics.on_claim(epoch, SiteState())
        if batch:
            metrics.observe_batch([event("queue_published", station="S1", length=2)], 0.01, 0.002, EVENT_TS)
            metrics.observe_lag(7)
    return registry


def generator_at(sim_time: int | None) -> CollectorRegistry:
    """Настоящие метрики генератора: до первого тика (None) или после тика с этим временем симуляции."""
    registry = CollectorRegistry()
    metrics = GeneratorMetrics(registry)
    if sim_time is not None:
        metrics.sync(
            produced=1, faults={}, expected_drops=0, received=0, applied=0, ignored={}, delays=(), sim_time=sim_time
        )
    return registry


def gauge_panels(dash: dict[str, Any], *scrapes: Vector) -> dict[str, list[float]]:
    """Значения панелей с датчиками STATE_GAUGES в момент QUERIED; [] — «No data»."""
    query = Instant({k: v for s in scrapes for k, v in s.items()}, QUERIED)
    got = {}
    for p in dash["panels"]:
        for t in p.get("targets", []):
            if STATE_GAUGES & set(PROMQL_TOKEN.findall(t["expr"])):
                value = query(t["expr"])
                assert isinstance(value, dict), t["expr"]
                got[f"{p['title']} {t['refId']}"] = sorted(value.values())
    return got


def test_gauge_panels_are_empty_while_no_dispatcher_has_claimed(dashboard: dict[str, Any]) -> None:
    """Окно перехвата: активный остановлен (его серии устарели), standby ещё не забрал партицию (vq_epoch = 0)."""
    standby = scrape(dispatcher_at(None), "dispatcher", "dispatcher-b:9100")
    generator = scrape(generator_at(EVENT_TS + 5), "generator", "generator:9101")
    assert gauge_panels(dashboard, standby, generator) == {k: [] for k in GAUGE_PANELS}


def test_gauge_panels_show_the_owner_and_skip_the_unclaimed_standby(dashboard: dict[str, Any]) -> None:
    owner = scrape(dispatcher_at(3), "dispatcher", "dispatcher:9100")
    standby = scrape(dispatcher_at(None), "dispatcher", "dispatcher-b:9100")
    generator = scrape(generator_at(EVENT_TS + 5), "generator", "generator:9101")
    assert gauge_panels(dashboard, owner, standby, generator) == {
        "«Сейчас» по времени событий A": [EVENT_TS * 1000.0],
        "С последнего чекпоинта A": [QUERIED - SCRAPED],
        "Длина очереди A": [2.0],
        "Лаг консьюмера, сообщений A": [7.0],
        "Отставание по времени событий, с A": [SCRAPED - EVENT_TS],
        "Отставание по времени событий, с B": [5.0],
    }


def test_generator_lag_is_empty_before_the_first_tick(dashboard: dict[str, Any]) -> None:
    owner = scrape(dispatcher_at(3), "dispatcher", "dispatcher:9100")
    unticked = scrape(generator_at(None), "generator", "generator:9101")
    assert gauge_panels(dashboard, owner, unticked)["Отставание по времени событий, с B"] == []  # не −1,8·10⁹ с


def test_now_and_checkpoint_are_empty_after_a_fresh_claim_before_the_first_batch(dashboard: dict[str, Any]) -> None:
    fresh = scrape(dispatcher_at(1, batch=False), "dispatcher", "dispatcher:9100")
    generator = scrape(generator_at(EVENT_TS + 5), "generator", "generator:9101")
    got = gauge_panels(dashboard, fresh, generator)
    now_panels = ("«Сейчас» по времени событий A", "С последнего чекпоинта A", "Отставание по времени событий, с B")
    assert {k: got[k] for k in now_panels} == {k: [] for k in now_panels}


def test_dashboard_structure(dashboard: dict[str, Any]) -> None:
    assert dashboard["uid"] == "vq"
    ids = [p["id"] for p in dashboard["panels"]]
    assert ids == list(range(1, len(ids) + 1))
    cells: set[tuple[int, int]] = set()
    for p in dashboard["panels"]:
        g = p["gridPos"]
        assert g["x"] + g["w"] <= 24, p["title"]
        own = {(x, y) for x in range(g["x"], g["x"] + g["w"]) for y in range(g["y"], g["y"] + g["h"])}
        assert not cells & own, f"перекрытие: {p['title']}"
        cells |= own
        for t in p.get("targets", []):
            assert t["datasource"] == {"type": "prometheus", "uid": "prom"}, p["title"]
    ds = (ROOT / "deploy/grafana/provisioning/datasources/prometheus.yml").read_text()
    assert "uid: prom" in ds


# --- tools/e2e.py: чистые части (живой прогон — make e2e / make e2e-chaos) ----------------------------------


E2E = load_tool("e2e")
T0 = datetime(2026, 10, 14, 7, 0, 0, tzinfo=UTC)


def ms(n: int) -> datetime:
    return T0 + timedelta(milliseconds=n)


def test_container_and_log_times_parse_to_utc() -> None:
    assert E2E.parse_time("2026-10-14T07:00:01.123456789Z") == ms(1123) + timedelta(microseconds=456)
    assert E2E.parse_time("2026-10-14T07:00:01.5Z") == ms(1500)
    assert E2E.parse_time("2026-10-14T07:00:01.250Z") == ms(1250)  # ts строки лога dispatcher (vq.logs)
    assert E2E.parse_time("2026-10-14T10:00:01.250+03:00") == ms(1250)  # со смещением
    assert E2E.parse_time("0001-01-01T00:00:00Z") is None  # FinishedAt контейнера, который ещё не завершался
    assert E2E.parse_time("") is None and E2E.parse_time(None) is None


def test_crash_moments_are_seeded() -> None:
    arm, kill = E2E.crash_schedule(42, 180.0)
    assert arm == pytest.approx(45.0)  # 25 % прогона
    assert 0.55 * 180 <= kill <= 0.65 * 180  # около 60 %, момент из random.Random(seed)
    assert E2E.crash_schedule(42, 180.0) == (arm, kill)
    assert E2E.crash_schedule(7, 180.0)[1] != kill


def test_last_json_line_of_tool_output() -> None:
    assert E2E.last_json('сводка\nещё строка\n{"ok": true}\n\n') == {"ok": True}
    assert E2E.last_json('{"ok": true}\nхвост не JSON') is None
    assert E2E.last_json("[1, 2]") is None and E2E.last_json("") is None


def test_generator_summary_from_its_log() -> None:
    """Файла out/summary.json нет — итог генератора берётся из события summary его лога (§4.4)."""
    events = [{"event": "start"}, {"event": "summary", "summary": {"produced": 3}}, {"event": "stop"}]
    assert E2E.logged_summary(events) == {"produced": 3}
    assert E2E.logged_summary([{"event": "start"}, {"event": "stop"}]) is None
    assert E2E.logged_summary([{"event": "summary", "summary": [1]}]) is None


def sample(t: int, epoch: int, nxt: int | None, upd: int) -> Any:
    return E2E.PgSample(at=ms(t), epoch=epoch, next_offset=nxt, updated_at=ms(upd))


def test_claim_and_first_checkpoint_from_postgres_polls() -> None:
    polls = [
        sample(0, 3, 1000, -100),
        sample(50, 3, 1000, -100),  # процесс уже мёртв
        sample(800, 4, 1000, 780),  # claim: epoch + 1, next_offset прежний, updated_at — момент claim
        sample(850, 4, 1000, 780),
        sample(900, 4, 1500, 870),  # первый чекпоинт после claim
        sample(950, 4, 1700, 940),
    ]
    got = E2E.after_claim(polls, epoch_before=3, claim_offset=1000)
    assert got == (ms(800), ms(780), ms(870))
    # опрос пропустил момент claim: первый же опрос с новым epoch видит чекпоинт
    got = E2E.after_claim([polls[0], polls[4], polls[5]], epoch_before=3, claim_offset=1000)
    assert got == (ms(900), None, ms(870))
    assert E2E.after_claim(polls[:2], epoch_before=3, claim_offset=1000) == (None, None, None)
    # чекпоинт ещё не случился
    assert E2E.after_claim(polls[:4], epoch_before=3, claim_offset=1000) == (ms(800), ms(780), None)


def crash(kind: str = "kill", **kw: Any) -> Any:
    base = {
        "kind": kind,
        "planned_s": 108.0,
        "restarts_before": 1,
        "epoch_before": 4,
        "restarts_after": 2,
        "exit_code": 137,
        "exited": ms(0),
        "started": ms(350),
        "claimed": ms(800),
        "claim_offset": 70_000,
        "epoch_after": 5,
        "epoch_seen": ms(830),
        "checkpoint": ms(900),
        "records": 60 if kind == "crash_file" else None,
    }
    base.update(kw)
    return E2E.Crash(**base)


def test_recovery_is_measured_from_container_exit() -> None:
    assert E2E.recovery(crash()) == {"restart_s": 0.35, "claim_s": 0.8, "epoch_seen_s": 0.83, "checkpoint_s": 0.9}
    assert E2E.recovery(crash(exited=None))["claim_s"] is None


def test_recovery_table_has_a_row_per_crash() -> None:
    lines = E2E.recovery_table([crash("crash_file", planned_s=45.0, restarts_before=0, restarts_after=1), crash()])
    assert lines[0].startswith("| Авария") and set(lines[1]) <= {"|", "-", " "}
    assert len(lines) == 4
    assert "VQ_CRASH_FILE" in lines[2] and "| 60 |" in lines[2] and "0 → 1" in lines[2]
    assert "kill -KILL -1" in lines[3] and "0.80" in lines[3] and "0.90" in lines[3]


def good_report() -> dict[str, Any]:
    return {
        "ok": True,
        "n": 150_000,
        "log": {
            "kept": {"queue.v1": 50_000, "decision.v1": 100},
            "replays": {"queue.v1": 0, "decision.v1": 0},
        },
        "mismatches": {"total": 0, "by_kind": {}, "first": []},
        "counters": {"ok": True, "diff": []},
        "offline": {
            "fsm_resync": 0,
            "decisions": {"recommended": 30, "no_gain": 70, "stale_telemetry": 0},
            "occupations": {"total": 500, "per_unit_min": 9, "per_unit_max": 16, "units_without": []},
        },
        "invariants": {"ok": True, "violations": 0, "first": []},
    }


def good_facts(chaos: bool = False) -> Any:
    summary = {
        "produced": 150_000,
        "expected_drops": 600,
        "decisions": {"received": 100, "applied": 30, "ignored": {"already_serving": 0, "stale_trip": 0}},
        "error": None,
        "stopped_by": "duration",
        "ticks": 3600,
    }
    facts = E2E.Facts(
        chaos=chaos,
        gen_rc=0,
        summary=summary,
        hwm=150_000,
        processor={"epoch": 1, "next_offset": 150_000, "msgs_not_newer": 600},
        waited_s=1.5,
        verify_rc=0,
        report=good_report(),
        restarts_before=0,
        restarts_after=0,
    )
    if chaos:
        facts.crashes = [
            crash("crash_file", planned_s=45.0, restarts_before=0, restarts_after=1, epoch_before=1, epoch_after=2),
            crash("kill", restarts_before=1, restarts_after=2, epoch_before=2, epoch_after=3),
        ]
        facts.restarts_after = 2
        facts.processor["epoch"] = 3
        facts.report["log"]["replays"] = {"queue.v1": 55, "decision.v1": 5}
    return facts


def failing(facts: Any) -> list[str]:
    return [c.key for c in E2E.evaluate(facts) if not c.ok]


def test_good_run_passes_every_check() -> None:
    plain = E2E.evaluate(good_facts())
    assert [c.key for c in plain] == [
        "generator",
        "hwm",
        "caught_up",
        "verify",
        "drops",
        "already_serving",
        "fsm_resync",
        "decision_kinds",
        "occupations",
        "invariants",
        "restarts",
    ]
    assert all(c.ok for c in plain), [c for c in plain if not c.ok]
    chaos = E2E.evaluate(good_facts(chaos=True))
    assert [c.key for c in chaos][-6:] == ["restarts", "crash_file", "kill", "replays", "replays_identical", "epoch"]
    assert all(c.ok for c in chaos), [c for c in chaos if not c.ok]


def _set(path: str, value: Any) -> Any:
    def mutate(f: Any) -> None:
        *head, last = path.split(".")
        obj = f
        for p in head:
            obj = obj[p] if isinstance(obj, dict) else getattr(obj, p)
        if isinstance(obj, dict):
            obj[last] = value
        else:
            setattr(obj, last, value)

    return mutate


@pytest.mark.parametrize(
    ("mutate", "keys"),
    [
        (_set("gen_rc", 1), ["generator"]),
        (_set("summary.error", "KafkaException: boom"), ["generator"]),
        (_set("hwm", 149_999), ["hwm", "caught_up"]),
        (_set("processor.next_offset", 149_000), ["caught_up"]),
        (_set("verify_rc", 1), ["verify"]),
        (_set("report.ok", False), ["verify"]),
        (_set("processor.msgs_not_newer", 601), ["drops"]),
        (_set("summary.decisions.ignored.already_serving", 1), ["already_serving"]),
        (_set("report.offline.fsm_resync", 2), ["fsm_resync"]),
        (_set("report.offline.decisions.no_gain", 0), ["decision_kinds"]),
        (_set("report.offline.decisions.recommended", 0), ["decision_kinds"]),
        (_set("report.offline.occupations.per_unit_min", 0), ["occupations"]),
        (_set("report.invariants.violations", 1), ["invariants"]),
        (_set("restarts_after", 1), ["restarts"]),  # dispatcher упал без сценария аварий
    ],
)
def test_each_check_fails_on_its_own_condition(mutate: Any, keys: list[str]) -> None:
    f = good_facts()
    mutate(f)
    assert failing(f) == keys


def test_missing_verify_report_fails_without_crashing() -> None:
    f = good_facts(chaos=True)
    f.report, f.verify_rc = None, 1
    bad = failing(f)
    assert {"verify", "fsm_resync", "decision_kinds", "occupations", "invariants", "replays"} <= set(bad)
    assert "replays_identical" in bad


@pytest.mark.parametrize(
    ("mutate", "key"),
    [
        (_set("report.log.replays", {"queue.v1": 0, "decision.v1": 0}), "replays"),
        (_set("report.log.replays", {"queue.v1": 30, "decision.v1": 0}), "replays"),  # меньше records точки аварии
        (_set("report.mismatches.by_kind", {"replay_differs": 1}), "replays_identical"),
        (_set("processor.epoch", 2), "epoch"),
        (_set("restarts_after", 3), "restarts"),  # лишний перезапуск — авария вне сценария
    ],
)
def test_chaos_checks_fail_on_their_own_condition(mutate: Any, key: str) -> None:
    f = good_facts(chaos=True)
    mutate(f)
    assert key in failing(f)


def test_chaos_run_without_crashes_fails() -> None:
    f = good_facts(chaos=True)
    f.crashes = []
    assert {"crash_file", "kill", "epoch"} <= set(failing(f))


@pytest.mark.parametrize(
    ("kind", "change"),
    [
        ("crash_file", {"restarts_after": 0}),  # политика не перезапустила контейнер
        ("crash_file", {"records": None}),  # строки chaos_crash нет: точка аварии не сработала
        ("crash_file", {"exit_code": 1}),
        ("kill", {"epoch_after": None}),  # нового claim нет
        ("kill", {"epoch_after": 2}),  # epoch не вырос: строка claim прежнего владельца
        ("kill", {"checkpoint": None}),  # обработка не возобновилась
        ("kill", {"error": "dispatcher не перезапустился за 60 с"}),
    ],
)
def test_crash_check_fails_when_recovery_is_incomplete(kind: str, change: dict[str, Any]) -> None:
    f = good_facts(chaos=True)
    f.crashes = [replace(c, **change) if c.kind == kind else c for c in f.crashes]
    assert kind in failing(f)
