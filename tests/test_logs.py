"""Общие JSON-логи сервисов (vq.logs, §2.4): формат строки, порядок ключей, поля, исключение, установка в stdout."""

import json
import logging
import math
import sys
from collections.abc import Iterator
from decimal import Decimal
from pathlib import Path

import pytest

from tests.helpers import NOON
from vq.logs import JsonFormatter, describe, setup_logging


def record(event: str, fields: dict | None = None, *, name: str = "vq.test", args: tuple = (), exc=None):
    rec = logging.LogRecord(name, logging.INFO, __file__, 1, event, args, exc)
    rec.created = NOON + 0.25
    if fields is not None:
        rec.fields = fields
    return rec


def strict(line: str) -> dict:
    """Строка лога — один объект строгого JSON (без NaN и Infinity) и без переводов строки."""

    def no_constants(name: str) -> None:
        raise ValueError(f"не строгий JSON: {name}")

    assert "\n" not in line
    doc = json.loads(line, parse_constant=no_constants)
    assert isinstance(doc, dict)
    return doc


def test_line_is_head_then_fields_in_the_given_order():
    line = JsonFormatter().format(record("claim", {"site": "s1", "epoch": 3, "offset": 40, "low": 0}))
    assert list(strict(line).items()) == [
        ("ts", "2026-09-15T12:00:00.250Z"),
        ("level", "INFO"),
        ("logger", "vq.test"),
        ("event", "claim"),
        ("site", "s1"),
        ("epoch", 3),
        ("offset", 40),
        ("low", 0),
    ]


def test_defaults_are_in_every_line_and_fields_override_them_in_place():
    fmt = JsonFormatter({"site": "s1", "epoch": None, "offset": None})
    doc = strict(fmt.format(record("claim", {"low": 0, "epoch": 3, "offset": 40})))
    assert list(doc)[4:] == ["site", "epoch", "offset", "low"]
    assert (doc["site"], doc["epoch"], doc["offset"], doc["low"]) == ("s1", 3, 40, 0)
    assert strict(fmt.format(record("stop", {})))["epoch"] is None


def test_record_without_fields_is_event_log_with_its_text_in_msg():
    """Чужой логгер (uvicorn, librdkafka): event = log, текст — в msg; его extra (color_message) в строку не идут."""
    rec = record("retry %d", name="urllib3", args=(2,))
    rec.color_message = "\x1b[1mretry\x1b[0m"
    assert strict(JsonFormatter({"site": "s1"}).format(rec)) == {
        "ts": "2026-09-15T12:00:00.250Z",
        "level": "INFO",
        "logger": "urllib3",
        "event": "log",
        "site": "s1",
        "msg": "retry 2",
    }


def test_fields_do_not_override_the_head():
    doc = strict(JsonFormatter().format(record("start", {"event": "x", "ts": 1, "level": "y", "logger": "z", "n": 1})))
    assert doc == {"ts": "2026-09-15T12:00:00.250Z", "level": "INFO", "logger": "vq.test", "event": "start", "n": 1}


def test_exception_goes_last_into_error():
    try:
        raise RuntimeError("sim bug")
    except RuntimeError:
        exc = sys.exc_info()
    doc = strict(JsonFormatter().format(record("run_failed", {"error": "RuntimeError: sim bug", "ticks": 3}, exc=exc)))
    assert list(doc)[-2:] == ["ticks", "error"]
    assert doc["error"].startswith("Traceback (most recent call last):")
    assert doc["error"].endswith("RuntimeError: sim bug")


class Opaque:
    def __str__(self) -> str:
        return "opaque"


class Broken:
    def __str__(self) -> str:
        raise RuntimeError("__str__ сломан")


def test_values_json_cannot_hold_become_text_and_the_line_survives():
    """Значение, которое JSON не умеет, пишется строкой: строка лога не теряется и остаётся строгим JSON."""
    loop: list = []
    loop.append(loop)
    fields = {
        "ok": {"stations": ["S1"], "gain_s": 120},
        "obj": Opaque(),
        "path": Path("/out/summary.json"),
        "dec": Decimal("1.5"),
        "nan": math.nan,
        "inf": [1.0, math.inf],
        "tuple_key": {("S1", "S2"): 1},
        "loop": loop,
        "broken": Broken(),
    }
    doc = strict(JsonFormatter().format(record("decision", fields)))
    assert list(doc)[4:] == list(fields)
    assert doc["ok"] == {"stations": ["S1"], "gain_s": 120}  # остальные поля — как были
    assert (doc["obj"], doc["path"], doc["dec"]) == ("opaque", "/out/summary.json", "1.5")
    assert (doc["nan"], doc["inf"], doc["tuple_key"]) == ("nan", "[1.0, inf]", "{('S1', 'S2'): 1}")
    assert doc["loop"] == "[[...]]"
    assert doc["broken"].startswith("<") and "Broken object" in doc["broken"]


def test_text_stays_readable_and_on_one_line():
    doc = strict(JsonFormatter().format(record("refused_dirty_topic", {"error": "не пуст\nmake reset"})))
    assert doc["error"] == "не пуст\nmake reset"
    assert "не пуст" in JsonFormatter().format(record("x", {"error": "не пуст"}))  # ensure_ascii=False


@pytest.mark.parametrize(
    ("error", "text"),
    [
        (RuntimeError("ядро упало"), "RuntimeError: ядро упало"),
        (OSError("connection refused\n\tIs the server running?"), "OSError: connection refused"),
        (TimeoutError(), "TimeoutError"),
        (ValueError("  \n "), "ValueError"),
        (ValueError("x" * 500), "ValueError: " + "x" * 188),
    ],
)
def test_describe_is_type_and_first_line_at_most_200_chars(error, text):
    assert describe(error) == text
    assert len(describe(error)) <= 200


@pytest.fixture
def root_logger() -> Iterator[logging.Logger]:
    """Корневой логгер pytest после теста — как был: setup_logging меняет его."""
    root = logging.getLogger()
    handlers, level = root.handlers[:], root.level
    yield root
    root.handlers[:] = handlers
    root.setLevel(level)


def test_setup_logging_installs_one_stdout_handler(root_logger, capsys):
    fmt = setup_logging()
    assert setup_logging() is fmt  # повторный вызов второй обработчик не ставит
    ours = [h for h in root_logger.handlers if isinstance(h.formatter, JsonFormatter)]
    assert len(ours) == 1 and ours[0].formatter is fmt and root_logger.level == logging.INFO
    fmt.defaults = {"site": "s1"}
    logging.getLogger("vq.test").info("start", extra={"fields": {"seed": 42}})
    logging.getLogger("vq.test").debug("noise", extra={"fields": {}})
    out = capsys.readouterr().out.splitlines()
    assert [{k: v for k, v in strict(line).items() if k != "ts"} for line in out] == [
        {"level": "INFO", "logger": "vq.test", "event": "start", "site": "s1", "seed": 42}
    ]
    setup_logging(logging.WARNING)
    logging.getLogger("vq.test").info("quiet", extra={"fields": {}})
    assert capsys.readouterr().out == ""
