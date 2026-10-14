"""JSON-логи сервисов (§2.4): одна строка JSON на запись, в stdout. Общие для dispatcher, генератора и витрины."""

import json
import logging
import sys
from collections.abc import Mapping
from datetime import UTC, datetime
from typing import Any


def describe(e: BaseException) -> str:
    """Короткий текст ошибки: 'Тип: первая строка сообщения', не длиннее 200 символов; без сообщения — тип."""
    lines = str(e).strip().splitlines()
    return f"{type(e).__name__}: {lines[0]}"[:200] if lines else type(e).__name__


class JsonFormatter(logging.Formatter):
    """Ключи строки по порядку: ts (UTC, миллисекунды, Z), level, logger, event, поля, error.

    Поля события передаются как extra={"fields": {...}} (имена не спорят с атрибутами LogRecord), event — текст
    сообщения. У записи без fields (uvicorn, librdkafka) event = "log", текст — в msg. defaults — поля каждой
    строки (у dispatcher — site, epoch, offset): поля записи меняют их значение, но не место. error — трассировка,
    если запись несёт исключение. Значение, которое JSON не умеет (объект, NaN, ключ-кортеж), пишется текстом.
    """

    def __init__(self, defaults: Mapping[str, object] | None = None) -> None:
        super().__init__()
        self.defaults: Mapping[str, object] = dict(defaults or {})

    def format(self, record: logging.LogRecord) -> str:
        fields = getattr(record, "fields", None)
        body: dict[Any, object] = dict(self.defaults)
        if isinstance(fields, Mapping):
            event = record.getMessage()
            body.update(fields)
        else:
            event = "log"
            body["msg"] = record.getMessage()
        doc: dict[Any, object] = {
            "ts": _utc(record.created),
            "level": record.levelname,
            "logger": record.name,
            "event": event,
        }
        for key, value in body.items():
            doc.setdefault(key, value)  # голову строки поля не перекрывают
        if record.exc_info:
            doc.pop("error", None)  # трассировка полнее текста ошибки и стоит последней
            doc["error"] = self.formatException(record.exc_info)
        try:
            return _dumps(doc)
        except Exception:  # NaN, ключ не строка, цикл, сломанный __str__: такое значение — текстом
            return _dumps({k if isinstance(k, str) else _text(k): _jsonable(v) for k, v in doc.items()})


def _utc(created: float) -> str:
    """Момент записи в UTC с миллисекундами: '2026-09-15T12:00:00.250Z'."""
    return datetime.fromtimestamp(created, UTC).replace(tzinfo=None).isoformat(timespec="milliseconds") + "Z"


def _dumps(obj: object) -> str:
    """Строгий JSON (без NaN и Infinity) в одну строку; неизвестный тип — через str()."""
    return json.dumps(obj, ensure_ascii=False, allow_nan=False, default=str)


def _jsonable(value: object) -> object:
    """Значение, которое _dumps примет: само значение или его текст."""
    try:
        _dumps(value)
    except Exception:
        return _text(value)
    return value


def _text(value: object) -> str:
    """str(value); если __str__ сломан — repr object, он не бросает."""
    try:
        return str(value)
    except Exception:
        return object.__repr__(value)


def setup_logging(level: int = logging.INFO) -> JsonFormatter:
    """JsonFormatter в stdout на корневом логгере; возвращает его, через него сервис задаёт defaults.

    Повторный вызов второй обработчик не ставит: меняет только уровень и возвращает уже установленный форматтер.
    """
    root = logging.getLogger()
    root.setLevel(level)
    for installed in root.handlers:
        if isinstance(installed.formatter, JsonFormatter):
            return installed.formatter
    formatter = JsonFormatter()
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)
    root.addHandler(handler)
    return formatter
