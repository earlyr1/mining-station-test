"""Живая страница (§5.1): GET /api/state и GET /.

build_state — чистая функция от срезов источников: её проверяют тесты без Kafka и Postgres. Приложение только
отдаёт её результат и статическую страницу.
"""

from __future__ import annotations

import argparse
import math
import os
import time
from collections import Counter
from collections.abc import AsyncIterator, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Protocol

import uvicorn
from fastapi import FastAPI
from fastapi.responses import HTMLResponse, JSONResponse

from vq.config import SiteConfig, load_config
from vq.core.decision import DECISION_TOPIC
from vq.core.engine import QUEUE_TOPIC
from vq.geo import fmt_ts
from vq.logs import setup_logging
from vq.viewer.sources import (
    QUEUE_ROW_FIELDS,
    PgData,
    QueueRecord,
    Sample,
    Snapshot,
    Sources,
    SourceSnapshots,
    StreamData,
    bucket_value,
    event_ts,
    metric_value,
)

STATIC_DIR = Path(__file__).parent / "static"
STALE_S = 3.0  # срез старше этого — источник считается недоступным, даже если последняя попытка удалась
ACCURACY_BOUND_S = 5.0  # точность прогноза — доля занятий с |ошибкой| не больше этого


# --- сборка состояния ------------------------------------------------------


def source_status(snap: Snapshot[Any] | None, wall_now: float, stale_s: float = STALE_S) -> dict[str, Any]:
    """Пометка источника для страницы: ok, текст ошибки, возраст последнего удачного чтения."""
    if snap is None:
        return {"ok": False, "error": "источник не настроен", "age_s": None, "has_data": False}
    age = None if snap.checked_at is None else round(max(0.0, wall_now - snap.checked_at), 1)
    ok = snap.ok and age is not None and age <= stale_s
    error = snap.error
    if snap.ok and not ok:
        error = f"нет обновлений {age} сек"
    return {"ok": ok, "error": error, "age_s": age, "has_data": snap.data is not None}


def _int(value: Any) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def _finite_int(value: Any) -> int | None:
    """Число из /metrics → int; None, если его нет или оно не конечное (NaN из чужого /metrics не роняет страницу)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        return None
    if isinstance(value, float) and not math.isfinite(value):
        return None
    return int(value)


def json_safe(obj: Any) -> Any:
    """Не конечные float → None. JSONResponse сериализует с allow_nan=False, и одно такое число из любого источника
    (запись топика, строка Postgres, /metrics) иначе превратило бы весь /api/state в 500 — страница погасла бы."""
    if isinstance(obj, float):
        return obj if math.isfinite(obj) else None
    if isinstance(obj, dict):
        return {k: json_safe(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [json_safe(v) for v in obj]
    return obj


def _stream_now(stream: StreamData | None) -> int | None:
    """Самое позднее «сейчас», видимое в топиках: наибольший at среди очередей и решений."""
    if stream is None:
        return None
    stamps = [event_ts(q.at) for q in stream.queues.values()] + [event_ts(d.at) for d in stream.decisions[-1:]]
    known = [s for s in stamps if s is not None]
    return max(known) if known else None


def _queue_card(cfg: SiteConfig, rec: QueueRecord | None) -> dict[str, Any] | None:
    if rec is None:
        return None
    rows = []
    for row in rec.rows:
        out = {k: row.get(k) for k in QUEUE_ROW_FIELDS}
        unit = row.get("unit_uuid")
        out["home"] = cfg.assignment.get(unit) if isinstance(unit, str) else None
        rows.append(out)
    return {"at": rec.at, "src_offset": rec.src_offset, "rows": rows}


def _stations(cfg: SiteConfig, pg: PgData | None, stream: StreamData | None) -> list[dict[str, Any]]:
    """Станции: координаты и закрепление — из конфигурации, занявшая — из Postgres, очередь — из queue.v1."""
    by_uuid = {s.get("station_uuid"): s for s in pg.stations} if pg is not None else {}
    assigned = Counter(cfg.assignment.values())
    out = []
    for sc in cfg.stations:
        row = by_uuid.get(sc.uuid) or {}
        occupant, occupied_at = row.get("occupant_uuid"), _int(row.get("occupied_at"))
        free_at = occupied_at + cfg.constants.occupy_s if occupant is not None and occupied_at is not None else None
        out.append(
            {
                "uuid": sc.uuid,
                "lat": sc.lat,
                "lon": sc.lon,
                "assigned": assigned.get(sc.uuid, 0),
                "occupant": occupant,
                "occupied_at": occupied_at,
                "free_at": free_at,
                "last_release_at": _int(row.get("last_release_at")),
                "queue": _queue_card(cfg, stream.queues.get(sc.uuid) if stream is not None else None),
            }
        )
    return out


def _vehicle(v: Mapping[str, Any], pg_now: int | None, fresh_s: int) -> dict[str, Any]:
    last_ts = _int(v.get("last_ts"))
    age = None if pg_now is None or last_ts is None else pg_now - last_ts
    return {
        "unit": v.get("unit_uuid"),
        "home": v.get("home_station"),
        "target": v.get("target_station"),
        "phase": v.get("phase"),
        "lat": v.get("lat"),
        "lon": v.get("lon"),
        "speed_kmh": v.get("speed_kmh"),
        "last_ts": last_ts,
        "age_s": age,
        "fresh": age is not None and age <= fresh_s,
        "redirected": v.get("target_station") != v.get("home_station"),
        "arrived_at": v.get("arrived_at"),
    }


def _prediction(samples: Sequence[Sample]) -> dict[str, Any] | None:
    """Точность прогноза (§5.1): число занятий и доля |ошибки| ≤ 5 с — по корзине le=5 гистограммы модуля ошибки
    dispatcher. Квантили и знак ошибки — в Grafana (§5.2)."""
    total = metric_value(samples, "vq_prediction_abs_error_seconds_count")
    if total is None:
        return None
    count = _finite_int(total) or 0
    within = bucket_value(samples, "vq_prediction_abs_error_seconds", ACCURACY_BOUND_S)
    share = within / total if count > 0 and within is not None and math.isfinite(within) else None
    return {"count": count, "within_5s": None if share is None else round(share, 4)}


def _decisions_by_type(samples: Sequence[Sample]) -> dict[str, int]:
    def total(**labels: str) -> int:
        return _finite_int(metric_value(samples, "vq_decisions_total", **labels)) or 0

    return {
        "recommended": total(result="recommended"),
        "no_gain": total(reason="no_gain"),
        "stale_telemetry": total(reason="stale_telemetry"),
    }


def _dispatcher_health(samples: Sequence[Sample] | None) -> dict[str, Any]:
    """Решения по типам, точность прогноза, самолечение и лаг — из /metrics dispatcher (§5.1)."""
    if samples is None:
        return {"decisions": None, "prediction": None, "resync": None, "consumer_lag": None}
    return {
        "decisions": _decisions_by_type(samples),
        "prediction": _prediction(samples),
        "resync": _finite_int(metric_value(samples, "vq_fsm_resync_total")),
        "consumer_lag": _finite_int(metric_value(samples, "vq_consumer_lag_messages")),
    }


def _expected_drops(snaps: SourceSnapshots, statuses: Mapping[str, Mapping[str, Any]]) -> tuple[int | None, str | None]:
    """Ожидаемые отбросы: из /metrics работающего генератора, иначе из summary.json прошедшего прогона."""
    if statuses["generator"]["ok"] and snaps.generator is not None and snaps.generator.data is not None:
        value = _finite_int(metric_value(snaps.generator.data, "vq_gen_expected_drops_total"))
        if value is not None:
            return value, "generator"
    if snaps.summary is not None and snaps.summary.data is not None:
        value = snaps.summary.data.get("expected_drops")
        if isinstance(value, int) and not isinstance(value, bool):
            return value, "summary"
    return None, None


def _offset_text(x: int | None) -> str:
    return "—" if x is None else str(x)


def offset_marks(queues_offset: int | None, map_offset: int | None) -> dict[str, Any]:
    """Отметки «очереди / карта» (§5.1).

    queue.v1 публикуется только при изменении очереди, поэтому X — offset сообщения telemetry.v1, которое последним
    изменило какую-то очередь, а не offset, на котором очереди актуальны: в установившемся режиме X < Y — норма
    (очереди с тех пор не менялись). X > Y — очереди уже опубликованы, а чекпоинт с картой ещё нет (dispatcher
    отправляет выходы до чекпоинта, разрыв — не больше пачки).
    """
    ahead = None if queues_offset is None or map_offset is None else max(0, queues_offset - map_offset)
    text = f"очереди — последнее изменение на offset {_offset_text(queues_offset)}"
    text += f", карта на offset {_offset_text(map_offset)}"
    if ahead:  # в конце: узкая шапка карты обрезает текст справа, обе отметки должны остаться видны
        text += f"; очереди впереди карты на {ahead}"
    return {"queues_offset": queues_offset, "map_offset": map_offset, "queues_ahead": ahead, "text": text}


def _stream_health(stream: StreamData) -> dict[str, Any]:
    return {
        "decision_totals": dict(stream.decision_totals),
        "repeats": {t: stream.repeats.get(t, 0) for t in (QUEUE_TOPIC, DECISION_TOPIC)},
        "lag": {t: max(0, stream.high[t] - stream.next_offset.get(t, 0)) for t in stream.high},
        "malformed": stream.malformed,
        "resets": stream.resets,
    }


def _health(
    snaps: SourceSnapshots,
    statuses: Mapping[str, Mapping[str, Any]],
    stream: StreamData | None,
    proc: Mapping[str, Any] | None,
    wall_now: float,
) -> dict[str, Any]:
    """Полоса здоровья (§5.1): строка processor (точные счётчики), ожидаемые отбросы генератора, метрики
    dispatcher, отметки offset и свёртка топиков."""
    p: Mapping[str, Any] = proc or {}
    counters = None
    if proc is not None:
        counters = {
            "accepted": _int(p.get("msgs_accepted")),
            "not_newer": _int(p.get("msgs_not_newer")),
            "invalid": _int(p.get("msgs_invalid")),
        }
    expected, expected_source = _expected_drops(snaps, statuses)
    not_newer = None if counters is None else counters["not_newer"]
    next_offset = _int(p.get("next_offset"))
    updated_epoch = p.get("updated_at_epoch")
    queues_offset = None if stream is None else stream.max_src_offset.get(QUEUE_TOPIC)
    return {
        "epoch": _int(p.get("epoch")),
        "next_offset": next_offset,
        "updated_at": p.get("updated_at"),
        "updated_age_s": None if updated_epoch is None else round(max(0.0, wall_now - updated_epoch), 1),
        "logic_version": p.get("logic_version"),
        "counters": counters,
        "expected_drops": expected,
        "expected_drops_source": expected_source,
        "drops_match": None if expected is None or not_newer is None else expected == not_newer,
        **_dispatcher_health(None if snaps.dispatcher is None else snaps.dispatcher.data),
        "marks": offset_marks(queues_offset, next_offset - 1 if next_offset is not None and next_offset > 0 else None),
        "stream": None if stream is None else _stream_health(stream),
    }


def build_state(cfg: SiteConfig, snaps: SourceSnapshots, wall_now: float) -> dict[str, Any]:
    """JSON для /api/state (§5.1) из срезов источников. Без I/O и часов: wall_now передаётся снаружи.

    Результат сериализуется строго (без NaN и Infinity): не конечные числа источников становятся null.
    """
    c = cfg.constants
    sources = {
        "kafka": snaps.kafka,
        "postgres": snaps.pg,
        "dispatcher": snaps.dispatcher,
        "generator": snaps.generator,
        "summary": snaps.summary,
    }
    statuses = {name: source_status(snap, wall_now) for name, snap in sources.items()}
    stream = None if snaps.kafka is None else snaps.kafka.data
    pg = None if snaps.pg is None else snaps.pg.data
    proc = None if pg is None else pg.processor
    pg_now = None if proc is None else _int(proc.get("now_ts"))
    known_now = [x for x in (pg_now, _stream_now(stream)) if x is not None]
    now = max(known_now) if known_now else None
    decisions = (
        [] if stream is None else [{**d.payload, "src_offset": d.src_offset} for d in reversed(stream.decisions)]
    )
    state: dict[str, Any] = json_safe(
        {
            "now": now,
            "now_iso": None if now is None else fmt_ts(now),
            "map_now": pg_now,
            "site": {
                "id": cfg.site_id,
                "dump": {"lat": cfg.dump.lat, "lon": cfg.dump.lon},
                "radius_m": c.radius_m,
                "decision_radius_m": c.decision_radius_m,
                "fresh_s": c.fresh_s,
                "occupy_s": c.occupy_s,
            },
            "stations": _stations(cfg, pg, stream),
            "vehicles": [] if pg is None else [_vehicle(v, pg_now, c.fresh_s) for v in pg.vehicles],
            "decisions": decisions,
            "health": _health(snaps, statuses, stream, proc, wall_now),
            "sources": statuses,
        }
    )
    return state


# --- приложение -----------------------------------------------------------


class SourceSet(Protocol):
    def start(self) -> None: ...
    def stop(self) -> None: ...
    def snapshots(self) -> SourceSnapshots: ...


def create_app(cfg: SiteConfig, sources: SourceSet, clock: Callable[[], float] = time.time) -> FastAPI:
    """Приложение витрины. Потоки источников запускаются и останавливаются в lifespan приложения."""

    @asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        sources.start()
        try:
            yield
        finally:
            sources.stop()

    app = FastAPI(title="vq-viewer", lifespan=lifespan, docs_url=None, redoc_url=None, openapi_url=None)

    @app.get("/api/state")
    def state() -> JSONResponse:
        body = build_state(cfg, sources.snapshots(), clock())
        return JSONResponse(body, headers={"Cache-Control": "no-store"})

    @app.get("/", response_class=HTMLResponse)
    def index() -> HTMLResponse:
        # Файл читается на каждый запрос: правка страницы видна без рестарта, размер небольшой.
        html = (STATIC_DIR / "index.html").read_text(encoding="utf-8")
        return HTMLResponse(html, headers={"Cache-Control": "no-cache"})

    return app


def main(argv: Sequence[str] | None = None) -> None:
    parser = argparse.ArgumentParser(prog="vq-viewer", description="Живая страница виртуальных очередей")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8000, help="порт внутри контейнера; на хосте — VQ_VIEWER_PORT")
    args = parser.parse_args(argv)
    setup_logging()
    cfg = load_config(os.environ.get("VQ_CONFIG", "config/site.toml"))
    app = create_app(cfg, Sources.from_env(cfg.site_id))
    # log_config=None: uvicorn не ставит своих обработчиков, его строки идут в корневой логгер тем же JSON. Журнал
    # доступа выключен: страница опрашивает /api/state дважды в секунду с каждой вкладки, и строки запросов заслонили
    # бы source_down и source_up; исключение в обработчике uvicorn пишет в uvicorn.error с трассировкой.
    uvicorn.run(app, host=args.host, port=args.port, log_config=None, access_log=False, log_level="info")


if __name__ == "__main__":
    main()
