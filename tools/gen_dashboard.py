"""Генерирует deploy/grafana/dashboards/vq.json: дашборд «Виртуальные очереди» (панели §5.2, метрики §5.2 и §4.6).

JSON дашборда правится только здесь: так панели читаются как код, а тест сверяет файл с генерацией.

    uv run python tools/gen_dashboard.py            # перезаписать vq.json
    uv run python tools/gen_dashboard.py --check    # проверить, что файл совпадает с генерацией
"""

import argparse
import json
import sys
from pathlib import Path
from typing import Any

DS = {"type": "prometheus", "uid": "prom"}
DEFAULT_OUT = Path(__file__).resolve().parent.parent / "deploy" / "grafana" / "dashboards" / "vq.json"


def tgt(expr: str, legend: str = "", ref: str = "A", instant: bool = False, fmt: str = "time_series") -> dict[str, Any]:
    t: dict[str, Any] = {
        "datasource": DS,
        "refId": ref,
        "expr": expr,
        "legendFormat": legend,
        "range": not instant,
        "instant": instant,
    }
    if fmt != "time_series":
        t["format"] = fmt
    return t


def targets(*items: tuple[str, str]) -> list[dict[str, Any]]:
    return [tgt(e, lg, chr(ord("A") + i)) for i, (e, lg) in enumerate(items)]


def instant(*items: tuple[str, str]) -> list[dict[str, Any]]:
    """Мгновенные запросы для stat: значение за весь выбранный период ($__range)."""
    return [tgt(e, lg, chr(ord("A") + i), instant=True) for i, (e, lg) in enumerate(items)]


def owner(expr: str) -> str:
    """Датчики только экземпляра dispatcher, владеющего партицией (наибольший vq_epoch).

    У standby, потерявшего партицию, датчики замирают на старых значениях.
    """
    return f"({expr}) and on(instance) (vq_epoch == scalar(max(vq_epoch)))"


def period(expr: str) -> str:
    """Счётчик за выбранный период: increase() переживает рестарт процесса (§5.2), round — целое число."""
    return f"round(sum(increase({expr}[$__range])))"


def row(title: str, y: int) -> dict[str, Any]:
    return {
        "type": "row",
        "title": title,
        "collapsed": False,
        "panels": [],
        "gridPos": {"h": 1, "w": 24, "x": 0, "y": y},
    }


def thresholds(*steps: tuple[str, float | None]) -> dict[str, Any]:
    return {"mode": "absolute", "steps": [{"color": c, "value": v} for c, v in steps]}


def ts(
    title: str,
    desc: str,
    pos: tuple[int, int, int, int],
    tg: list[dict[str, Any]],
    unit: str = "short",
    interp: str = "linear",
    decimals: int | None = None,
) -> dict[str, Any]:
    x, y, w, h = pos
    defaults: dict[str, Any] = {
        "unit": unit,
        "color": {"mode": "palette-classic"},
        "custom": {
            "drawStyle": "line",
            "lineInterpolation": interp,
            "lineWidth": 1,
            "fillOpacity": 10,
            "showPoints": "never",
            "spanNulls": True,
            "axisSoftMin": 0,
        },
        "thresholds": thresholds(("green", None)),
    }
    if decimals is not None:
        defaults["decimals"] = decimals
    return {
        "type": "timeseries",
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {
            "legend": {"displayMode": "list", "placement": "bottom", "showLegend": True},
            "tooltip": {"mode": "multi", "sort": "none"},
        },
        "targets": tg,
    }


def stat(
    title: str,
    desc: str,
    pos: tuple[int, int, int, int],
    tg: list[dict[str, Any]],
    unit: str = "short",
    th: dict[str, Any] | None = None,
    mappings: list[dict[str, Any]] | None = None,
    decimals: int | None = None,
    color_mode: str = "value",
    text_mode: str = "auto",
) -> dict[str, Any]:
    x, y, w, h = pos
    defaults: dict[str, Any] = {
        "unit": unit,
        "color": {"mode": "thresholds"},
        "thresholds": th or thresholds(("blue", None)),
        "mappings": mappings or [],
    }
    if decimals is not None:
        defaults["decimals"] = decimals
    return {
        "type": "stat",
        "title": title,
        "description": desc,
        "datasource": DS,
        "gridPos": {"h": h, "w": w, "x": x, "y": y},
        "fieldConfig": {"defaults": defaults, "overrides": []},
        "options": {
            "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            "colorMode": color_mode,
            "graphMode": "none",
            "textMode": text_mode,
            "justifyMode": "auto",
            "orientation": "auto",
            "wideLayout": True,
            "showPercentChange": False,
        },
        "targets": tg,
    }


UP_MAPPINGS = [
    {
        "type": "value",
        "options": {
            "0": {"text": "нет", "color": "red", "index": 0},
            "1": {"text": "работает", "color": "green", "index": 1},
        },
    }
]


def quantile(q: float, metric: str, window: str = "5m") -> str:
    """Квантиль гистограммы по всем экземплярам."""
    return f"histogram_quantile({q}, sum by (le) (rate({metric}_bucket[{window}])))"


def build() -> dict[str, Any]:
    panels: list[dict[str, Any]] = []
    add = panels.append

    add(row("Состояние", 0))
    add(
        stat(
            "dispatcher",
            "Prometheus достаёт /metrics dispatcher (job dispatcher: dispatcher и standby dispatcher-b, если запущен).",
            (0, 1, 4, 4),
            targets(('max(up{job="dispatcher"})', "")),
            mappings=UP_MAPPINGS,
            th=thresholds(("red", None), ("green", 1)),
            color_mode="background",
        )
    )
    add(
        stat(
            "generator",
            "Prometheus достаёт /metrics генератора (job generator). Генератор из "
            "`docker compose run` виден только с --use-aliases.",
            (4, 1, 4, 4),
            targets(('max(up{job="generator"})', "")),
            mappings=UP_MAPPINGS,
            th=thresholds(("red", None), ("green", 1)),
            color_mode="background",
        )
    )
    add(
        stat(
            "epoch",
            "Номер владения партицией (vq_epoch): растёт на каждом claim.",
            (8, 1, 3, 4),
            targets(("max(vq_epoch)", "")),
        )
    )
    add(
        stat(
            "«Сейчас» по времени событий",
            "vq_event_time_seconds — наибольший учтённый ts, UTC.",
            (11, 1, 5, 4),
            targets(("max(vq_event_time_seconds) * 1000", "")),
            unit="dateTimeAsIso",
        )
    )
    add(
        stat(
            "С последнего чекпоинта",
            "time() − vq_last_checkpoint_unixtime. Без входящих сообщений растёт — это нормально.",
            (16, 1, 4, 4),
            targets(("time() - max(vq_last_checkpoint_unixtime)", "")),
            unit="s",
            decimals=0,
            th=thresholds(("green", None), ("yellow", 10), ("red", 60)),
        )
    )
    add(
        stat(
            "Самолечение за период",
            "vq_fsm_resync_total за выбранный период: машина встала у нецелевой "
            "станции или «к станции» вошла в радиус разгрузки (§3.5). При послушном генераторе — 0. increase() "
            "не теряет срабатывания при рестарте dispatcher.",
            (20, 1, 4, 4),
            instant((f"{period('vq_fsm_resync_total')} or vector(0)", "")),
            decimals=0,
            th=thresholds(("green", None), ("red", 1)),
        )
    )

    add(row("Поток сообщений и отбросы", 5))
    add(
        ts(
            "Сообщения, шт/с",
            "Обработанные dispatcher по результату (vq_messages_total) и отправленные "
            "генератором (vq_gen_telemetry_produced_total).",
            (0, 6, 12, 8),
            targets(
                ("sum by (result) (rate(vq_messages_total[$__rate_interval]))", "dispatcher: {{result}}"),
                ("sum(rate(vq_gen_telemetry_produced_total[$__rate_interval]))", "generator: отправлено"),
            ),
        )
    )
    add(
        ts(
            "Отбросы, шт/мин: сервис против ожидания генератора",
            "Отброшенные dispatcher как не новые "
            '(vq_messages_total{result="not_newer"}) и ожидаемые отбросы генератора '
            "(vq_gen_expected_drops_total), среднее за 5 минут. Линии должны совпадать; increase() переживает "
            "рестарт dispatcher. Точные числа с начала лога — processor.msgs_not_newer (витрина, make e2e).",
            (12, 6, 12, 8),
            targets(
                ('sum(increase(vq_messages_total{result="not_newer"}[5m])) / 5', "dispatcher: отброшено"),
                ("sum(increase(vq_gen_expected_drops_total[5m])) / 5", "generator: ожидалось"),
            ),
        )
    )
    add(
        stat(
            "Отбросы за период",
            "increase() за выбранный период: отброшено dispatcher и ожидалось "
            "генератором. Числа близки, но не обязаны совпадать до штуки: increase() экстраполирует, цели "
            "опрашиваются в разные моменты, а приращения упавшего процесса после последнего опроса теряются. "
            "Точное равенство processor.msgs_not_newer и expected_drops показывают витрина и make e2e.",
            (0, 14, 8, 5),
            instant(
                (period('vq_messages_total{result="not_newer"}'), "dispatcher"),
                (period("vq_gen_expected_drops_total"), "generator"),
            ),
            decimals=0,
            text_mode="value_and_name",
        )
    )
    add(
        stat(
            "Невалидные за период",
            'vq_messages_total{result="invalid"} за выбранный период.',
            (8, 14, 8, 5),
            instant((period('vq_messages_total{result="invalid"}') + " or vector(0)", "")),
            decimals=0,
            th=thresholds(("green", None), ("yellow", 1)),
        )
    )
    add(
        stat(
            "Сбои генератора",
            "vq_gen_faults_injected_total по типам: dup, late, outage.",
            (16, 14, 8, 5),
            targets(("sum by (type) (vq_gen_faults_injected_total)", "{{type}}")),
            decimals=0,
            text_mode="value_and_name",
        )
    )

    add(row("Решения", 19))
    add(
        ts(
            "Решения, шт/мин",
            "vq_decisions_total по результату и причине, среднее за 5 минут.",
            (0, 20, 8, 8),
            targets(("sum by (result, reason) (increase(vq_decisions_total[5m])) / 5", "{{result}} {{reason}}")),
        )
    )
    add(
        ts(
            "Выигрыш рекомендации, с",
            "vq_decision_gain_seconds: p50, p95 и среднее за 5 минут.",
            (8, 20, 8, 8),
            targets(
                (quantile(0.5, "vq_decision_gain_seconds"), "p50"),
                (quantile(0.95, "vq_decision_gain_seconds"), "p95"),
                (
                    "sum(rate(vq_decision_gain_seconds_sum[5m])) / sum(rate(vq_decision_gain_seconds_count[5m]))",
                    "среднее",
                ),
            ),
            unit="s",
        )
    )
    add(
        ts(
            "Генератор: решения",
            "Нарастающим итогом: получено, исполнено и проигнорировано генератором "
            "(already_serving при послушном контуре — 0).",
            (16, 20, 8, 8),
            targets(
                ("sum(vq_gen_decisions_received_total)", "получено"),
                ("sum(vq_gen_decisions_applied_total)", "исполнено"),
                ("sum by (reason) (vq_gen_decisions_ignored_total)", "проигнорировано: {{reason}}"),
            ),
            interp="stepAfter",
            decimals=0,
        )
    )
    add(
        stat(
            "Решения за период",
            "vq_decisions_total за выбранный период по результату и причине.",
            (0, 28, 12, 5),
            instant(
                ("round(sum by (result, reason) (increase(vq_decisions_total[$__range])))", "{{result}} {{reason}}")
            ),
            decimals=0,
            text_mode="value_and_name",
        )
    )
    add(
        ts(
            "Задержка исполнения рекомендации, с симуляции",
            "vq_gen_decision_delay_sim_seconds: тик, на котором "
            "генератор применил рекомендацию, минус тик входа машины в зону решения (§4.2 п. 4); при ускорении ×N "
            "растёт примерно в N раз.",
            (12, 28, 12, 5),
            targets(
                (quantile(0.5, "vq_gen_decision_delay_sim_seconds"), "p50"),
                (quantile(0.95, "vq_gen_decision_delay_sim_seconds"), "p95"),
            ),
            unit="s",
        )
    )

    add(row("Точность прогноза", 33))
    add(
        ts(
            "Ошибка прогноза |факт − прогноз|, с",
            "vq_prediction_abs_error_seconds: модуль разницы фактического "
            "занятия и предсказанного начала обслуживания, p50 и p95 за 5 минут.",
            (0, 34, 12, 8),
            targets(
                (quantile(0.5, "vq_prediction_abs_error_seconds"), "p50"),
                (quantile(0.95, "vq_prediction_abs_error_seconds"), "p95"),
            ),
            unit="s",
        )
    )
    add(
        {
            "type": "bargauge",
            "title": "Ошибка прогноза со знаком за выбранный период",
            "description": "vq_prediction_error_seconds: занятий в каждой корзине (факт − прогноз, с); "
            "отрицательное — заняли раньше прогноза.",
            "datasource": DS,
            "gridPos": {"h": 8, "w": 12, "x": 12, "y": 34},
            "fieldConfig": {
                "defaults": {
                    "unit": "short",
                    "decimals": 0,
                    "min": 0,
                    "color": {"mode": "thresholds"},
                    "thresholds": thresholds(("blue", None)),
                },
                "overrides": [],
            },
            "options": {
                "displayMode": "gradient",
                "orientation": "vertical",
                "showUnfilled": True,
                "valueMode": "color",
                "namePlacement": "auto",
                "sizing": "auto",
                "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
            },
            "targets": [
                tgt(
                    "sum by (le) (increase(vq_prediction_error_seconds_bucket[$__range]))",
                    "{{le}}",
                    instant=True,
                    fmt="heatmap",
                )
            ],
        }
    )

    add(row("Очереди станций", 42))
    add(
        ts(
            "Длина очереди",
            "vq_queue_length по станциям: строк в последней опубликованной очереди "
            "(экземпляр dispatcher, владеющий партицией).",
            (0, 43, 8, 8),
            targets((f"max by (station) ({owner('vq_queue_length')})", "{{station}}")),
            interp="stepAfter",
            decimals=0,
        )
    )
    add(
        ts(
            "Публикации queue.v1, шт/мин",
            "vq_queue_publications_total по станциям.",
            (8, 43, 8, 8),
            targets(("sum by (station) (rate(vq_queue_publications_total[$__rate_interval])) * 60", "{{station}}")),
        )
    )
    add(
        ts(
            "Освобождения станций, шт/мин",
            "vq_station_releases_total по станции и причине (timer | exit), среднее за 5 минут.",
            (16, 43, 8, 8),
            targets(
                ("sum by (station, reason) (increase(vq_station_releases_total[5m])) / 5", "{{station}} {{reason}}")
            ),
        )
    )

    add(row("Чекпоинт и лаг", 51))
    add(
        ts(
            "Чекпоинт и пачка, с",
            "vq_checkpoint_seconds (транзакция Postgres) и vq_batch_seconds (пачка целиком), p50 и p95.",
            (0, 52, 8, 8),
            targets(
                (quantile(0.5, "vq_checkpoint_seconds", "$__rate_interval"), "чекпоинт p50"),
                (quantile(0.95, "vq_checkpoint_seconds", "$__rate_interval"), "чекпоинт p95"),
                (quantile(0.5, "vq_batch_seconds", "$__rate_interval"), "пачка p50"),
                (quantile(0.95, "vq_batch_seconds", "$__rate_interval"), "пачка p95"),
            ),
            unit="s",
        )
    )
    add(
        ts(
            "Лаг консьюмера, сообщений",
            "vq_consumer_lag_messages: high watermark telemetry.v1 минус "
            "следующий offset dispatcher (экземпляр, владеющий партицией).",
            (8, 52, 8, 8),
            targets((f"max({owner('vq_consumer_lag_messages')})", "лаг")),
            decimals=0,
        )
    )
    add(
        ts(
            "Отставание по времени событий, с",
            "Часы минус «сейчас» (vq_event_time_lag_seconds, осмысленно "
            "только в реальном темпе) и время симуляции генератора минус «сейчас» dispatcher.",
            (16, 52, 8, 8),
            targets(
                (f"max({owner('vq_event_time_lag_seconds')})", "часы − «сейчас»"),
                ("max(vq_gen_sim_time_seconds) - max(vq_event_time_seconds)", "генератор − dispatcher"),
            ),
            unit="s",
        )
    )

    return {
        "uid": "vq",
        "title": "Виртуальные очереди",
        "description": "Поток сообщений, решения, точность прогноза, очереди станций, чекпоинт и лаг (§5.2).",
        "tags": ["vq"],
        "timezone": "utc",
        "editable": False,
        "graphTooltip": 1,
        "refresh": "5s",
        "time": {"from": "now-30m", "to": "now"},
        "timepicker": {"refresh_intervals": ["5s", "10s", "30s", "1m", "5m"]},
        "schemaVersion": 41,
        "version": 1,
        "links": [],
        "templating": {"list": []},
        "annotations": {
            "list": [
                {
                    "builtIn": 1,
                    "datasource": {"type": "grafana", "uid": "-- Grafana --"},
                    "enable": True,
                    "hide": True,
                    "iconColor": "rgba(0, 211, 255, 1)",
                    "name": "Annotations & Alerts",
                    "type": "dashboard",
                }
            ]
        },
        # id — номер панели по порядку; ключ — сразу за type
        "panels": [{"type": panel["type"], "id": i, **panel} for i, panel in enumerate(panels, 1)],
    }


def render() -> str:
    return json.dumps(build(), ensure_ascii=False, indent=2) + "\n"


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--out", type=Path, default=DEFAULT_OUT)
    p.add_argument("--check", action="store_true", help="не писать, а сверить файл с генерацией")
    args = p.parse_args(argv)
    text = render()
    if args.check:
        ok = args.out.exists() and args.out.read_text(encoding="utf-8") == text
        print(f"{args.out}: {'совпадает' if ok else 'РАСХОДИТСЯ с генерацией'}")
        return 0 if ok else 1
    args.out.write_text(text, encoding="utf-8")
    print(f"записан {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
