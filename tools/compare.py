"""
Инструмент: compare_periods
Сравнение метрики между двумя периодами с расчётом процентного изменения.
"""

from __future__ import annotations

import json
from typing import Optional

from mcp.server.fastmcp import Context

from app import mcp, resolve_counter
from metrica_client import MetricaAPIError

# Маппинг дружелюбных имён метрик → API-имена и русские названия
_METRIC_MAP: dict[str, tuple[str, str]] = {
    "visits":      ("ym:s:visits",                 "Сессии"),
    "users":       ("ym:s:users",                  "Пользователи"),
    "pageviews":   ("ym:s:pageviews",              "Просмотры страниц"),
    "bounce_rate": ("ym:s:bounceRate",             "Показатель отказов (%)"),
    "duration":    ("ym:s:avgVisitDurationSeconds", "Среднее время на сайте (сек)"),
    "new_users":   ("ym:s:newUsers",               "Новые пользователи"),
}


@mcp.tool()
async def compare_periods(
    ctx: Context,
    metric: str,
    date_from_a: str,
    date_to_a: str,
    date_from_b: str,
    date_to_b: str,
    counter_id: Optional[str] = None,
) -> str:
    """
    Сравнить метрику между двумя периодами и рассчитать процентное изменение.

    Доступные значения metric:
    - visits      — сессии
    - users       — пользователи
    - pageviews   — просмотры страниц
    - bounce_rate — показатель отказов
    - duration    — среднее время на сайте в секундах
    - new_users   — новые пользователи

    Параметры:
    - metric:      название метрики (из списка выше)
    - date_from_a: начало периода А. Форматы: YYYY-MM-DD, today, yesterday, NdaysAgo
    - date_to_a:   конец периода А. Те же форматы.
    - date_from_b: начало периода Б (базовый/сравниваемый)
    - date_to_b:   конец периода Б
    - counter_id:  ID счётчика (необязательно)

    Возвращает сырые данные в JSON. При ответе пользователю ВСЕГДА форматируй
    данные в виде читаемой таблицы Markdown на русском языке, указывай
    направление изменения (▲ рост / ▼ снижение) и процент.
    """
    lc = ctx.request_context.lifespan_context
    client = lc["client"]
    resolved_id = resolve_counter(counter_id, lc)

    if metric not in _METRIC_MAP:
        valid = ", ".join(_METRIC_MAP.keys())
        return json.dumps(
            {"error": f"Неизвестная метрика: «{metric}». Допустимые значения: {valid}."},
            ensure_ascii=False,
        )

    api_metric, display_name = _METRIC_MAP[metric]

    try:
        data = await client.get_comparison(
            metrics=api_metric,
            date1_a=date_from_a,
            date2_a=date_to_a,
            date1_b=date_from_b,
            date2_b=date_to_b,
            counter_id=resolved_id,
        )
    except ValueError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)
    except MetricaAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    totals_a = data.get("totals_a", [0])
    totals_b = data.get("totals_b", [0])

    val_a = float(totals_a[0]) if totals_a else 0.0
    val_b = float(totals_b[0]) if totals_b else 0.0

    if val_b != 0:
        pct_change = round(((val_a - val_b) / abs(val_b)) * 100, 2)
    else:
        pct_change = 0.0

    if pct_change > 0:
        trend = "рост"
        direction = "▲"
    elif pct_change < 0:
        trend = "снижение"
        direction = "▼"
    else:
        trend = "без изменений"
        direction = "→"

    result: dict = {
        "metric": metric,
        "metric_label": display_name,
        "counter_id": resolved_id,
        "period_a": {"from": date_from_a, "to": date_to_a, "value": val_a},
        "period_b": {"from": date_from_b, "to": date_to_b, "value": val_b},
        "pct_change": pct_change,
        "trend": trend,
        "direction": direction,
    }

    if "_sampling_warning" in data:
        result["_sampling_warning"] = data["_sampling_warning"]

    return json.dumps(result, ensure_ascii=False, indent=2)
