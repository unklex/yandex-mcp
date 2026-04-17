"""
Инструмент: get_traffic_summary
Общая сводка трафика: сессии, пользователи, просмотры, отказы, время на сайте.
"""

from __future__ import annotations

import json
from typing import Optional

from mcp.server.fastmcp import Context

from app import mcp, resolve_counter
from metrica_client import MetricaAPIError


@mcp.tool()
async def get_traffic_summary(
    ctx: Context,
    date_from: str,
    date_to: str,
    counter_id: Optional[str] = None,
) -> str:
    """
    Получить общую сводку трафика сайта за указанный период:
    сессии, уникальные пользователи, просмотры страниц, показатель отказов,
    среднее время на сайте.

    Параметры:
    - date_from: дата начала. Форматы: YYYY-MM-DD, today, yesterday, NdaysAgo (например, 7daysAgo)
    - date_to:   дата окончания. Те же форматы.
    - counter_id: ID счётчика (необязательно; если не указан — используется значение из конфига)

    Возвращает сырые данные в JSON. При ответе пользователю ВСЕГДА форматируй
    данные в виде читаемой таблицы Markdown на русском языке.
    """
    lc = ctx.request_context.lifespan_context
    client = lc["client"]
    resolved_id = resolve_counter(counter_id, lc)

    try:
        data = await client.get_data(
            metrics=(
                "ym:s:visits,"
                "ym:s:users,"
                "ym:s:pageviews,"
                "ym:s:bounceRate,"
                "ym:s:avgVisitDurationSeconds"
            ),
            date1=date_from,
            date2=date_to,
            counter_id=int(resolved_id),
        )
    except ValueError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)
    except MetricaAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    totals = data.get("totals", [])
    if not totals:
        return json.dumps(
            {"error": f"Нет данных за период {date_from} — {date_to}."},
            ensure_ascii=False,
        )

    visits, users, pageviews, bounce_rate, avg_duration = (list(totals) + [0] * 5)[:5]

    result: dict = {
        "period": {"from": date_from, "to": date_to},
        "counter_id": resolved_id,
        "totals": {
            "sessions": int(visits),
            "users": int(users),
            "pageviews": int(pageviews),
            "bounce_rate_pct": round(float(bounce_rate), 2),
            "avg_visit_duration_sec": round(float(avg_duration), 1),
        },
    }

    if "_sampling_warning" in data:
        result["_sampling_warning"] = data["_sampling_warning"]

    return json.dumps(result, ensure_ascii=False, indent=2)
