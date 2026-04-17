"""
Инструмент: get_top_pages
Топ страниц по просмотрам с показателем отказов и временем на странице.

Важно: метрики страниц используют префикс ym:pv: (pageview),
а НЕ ym:s: (session). Нельзя смешивать префиксы в одном запросе.
"""

from __future__ import annotations

import json
from typing import Optional

from mcp.server.fastmcp import Context

from app import mcp, resolve_counter
from metrica_client import MetricaAPIError


@mcp.tool()
async def get_top_pages(
    ctx: Context,
    date_from: str,
    date_to: str,
    limit: int = 20,
    counter_id: Optional[str] = None,
) -> str:
    """
    Получить топ страниц сайта по количеству просмотров с показателем отказов
    и средним временем на странице.

    Параметры:
    - date_from:  дата начала. Форматы: YYYY-MM-DD, today, yesterday, NdaysAgo
    - date_to:    дата окончания. Те же форматы.
    - limit:      количество страниц в отчёте (по умолчанию 20, максимум 100)
    - counter_id: ID счётчика (необязательно)

    Возвращает сырые данные в JSON. При ответе пользователю ВСЕГДА форматируй
    данные в виде читаемой таблицы Markdown на русском языке с нумерацией.
    """
    lc = ctx.request_context.lifespan_context
    client = lc["client"]
    resolved_id = resolve_counter(counter_id, lc)

    try:
        data = await client.get_data(
            # Используем ym:pv: — метрики просмотров страниц
            metrics=(
                "ym:pv:pageviews,"
                "ym:pv:bounceRate,"
                "ym:pv:avgVisitDurationSeconds"
            ),
            dimensions="ym:pv:URLPathFull",
            date1=date_from,
            date2=date_to,
            sort="-ym:pv:pageviews",
            limit=limit,
            counter_id=resolved_id,
        )
    except ValueError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)
    except MetricaAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    rows = data.get("data", [])
    if not rows:
        return json.dumps(
            {"error": f"Нет данных по страницам за период {date_from} — {date_to}."},
            ensure_ascii=False,
        )

    pages = []
    for row in rows:
        dim = row.get("dimensions", [{}])[0]
        url = dim.get("name", "—")
        m = (list(row.get("metrics", [])) + [0, 0, 0])[:3]
        pageviews, bounce_rate, avg_duration = m
        pages.append({
            "url": url,
            "pageviews": int(pageviews),
            "bounce_rate_pct": round(float(bounce_rate), 2),
            "avg_duration_sec": round(float(avg_duration), 1),
        })

    result: dict = {
        "period": {"from": date_from, "to": date_to},
        "counter_id": resolved_id,
        "returned_rows": len(pages),
        "total_rows": data.get("total_rows", len(pages)),
        "pages": pages,
    }

    if "_sampling_warning" in data:
        result["_sampling_warning"] = data["_sampling_warning"]

    return json.dumps(result, ensure_ascii=False, indent=2)
