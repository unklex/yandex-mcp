"""
Инструмент: get_top_pages
Топ страниц входа по визитам с показателем отказов и временем на сайте.

Важно (исправлено 2026-08-09): метрик `ym:pv:bounceRate` и
`ym:pv:avgVisitDurationSeconds` В API НЕ СУЩЕСТВУЕТ. Отказ и длительность —
характеристики ВИЗИТА (`ym:s:`), а не отдельного просмотра (`ym:pv:`).
Прежняя версия просила их с префиксом `ym:pv:` и получала ошибку 4002
(«нельзя смешивать ym:s: и ym:pv: в одном запросе») на каждом вызове.

Поэтому отчёт построен целиком на `ym:s:` и сгруппирован по СТРАНИЦЕ ВХОДА
(`ym:s:startURLPathFull`). Для SEO это и нужно: видно, на какие статьи люди
приземляются из поиска, с каким отказом и сколько времени проводят.
"""

from __future__ import annotations

import json
from typing import Optional

from mcp.server.mcpserver import Context

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
    Получить топ страниц входа по количеству визитов с показателем отказов
    и средним временем на сайте.

    Параметры:
    - date_from:  дата начала. Форматы: YYYY-MM-DD, today, yesterday, NdaysAgo
    - date_to:    дата окончания. Те же форматы.
    - limit:      количество страниц в отчёте (по умолчанию 20, максимум 100)
    - counter_id: ID счётчика (необязательно)

    Группировка — по странице входа (landing page). Это отчёт «куда люди
    приземляются», а не «какие страницы просматривают внутри визита».

    Возвращает сырые данные в JSON. При ответе пользователю ВСЕГДА форматируй
    данные в виде читаемой таблицы Markdown на русском языке с нумерацией.
    """
    lc = ctx.request_context.lifespan_context
    client = lc["client"]
    resolved_id = resolve_counter(counter_id, lc)

    try:
        data = await client.get_data(
            # Только ym:s: — смешивать префиксы с ym:pv: нельзя (ошибка 4002)
            metrics=(
                "ym:s:visits,"
                "ym:s:bounceRate,"
                "ym:s:avgVisitDurationSeconds"
            ),
            dimensions="ym:s:startURLPathFull",
            date1=date_from,
            date2=date_to,
            sort="-ym:s:visits",
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
        visits, bounce_rate, avg_duration = m
        pages.append({
            "url": url,
            "visits": int(visits),
            "bounce_rate_pct": round(float(bounce_rate), 2),
            "avg_duration_sec": round(float(avg_duration), 1),
        })

    result: dict = {
        "period": {"from": date_from, "to": date_to},
        "counter_id": resolved_id,
        "grouping": "страница входа (ym:s:startURLPathFull)",
        "returned_rows": len(pages),
        "total_rows": data.get("total_rows", len(pages)),
        "pages": pages,
    }

    if "_sampling_warning" in data:
        result["_sampling_warning"] = data["_sampling_warning"]

    return json.dumps(result, ensure_ascii=False, indent=2)
