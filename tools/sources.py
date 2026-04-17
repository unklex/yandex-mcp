"""
Инструмент: get_traffic_sources
Разбивка трафика по источникам/каналам.
"""

from __future__ import annotations

import json
from typing import Optional

from mcp.server.fastmcp import Context

from app import mcp, resolve_counter
from metrica_client import MetricaAPIError

_SOURCE_LABELS: dict[str, str] = {
    "organic": "Поисковый трафик",
    "direct": "Прямые заходы",
    "referral": "Переходы с сайтов",
    "ad": "Реклама",
    "social": "Социальные сети",
    "email": "Email-рассылки",
    "messenger": "Мессенджеры",
    "internal": "Внутренние переходы",
    "recommendation": "Рекомендательные системы",
}


@mcp.tool()
async def get_traffic_sources(
    ctx: Context,
    date_from: str,
    date_to: str,
    limit: int = 10,
    counter_id: Optional[str] = None,
) -> str:
    """
    Получить разбивку трафика по источникам и каналам (органика, прямые,
    реферальные, реклама, соцсети и т.д.) с показателями качества трафика.

    Параметры:
    - date_from:  дата начала. Форматы: YYYY-MM-DD, today, yesterday, NdaysAgo
    - date_to:    дата окончания. Те же форматы.
    - limit:      количество источников в отчёте (по умолчанию 10)
    - counter_id: ID счётчика (необязательно)

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
                "ym:s:bounceRate,"
                "ym:s:avgVisitDurationSeconds"
            ),
            dimensions="ym:s:lastTrafficSource",
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
            {"error": f"Нет данных по источникам трафика за период {date_from} — {date_to}."},
            ensure_ascii=False,
        )

    total_visits = sum(r["metrics"][0] for r in rows if r.get("metrics"))

    sources = []
    for row in rows:
        dim = row.get("dimensions", [{}])[0]
        source_key = dim.get("id", "")
        source_label = _SOURCE_LABELS.get(source_key, source_key or "Неизвестно")
        m = (list(row.get("metrics", [])) + [0, 0, 0, 0])[:4]
        visits, users, bounce_rate, avg_duration = m
        share_pct = round((visits / total_visits * 100), 1) if total_visits else 0.0
        sources.append({
            "source_key": source_key,
            "source_label": source_label,
            "visits": int(visits),
            "share_pct": share_pct,
            "users": int(users),
            "bounce_rate_pct": round(float(bounce_rate), 2),
            "avg_visit_duration_sec": round(float(avg_duration), 1),
        })

    result: dict = {
        "period": {"from": date_from, "to": date_to},
        "counter_id": resolved_id,
        "returned_rows": len(sources),
        "total_rows": data.get("total_rows", len(sources)),
        "sources": sources,
    }

    if "_sampling_warning" in data:
        result["_sampling_warning"] = data["_sampling_warning"]

    return json.dumps(result, ensure_ascii=False, indent=2)
