"""
Инструмент: get_audience
Разбивка аудитории по устройствам, географии или браузерам.
"""

from __future__ import annotations

import json
from typing import Optional

from mcp.server.fastmcp import Context

from app import mcp, resolve_counter
from metrica_client import MetricaAPIError

_DIMENSION_MAP: dict[str, str] = {
    "device": "ym:s:deviceCategory",
    "city": "ym:s:regionCityName",
    "region": "ym:s:regionName",
    "browser": "ym:s:browser",
}

_BREAKDOWN_LABELS: dict[str, str] = {
    "device": "тип устройства",
    "city": "город",
    "region": "регион",
    "browser": "браузер",
}

_DEVICE_LABELS: dict[str, str] = {
    "desktop": "Десктоп",
    "mobile": "Мобильный",
    "tablet": "Планшет",
    "tv": "Smart TV",
}


@mcp.tool()
async def get_audience(
    ctx: Context,
    date_from: str,
    date_to: str,
    breakdown: str = "device",
    limit: int = 15,
    counter_id: Optional[str] = None,
) -> str:
    """
    Получить разбивку аудитории сайта по выбранному параметру.

    Доступные значения breakdown:
    - device  — тип устройства (десктоп, мобильный, планшет)
    - city    — топ городов по трафику
    - region  — регионы (область/край)
    - browser — браузеры

    Параметры:
    - date_from:  дата начала. Форматы: YYYY-MM-DD, today, yesterday, NdaysAgo
    - date_to:    дата окончания. Те же форматы.
    - breakdown:  тип разбивки — device | city | region | browser (по умолчанию device)
    - limit:      количество строк в отчёте (по умолчанию 15)
    - counter_id: ID счётчика (необязательно)

    Возвращает сырые данные в JSON. При ответе пользователю ВСЕГДА форматируй
    данные в виде читаемой таблицы Markdown на русском языке.
    """
    lc = ctx.request_context.lifespan_context
    client = lc["client"]
    resolved_id = resolve_counter(counter_id, lc)

    dimension = _DIMENSION_MAP.get(breakdown)
    if not dimension:
        valid = ", ".join(_DIMENSION_MAP.keys())
        return json.dumps(
            {"error": f"Неизвестный тип разбивки: «{breakdown}». Допустимые значения: {valid}."},
            ensure_ascii=False,
        )

    try:
        data = await client.get_data(
            metrics="ym:s:visits,ym:s:users,ym:s:bounceRate",
            dimensions=dimension,
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
            {"error": f"Нет данных по аудитории ({breakdown}) за период {date_from} — {date_to}."},
            ensure_ascii=False,
        )

    total_visits = sum(r["metrics"][0] for r in rows if r.get("metrics"))

    segments = []
    for row in rows:
        dim = row.get("dimensions", [{}])[0]
        raw_name = dim.get("name", "—")
        name = _DEVICE_LABELS.get(raw_name.lower(), raw_name) if breakdown == "device" else raw_name
        m = (list(row.get("metrics", [])) + [0, 0, 0])[:3]
        visits, users, bounce_rate = m
        share_pct = round((visits / total_visits * 100), 1) if total_visits else 0.0
        segments.append({
            "name": name,
            "visits": int(visits),
            "share_pct": share_pct,
            "users": int(users),
            "bounce_rate_pct": round(float(bounce_rate), 2),
        })

    result: dict = {
        "period": {"from": date_from, "to": date_to},
        "counter_id": resolved_id,
        "breakdown_type": breakdown,
        "breakdown_label": _BREAKDOWN_LABELS[breakdown],
        "returned_rows": len(segments),
        "total_rows": data.get("total_rows", len(segments)),
        "segments": segments,
    }

    if "_sampling_warning" in data:
        result["_sampling_warning"] = data["_sampling_warning"]

    return json.dumps(result, ensure_ascii=False, indent=2)
