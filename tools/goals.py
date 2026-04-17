"""
Инструмент: get_goals
Двухфазный запрос: получение списка целей из Management API,
затем их статистики из Reporting API.
"""

from __future__ import annotations

import json
from typing import Optional

from mcp.server.fastmcp import Context

from app import mcp, resolve_counter
from metrica_client import MetricaAPIError

_GOAL_TYPE_LABELS: dict[str, str] = {
    "url": "Посещение страницы",
    "action": "JavaScript-событие",
    "step": "Многошаговая цель",
    "number": "Количество страниц",
    "duration": "Время на сайте",
    "ecommerce": "Электронная торговля",
}


@mcp.tool()
async def get_goals(
    ctx: Context,
    date_from: str,
    date_to: str,
    counter_id: Optional[str] = None,
) -> str:
    """
    Получить список целей счётчика Яндекс.Метрики и их показатели конверсии
    (количество достижений и процент конверсии) за указанный период.

    Параметры:
    - date_from:  дата начала. Форматы: YYYY-MM-DD, today, yesterday, NdaysAgo
    - date_to:    дата окончания. Те же форматы.
    - counter_id: ID счётчика (необязательно)

    Возвращает сырые данные в JSON. При ответе пользователю ВСЕГДА форматируй
    данные в виде читаемой таблицы Markdown на русском языке.
    """
    lc = ctx.request_context.lifespan_context
    client = lc["client"]
    resolved_id = resolve_counter(counter_id, lc)

    # Фаза 1: получаем список целей из Management API
    try:
        goals_data = await client.get_goals_list(counter_id=resolved_id)
    except MetricaAPIError as e:
        return json.dumps(
            {"error": f"Ошибка получения списка целей: {e}"},
            ensure_ascii=False,
        )

    goals = goals_data.get("goals", [])
    if not goals:
        return json.dumps(
            {"error": "В счётчике не настроено ни одной цели."},
            ensure_ascii=False,
        )

    # Фаза 2: запрашиваем статистику по первым 10 целям (20 метрик максимум)
    active_goals = goals[:10]
    goal_metrics = ",".join(
        f"ym:s:goal{g['id']}reaches,ym:s:goal{g['id']}conversionRate"
        for g in active_goals
    )

    try:
        stats = await client.get_data(
            metrics=goal_metrics,
            date1=date_from,
            date2=date_to,
            counter_id=resolved_id,
        )
    except ValueError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)
    except MetricaAPIError as e:
        return json.dumps(
            {"error": f"Ошибка получения статистики по целям: {e}"},
            ensure_ascii=False,
        )

    totals = stats.get("totals", [])

    goals_result = []
    for idx, goal in enumerate(active_goals):
        reaches = int(totals[idx * 2]) if len(totals) > idx * 2 else 0
        conv_rate = round(float(totals[idx * 2 + 1]), 4) if len(totals) > idx * 2 + 1 else 0.0
        goals_result.append({
            "id": goal.get("id"),
            "name": goal.get("name", "Без названия"),
            "type": goal.get("type", ""),
            "type_label": _GOAL_TYPE_LABELS.get(goal.get("type", ""), goal.get("type", "—")),
            "reaches": reaches,
            "conversion_rate_pct": conv_rate,
        })

    result: dict = {
        "period": {"from": date_from, "to": date_to},
        "counter_id": resolved_id,
        "total_goals_in_counter": len(goals),
        "returned_goals": len(goals_result),
        "goals": goals_result,
    }

    if len(goals) > 10:
        result["_note"] = f"Показаны первые 10 из {len(goals)} целей."

    if "_sampling_warning" in stats:
        result["_sampling_warning"] = stats["_sampling_warning"]

    return json.dumps(result, ensure_ascii=False, indent=2)
