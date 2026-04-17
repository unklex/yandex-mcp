"""
Инструмент: get_realtime
Активность сайта сегодня с разбивкой по часам (псевдореальное время).

Яндекс.Метрика не предоставляет настоящий WebSocket realtime API.
Ближайший аналог — bytime с group=hour и date1=today.
Данные обновляются с задержкой ~5 минут.
"""

from __future__ import annotations

import json
from datetime import datetime
from typing import Optional

from mcp.server.fastmcp import Context

from app import mcp, resolve_counter
from metrica_client import MetricaAPIError


@mcp.tool()
async def get_realtime(
    ctx: Context,
    counter_id: Optional[str] = None,
) -> str:
    """
    Получить активность сайта за сегодня с разбивкой по часам.
    Показывает сессии и пользователей по каждому часу текущих суток.

    Параметры:
    - counter_id: ID счётчика (необязательно)

    Примечание: данные обновляются с задержкой около 5 минут.
    Последний час может быть неполным — он ещё продолжается.

    Возвращает сырые данные в JSON. При ответе пользователю ВСЕГДА форматируй
    данные в виде читаемой таблицы Markdown на русском языке.
    """
    lc = ctx.request_context.lifespan_context
    client = lc["client"]
    resolved_id = resolve_counter(counter_id, lc)

    try:
        data = await client.get_bytime(
            metrics="ym:s:visits,ym:s:users",
            date1="today",
            date2="today",
            group="hour",
            counter_id=resolved_id,
        )
    except MetricaAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    time_intervals = data.get("time_intervals", [])
    data_rows = data.get("data", [])

    if not time_intervals or not data_rows:
        return json.dumps(
            {
                "error": (
                    "Нет данных об активности за сегодня. "
                    "Возможно, статистика ещё не накоплена или счётчик не получал трафик."
                )
            },
            ensure_ascii=False,
        )

    # Структура bytime: data[0]["metrics"][metric_index][time_index]
    try:
        visits_by_hour: list = data_rows[0]["metrics"][0]
        users_by_hour: list = data_rows[0]["metrics"][1]
    except (KeyError, IndexError):
        return json.dumps(
            {"error": "Не удалось разобрать ответ API bytime. Попробуйте позже."},
            ensure_ascii=False,
        )

    by_hour = []
    for interval, visits, users in zip(time_intervals, visits_by_hour, users_by_hour):
        # interval: ["2024-01-15 09:00:00", "2024-01-15 09:59:59"]
        hour_label = interval[0][11:16] if isinstance(interval, list) and interval else str(interval)[:5]
        by_hour.append({
            "hour": hour_label,
            "visits": int(visits),
            "users": int(users),
        })

    total_visits = sum(h["visits"] for h in by_hour)
    max_users = max((h["users"] for h in by_hour), default=0)

    result: dict = {
        "date": datetime.now().strftime("%Y-%m-%d"),
        "counter_id": resolved_id,
        "totals": {
            "visits_today": total_visits,
            "peak_users_per_hour": max_users,
        },
        "by_hour": by_hour,
        "_note": (
            "Данные обновляются с задержкой ~5 минут. "
            "Последний час в таблице может быть неполным."
        ),
    }

    return json.dumps(result, ensure_ascii=False, indent=2)
