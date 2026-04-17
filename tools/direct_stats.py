"""
Инструменты Яндекс.Директ — статистика эффективности и ключевые фразы.

Инструменты:
  - get_direct_performance     — клики, показы, расход, CTR, CPC, конверсии, ROI
  - get_direct_keywords        — топ ключевых фраз по расходу или кликам
  - get_direct_search_queries  — реальные поисковые запросы пользователей
"""

from __future__ import annotations

import json
from typing import Optional

from mcp.server.fastmcp import Context

from app import mcp, resolve_direct_client
from direct_client import DirectAPIError

# Поля из Reports API, которые хранятся в микро-рублях (÷1_000_000 = рубли)
_MICRO_FIELDS = frozenset({"Cost", "CostPerConversion", "Revenue", "AvgCpc"})
# Поля, которые округляем до 2 знаков (проценты и коэффициенты)
_ROUND2_FIELDS = frozenset({"Ctr", "ConversionRate", "GoalsRoi"})

_VALID_RANGES = frozenset({"LAST_7_DAYS", "LAST_30_DAYS", "THIS_MONTH", "LAST_MONTH", "CUSTOM_DATE"})


def format_metrics(row: dict[str, str]) -> dict[str, float | str]:
    """
    Конвертирует денежные поля из микро-рублей в рубли (÷1,000,000).
    Округляет процентные/коэффициентные поля до 2 знаков.
    Это предотвращает передачу LLM длинных float-хвостов.
    """
    result: dict[str, float | str] = {}
    for key, val in row.items():
        try:
            f = float(val or 0)
            if key in _MICRO_FIELDS:
                result[key] = round(f / 1_000_000, 2)
            elif key in _ROUND2_FIELDS:
                result[key] = round(f, 2)
            else:
                result[key] = f
        except (ValueError, TypeError):
            result[key] = val or ""
    return result


def _safe_int(val) -> int:
    try:
        return int(float(val or 0))
    except (ValueError, TypeError):
        return 0


def _safe_float(val) -> float:
    try:
        return float(val or 0)
    except (ValueError, TypeError):
        return 0.0


def _no_direct_error(account: str | None = None) -> str:
    msg = "Клиент Яндекс.Директа не инициализирован."
    if account:
        msg += f" Аккаунт «{account}» не найден в YANDEX_DIRECT_ACCOUNTS."
    msg += " Проверьте переменные окружения YANDEX_DIRECT_ACCOUNTS или YANDEX_DIRECT_TOKEN."
    return json.dumps({"error": msg}, ensure_ascii=False)


def _parse_campaign_ids(campaign_ids: str | None) -> tuple[list[int] | None, str | None]:
    """Парсит строку '123,456' в список int. Возвращает (список, ошибку)."""
    if not campaign_ids:
        return None, None
    try:
        ids = [int(x.strip()) for x in campaign_ids.split(",") if x.strip()]
        return ids, None
    except ValueError:
        return None, "Параметр campaign_ids должен содержать целые числа через запятую (например, '123,456')."


@mcp.tool()
async def get_direct_performance(
    ctx: Context,
    date_range: str = "LAST_30_DAYS",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    campaign_ids: Optional[str] = None,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Получить сводку эффективности рекламы в Яндекс.Директе:
    клики, показы, расход, CTR, средняя цена клика, конверсии, стоимость конверсии, ROI.

    Параметры:
    - date_range:   период. Допустимые значения:
                    LAST_7_DAYS, LAST_30_DAYS, THIS_MONTH, LAST_MONTH, CUSTOM_DATE
    - date_from:    дата начала YYYY-MM-DD (только при date_range=CUSTOM_DATE)
    - date_to:      дата окончания YYYY-MM-DD (только при date_range=CUSTOM_DATE)
    - campaign_ids: ID кампаний через запятую для фильтрации (например, '123,456').
                    Если не указаны — данные по всем кампаниям аккаунта.
    - account:      псевдоним аккаунта Директа (необязательно)
    - client_login: логин клиента (для агентских аккаунтов, необязательно)

    Возвращает сырые данные в JSON. При ответе пользователю ВСЕГДА форматируй
    данные в виде читаемой таблицы Markdown на русском языке.
    Суммы расходов и конверсий указаны в рублях без НДС. CTR в процентах (%).
    """
    lc = ctx.request_context.lifespan_context
    direct = resolve_direct_client(account, lc)
    if direct is None:
        return _no_direct_error(account)

    if date_range not in _VALID_RANGES:
        return json.dumps(
            {"error": f"Неверный date_range: «{date_range}». Допустимые: {', '.join(sorted(_VALID_RANGES))}."},
            ensure_ascii=False,
        )

    parsed_ids, err = _parse_campaign_ids(campaign_ids)
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    try:
        rows = await direct.get_report(
            field_names=[
                "CampaignId", "CampaignName",
                "Clicks", "Impressions", "Cost", "Ctr", "AvgCpc",
                "Conversions", "CostPerConversion", "ConversionRate", "Revenue", "GoalsRoi",
            ],
            date_range_type=date_range,
            report_name="performance",
            date_from=date_from,
            date_to=date_to,
            campaign_ids=parsed_ids,
            client_login=client_login,
        )
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    if not rows:
        return json.dumps(
            {"error": f"Нет данных за период {date_range}. Возможно, кампании не показывались."},
            ensure_ascii=False,
        )

    # Агрегируем totals и группируем по кампаниям
    campaigns_map: dict[str, dict] = {}
    total_clicks = 0
    total_impressions = 0
    total_cost = 0.0
    total_conversions = 0
    total_revenue = 0.0

    for row in rows:
        m = format_metrics(row)
        cid = row.get("CampaignId", "—")

        clicks = _safe_int(m.get("Clicks"))
        impressions = _safe_int(m.get("Impressions"))
        cost = _safe_float(m.get("Cost"))
        conversions = _safe_int(m.get("Conversions"))
        revenue = _safe_float(m.get("Revenue"))

        total_clicks += clicks
        total_impressions += impressions
        total_cost += cost
        total_conversions += conversions
        total_revenue += revenue

        if cid not in campaigns_map:
            campaigns_map[cid] = {
                "campaign_id": cid,
                "campaign_name": row.get("CampaignName", "—"),
                "clicks": 0,
                "impressions": 0,
                "cost_rub": 0.0,
                "conversions": 0,
                "revenue_rub": 0.0,
            }
        campaigns_map[cid]["clicks"] += clicks
        campaigns_map[cid]["impressions"] += impressions
        campaigns_map[cid]["cost_rub"] = round(campaigns_map[cid]["cost_rub"] + cost, 2)
        campaigns_map[cid]["conversions"] += conversions
        campaigns_map[cid]["revenue_rub"] = round(campaigns_map[cid]["revenue_rub"] + revenue, 2)

    # Добавляем производные метрики на уровень кампании
    by_campaign = []
    for c in sorted(campaigns_map.values(), key=lambda x: x["cost_rub"], reverse=True):
        imp = c["impressions"]
        clicks = c["clicks"]
        cost = c["cost_rub"]
        rev = c["revenue_rub"]
        c["ctr_pct"] = round(clicks / imp * 100, 2) if imp > 0 else 0.0
        c["avg_cpc_rub"] = round(cost / clicks, 2) if clicks > 0 else 0.0
        c["roi_pct"] = round((rev - cost) / cost * 100, 2) if cost > 0 else 0.0
        by_campaign.append(c)

    # Сводные итоги
    total_ctr = round(total_clicks / total_impressions * 100, 2) if total_impressions > 0 else 0.0
    avg_cpc = round(total_cost / total_clicks, 2) if total_clicks > 0 else 0.0
    roi = round((total_revenue - total_cost) / total_cost * 100, 2) if total_cost > 0 else 0.0
    total_cost = round(total_cost, 2)
    total_revenue = round(total_revenue, 2)

    result: dict = {
        "account": account or "primary",
        "date_range": date_range,
        "totals": {
            "clicks": total_clicks,
            "impressions": total_impressions,
            "cost_rub": total_cost,
            "ctr_pct": total_ctr,
            "avg_cpc_rub": avg_cpc,
            "conversions": total_conversions,
            "revenue_rub": total_revenue,
            "roi_pct": roi,
        },
        "by_campaign": by_campaign,
    }
    if date_from and date_to:
        result["period"] = {"from": date_from, "to": date_to}
    if parsed_ids:
        result["filtered_campaign_ids"] = parsed_ids

    warning = direct.units_warning()
    if warning:
        result["_units_warning"] = warning

    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
async def get_direct_keywords(
    ctx: Context,
    date_range: str = "LAST_30_DAYS",
    sort_by: str = "Cost",
    top_n: int = 20,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    campaign_ids: Optional[str] = None,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Получить топ ключевых фраз в Яндекс.Директе по расходу или кликам.

    Параметры:
    - date_range:   период. Допустимые значения:
                    LAST_7_DAYS, LAST_30_DAYS, THIS_MONTH, LAST_MONTH, CUSTOM_DATE
    - sort_by:      метрика сортировки: Cost (расход) или Clicks (клики).
                    По умолчанию Cost.
    - top_n:        количество ключевых фраз в результате (по умолчанию 20)
    - date_from:    дата начала YYYY-MM-DD (только при date_range=CUSTOM_DATE)
    - date_to:      дата окончания YYYY-MM-DD (только при date_range=CUSTOM_DATE)
    - campaign_ids: ID кампаний через запятую для фильтрации (необязательно)
    - account:      псевдоним аккаунта Директа (необязательно)
    - client_login: логин клиента (для агентских аккаунтов, необязательно)

    Возвращает сырые данные в JSON. При ответе пользователю ВСЕГДА форматируй
    данные в виде читаемой таблицы Markdown на русском языке с нумерацией.
    Суммы расходов указаны в рублях без НДС.
    """
    lc = ctx.request_context.lifespan_context
    direct = resolve_direct_client(account, lc)
    if direct is None:
        return _no_direct_error(account)

    if date_range not in _VALID_RANGES:
        return json.dumps(
            {"error": f"Неверный date_range: «{date_range}». Допустимые: {', '.join(sorted(_VALID_RANGES))}."},
            ensure_ascii=False,
        )
    if sort_by not in ("Cost", "Clicks"):
        return json.dumps(
            {"error": "Параметр sort_by должен быть 'Cost' (расход) или 'Clicks' (клики)."},
            ensure_ascii=False,
        )

    parsed_ids, err = _parse_campaign_ids(campaign_ids)
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    try:
        rows = await direct.get_report(
            # В CUSTOM_REPORT поле Keyword невалидно (ошибка 4000). Используем
            # Criterion + CriterionType — это современная схема Direct, где
            # ключевая фраза — один из типов таргетинга (KEYWORD).
            field_names=[
                "Criterion", "CriterionType", "CriterionId",
                "CampaignId", "CampaignName",
                "Clicks", "Impressions", "Cost", "Ctr", "AvgCpc",
            ],
            date_range_type=date_range,
            report_name="keywords",
            date_from=date_from,
            date_to=date_to,
            campaign_ids=parsed_ids,
            order_by=sort_by,
            top_n=None,  # top_n применим после фильтра по CriterionType
            client_login=client_login,
        )
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    if not rows:
        return json.dumps(
            {"error": f"Нет данных по ключевым фразам за период {date_range}."},
            ensure_ascii=False,
        )

    # Оставляем только KEYWORD — без автотаргетов, ретаргетинга и т.п.
    keyword_rows = [r for r in rows if r.get("CriterionType") == "KEYWORD"] or rows

    keywords = []
    for row in keyword_rows[:top_n]:
        m = format_metrics(row)
        keywords.append({
            "keyword": row.get("Criterion", "—"),
            "criterion_type": row.get("CriterionType", "—"),
            "criterion_id": row.get("CriterionId", "—"),
            "campaign_id": row.get("CampaignId", "—"),
            "campaign_name": row.get("CampaignName", "—"),
            "clicks": _safe_int(m.get("Clicks")),
            "impressions": _safe_int(m.get("Impressions")),
            "cost_rub": _safe_float(m.get("Cost")),
            "ctr_pct": _safe_float(m.get("Ctr")),
            "avg_cpc_rub": _safe_float(m.get("AvgCpc")),
        })

    # Финальная сортировка (API уже сортирует, но перестрахуемся)
    sort_key = "cost_rub" if sort_by == "Cost" else "clicks"
    keywords.sort(key=lambda x: x[sort_key], reverse=True)

    result: dict = {
        "account": account or "primary",
        "date_range": date_range,
        "sort_by": sort_by,
        "returned_rows": len(keywords),
        "keywords": keywords,
    }
    if date_from and date_to:
        result["period"] = {"from": date_from, "to": date_to}
    if parsed_ids:
        result["filtered_campaign_ids"] = parsed_ids

    warning = direct.units_warning()
    if warning:
        result["_units_warning"] = warning

    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
async def get_direct_search_queries(
    ctx: Context,
    date_range: str = "LAST_30_DAYS",
    sort_by: str = "Cost",
    top_n: int = 30,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    campaign_ids: Optional[str] = None,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Получить отчёт по реальным поисковым запросам пользователей, по которым
    показывались объявления (SEARCH_QUERY_PERFORMANCE_REPORT).

    Позволяет найти:
    - Запросы с кликами, но без конверсий — кандидаты в минус-слова.
    - Запросы с высоким CTR и низким CPC — кандидаты в новые ключевые фразы.
    - Нерелевантные запросы, по которым реклама показывается ошибочно.

    Параметры:
    - date_range:   LAST_7_DAYS, LAST_30_DAYS, THIS_MONTH, LAST_MONTH, CUSTOM_DATE
    - sort_by:      Cost (расход) или Clicks (клики). По умолчанию Cost.
    - top_n:        количество запросов в результате (по умолчанию 30)
    - date_from/to: даты YYYY-MM-DD при CUSTOM_DATE
    - campaign_ids: ID кампаний через запятую (необязательно)
    - account:      псевдоним аккаунта Директа (необязательно)
    - client_login: логин клиента для агентских аккаунтов (необязательно)

    Возвращает JSON. Суммы расходов в рублях без НДС.
    """
    lc = ctx.request_context.lifespan_context
    direct = resolve_direct_client(account, lc)
    if direct is None:
        return _no_direct_error(account)

    if date_range not in _VALID_RANGES:
        return json.dumps(
            {"error": f"Неверный date_range: «{date_range}». Допустимые: {', '.join(sorted(_VALID_RANGES))}."},
            ensure_ascii=False,
        )
    if sort_by not in ("Cost", "Clicks"):
        return json.dumps(
            {"error": "Параметр sort_by должен быть 'Cost' (расход) или 'Clicks' (клики)."},
            ensure_ascii=False,
        )

    parsed_ids, err = _parse_campaign_ids(campaign_ids)
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    try:
        rows = await direct.get_report(
            # Query — реальный запрос пользователя; Criterion — ключевая фраза, на которую сработало.
            field_names=[
                "Query", "Criterion", "CampaignId", "CampaignName",
                "Clicks", "Impressions", "Cost", "Ctr", "AvgCpc",
            ],
            date_range_type=date_range,
            report_name="search_queries",
            report_type="SEARCH_QUERY_PERFORMANCE_REPORT",
            date_from=date_from,
            date_to=date_to,
            campaign_ids=parsed_ids,
            order_by=sort_by,
            top_n=top_n,
            client_login=client_login,
        )
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    if not rows:
        return json.dumps(
            {"error": f"Нет данных по поисковым запросам за период {date_range}."},
            ensure_ascii=False,
        )

    queries = []
    for row in rows:
        m = format_metrics(row)
        queries.append({
            "query": row.get("Query", "—"),
            "matched_keyword": row.get("Criterion", "—"),
            "campaign_id": row.get("CampaignId", "—"),
            "campaign_name": row.get("CampaignName", "—"),
            "clicks": _safe_int(m.get("Clicks")),
            "impressions": _safe_int(m.get("Impressions")),
            "cost_rub": _safe_float(m.get("Cost")),
            "ctr_pct": _safe_float(m.get("Ctr")),
            "avg_cpc_rub": _safe_float(m.get("AvgCpc")),
        })

    sort_key = "cost_rub" if sort_by == "Cost" else "clicks"
    queries.sort(key=lambda x: x[sort_key], reverse=True)

    result: dict = {
        "account": account or "primary",
        "date_range": date_range,
        "sort_by": sort_by,
        "returned_rows": len(queries),
        "search_queries": queries,
    }
    if date_from and date_to:
        result["period"] = {"from": date_from, "to": date_to}
    if parsed_ids:
        result["filtered_campaign_ids"] = parsed_ids

    warning = direct.units_warning()
    if warning:
        result["_units_warning"] = warning

    return json.dumps(result, ensure_ascii=False, indent=2)
