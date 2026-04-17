"""
Инструменты Яндекс.Директ — разрезы статистики из Reports API.

Инструменты:
  - get_direct_stats_by_day       — динамика по дням: клики, показы, расход
  - get_direct_stats_by_region    — статистика по регионам/городам
  - get_direct_stats_by_device    — разбивка по устройствам (desktop/mobile/tablet)
  - get_direct_stats_by_placement — поиск vs РСЯ (сеть)
"""

from __future__ import annotations

import json
from typing import Optional

from mcp.server.fastmcp import Context

from app import mcp, resolve_direct_client
from direct_client import DirectAPIError

_MICRO_FIELDS = frozenset({"Cost", "CostPerConversion", "Revenue", "AvgCpc"})
_ROUND2_FIELDS = frozenset({"Ctr", "ConversionRate", "GoalsRoi"})
_VALID_RANGES = frozenset({"LAST_7_DAYS", "LAST_30_DAYS", "THIS_MONTH", "LAST_MONTH", "CUSTOM_DATE"})

_NETWORK_LABELS: dict[str, str] = {
    "SEARCH": "Поиск",
    "AD_NETWORK": "РСЯ (рекламная сеть)",
    "UNKNOWN": "Неизвестно",
}

_DEVICE_LABELS: dict[str, str] = {
    "DESKTOP": "Компьютер",
    "MOBILE": "Смартфон",
    "TABLET": "Планшет",
    "OTHER": "Другое",
    "UNKNOWN": "Неизвестно",
}


def _fmt(row: dict[str, str]) -> dict[str, float | str]:
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
    if not campaign_ids:
        return None, None
    try:
        ids = [int(x.strip()) for x in campaign_ids.split(",") if x.strip()]
        return ids, None
    except ValueError:
        return None, "Параметр campaign_ids должен содержать целые числа через запятую (например, '123,456')."


@mcp.tool()
async def get_direct_stats_by_day(
    ctx: Context,
    date_range: str = "LAST_30_DAYS",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    campaign_ids: Optional[str] = None,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Получить динамику рекламы Яндекс.Директа по дням: клики, показы, расход, CTR, CPC.

    Используй для анализа трендов, выявления провалов/пиков, сравнения дней.

    Параметры:
    - date_range:   LAST_7_DAYS, LAST_30_DAYS, THIS_MONTH, LAST_MONTH, CUSTOM_DATE
    - date_from:    дата начала YYYY-MM-DD (только при CUSTOM_DATE)
    - date_to:      дата окончания YYYY-MM-DD (только при CUSTOM_DATE)
    - campaign_ids: ID кампаний через запятую (необязательно)
    - account:      псевдоним аккаунта Директа (необязательно)
    - client_login: логин клиента для агентских аккаунтов (необязательно)

    Возвращает JSON. При ответе форматируй в таблицу Markdown по дате хронологически.
    Суммы расходов в рублях без НДС.
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
            field_names=["Date", "Clicks", "Impressions", "Cost", "Ctr", "AvgCpc", "Conversions"],
            date_range_type=date_range,
            report_name="stats_by_day",
            date_from=date_from,
            date_to=date_to,
            campaign_ids=parsed_ids,
            order_by="Date",
            client_login=client_login,
        )
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    if not rows:
        return json.dumps({"error": f"Нет данных за период {date_range}."}, ensure_ascii=False)

    days = []
    for row in rows:
        m = _fmt(row)
        days.append({
            "date": row.get("Date", "—"),
            "clicks": _safe_int(m.get("Clicks")),
            "impressions": _safe_int(m.get("Impressions")),
            "cost_rub": _safe_float(m.get("Cost")),
            "ctr_pct": _safe_float(m.get("Ctr")),
            "avg_cpc_rub": _safe_float(m.get("AvgCpc")),
            "conversions": _safe_int(m.get("Conversions")),
        })

    days.sort(key=lambda x: x["date"])

    result: dict = {
        "account": account or "primary",
        "date_range": date_range,
        "days_count": len(days),
        "totals": {
            "clicks": sum(d["clicks"] for d in days),
            "cost_rub": round(sum(d["cost_rub"] for d in days), 2),
            "conversions": sum(d["conversions"] for d in days),
        },
        "by_day": days,
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
async def get_direct_stats_by_region(
    ctx: Context,
    date_range: str = "LAST_30_DAYS",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    campaign_ids: Optional[str] = None,
    top_n: int = 20,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Получить статистику рекламы Яндекс.Директа по регионам/городам.

    Показывает, из каких регионов приходят клики, где выше CTR и ниже CPC.
    Помогает оптимизировать географический таргетинг и корректировки ставок по регионам.

    Параметры:
    - date_range:   LAST_7_DAYS, LAST_30_DAYS, THIS_MONTH, LAST_MONTH, CUSTOM_DATE
    - date_from:    дата начала YYYY-MM-DD (только при CUSTOM_DATE)
    - date_to:      дата окончания YYYY-MM-DD (только при CUSTOM_DATE)
    - campaign_ids: ID кампаний через запятую (необязательно)
    - top_n:        количество регионов в результате (по умолчанию 20)
    - account:      псевдоним аккаунта Директа (необязательно)
    - client_login: логин клиента для агентских аккаунтов (необязательно)

    Возвращает JSON. При ответе форматируй в таблицу Markdown по расходу убыванием.
    Суммы расходов в рублях без НДС.
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
                "LocationOfPresenceName", "LocationOfPresenceId",
                "Clicks", "Impressions", "Cost", "Ctr", "AvgCpc", "Conversions",
            ],
            date_range_type=date_range,
            report_name="stats_by_region",
            date_from=date_from,
            date_to=date_to,
            campaign_ids=parsed_ids,
            order_by="Cost",
            top_n=top_n * 3,
            client_login=client_login,
        )
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    if not rows:
        return json.dumps({"error": f"Нет данных по регионам за период {date_range}."}, ensure_ascii=False)

    regions = []
    for row in rows:
        m = _fmt(row)
        regions.append({
            "region": row.get("LocationOfPresenceName", "—"),
            "region_id": row.get("LocationOfPresenceId", "—"),
            "clicks": _safe_int(m.get("Clicks")),
            "impressions": _safe_int(m.get("Impressions")),
            "cost_rub": _safe_float(m.get("Cost")),
            "ctr_pct": _safe_float(m.get("Ctr")),
            "avg_cpc_rub": _safe_float(m.get("AvgCpc")),
            "conversions": _safe_int(m.get("Conversions")),
        })

    regions.sort(key=lambda x: x["cost_rub"], reverse=True)
    regions = regions[:top_n]

    result: dict = {
        "account": account or "primary",
        "date_range": date_range,
        "returned_rows": len(regions),
        "by_region": regions,
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
async def get_direct_stats_by_device(
    ctx: Context,
    date_range: str = "LAST_30_DAYS",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    campaign_ids: Optional[str] = None,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Получить статистику рекламы Яндекс.Директа по устройствам (desktop/mobile/tablet).

    Показывает разницу в CTR, CPC и конверсиях между устройствами.
    Помогает настроить корректировки ставок по типу устройства.

    Параметры:
    - date_range:   LAST_7_DAYS, LAST_30_DAYS, THIS_MONTH, LAST_MONTH, CUSTOM_DATE
    - date_from:    дата начала YYYY-MM-DD (только при CUSTOM_DATE)
    - date_to:      дата окончания YYYY-MM-DD (только при CUSTOM_DATE)
    - campaign_ids: ID кампаний через запятую (необязательно)
    - account:      псевдоним аккаунта Директа (необязательно)
    - client_login: логин клиента для агентских аккаунтов (необязательно)

    Возвращает JSON. При ответе форматируй в таблицу Markdown.
    Суммы расходов в рублях без НДС.
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
            field_names=["Device", "Clicks", "Impressions", "Cost", "Ctr", "AvgCpc", "Conversions"],
            date_range_type=date_range,
            report_name="stats_by_device",
            date_from=date_from,
            date_to=date_to,
            campaign_ids=parsed_ids,
            client_login=client_login,
        )
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    if not rows:
        return json.dumps({"error": f"Нет данных по устройствам за период {date_range}."}, ensure_ascii=False)

    devices = []
    for row in rows:
        m = _fmt(row)
        device_code = row.get("Device", "UNKNOWN")
        devices.append({
            "device": device_code,
            "device_label": _DEVICE_LABELS.get(device_code, device_code),
            "clicks": _safe_int(m.get("Clicks")),
            "impressions": _safe_int(m.get("Impressions")),
            "cost_rub": _safe_float(m.get("Cost")),
            "ctr_pct": _safe_float(m.get("Ctr")),
            "avg_cpc_rub": _safe_float(m.get("AvgCpc")),
            "conversions": _safe_int(m.get("Conversions")),
        })

    devices.sort(key=lambda x: x["cost_rub"], reverse=True)

    result: dict = {
        "account": account or "primary",
        "date_range": date_range,
        "by_device": devices,
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
async def get_direct_stats_by_placement(
    ctx: Context,
    date_range: str = "LAST_30_DAYS",
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    campaign_ids: Optional[str] = None,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Получить статистику рекламы Яндекс.Директа по типу площадки: поиск vs РСЯ.

    Показывает разницу в эффективности между поисковыми и сетевыми размещениями.
    Помогает понять, какой канал даёт лучший ROI и стоимость конверсии.

    Параметры:
    - date_range:   LAST_7_DAYS, LAST_30_DAYS, THIS_MONTH, LAST_MONTH, CUSTOM_DATE
    - date_from:    дата начала YYYY-MM-DD (только при CUSTOM_DATE)
    - date_to:      дата окончания YYYY-MM-DD (только при CUSTOM_DATE)
    - campaign_ids: ID кампаний через запятую (необязательно)
    - account:      псевдоним аккаунта Директа (необязательно)
    - client_login: логин клиента для агентских аккаунтов (необязательно)

    Возвращает JSON. При ответе форматируй в таблицу Markdown.
    Суммы расходов и стоимость конверсии в рублях без НДС.
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
                "AdNetworkType", "Clicks", "Impressions", "Cost",
                "Ctr", "AvgCpc", "Conversions", "CostPerConversion",
            ],
            date_range_type=date_range,
            report_name="stats_by_placement",
            date_from=date_from,
            date_to=date_to,
            campaign_ids=parsed_ids,
            client_login=client_login,
        )
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    if not rows:
        return json.dumps({"error": f"Нет данных по площадкам за период {date_range}."}, ensure_ascii=False)

    placements = []
    for row in rows:
        m = _fmt(row)
        net_code = row.get("AdNetworkType", "UNKNOWN")
        placements.append({
            "placement": net_code,
            "placement_label": _NETWORK_LABELS.get(net_code, net_code),
            "clicks": _safe_int(m.get("Clicks")),
            "impressions": _safe_int(m.get("Impressions")),
            "cost_rub": _safe_float(m.get("Cost")),
            "ctr_pct": _safe_float(m.get("Ctr")),
            "avg_cpc_rub": _safe_float(m.get("AvgCpc")),
            "conversions": _safe_int(m.get("Conversions")),
            "cost_per_conversion_rub": _safe_float(m.get("CostPerConversion")),
        })

    placements.sort(key=lambda x: x["cost_rub"], reverse=True)

    result: dict = {
        "account": account or "primary",
        "date_range": date_range,
        "by_placement": placements,
    }
    if date_from and date_to:
        result["period"] = {"from": date_from, "to": date_to}
    if parsed_ids:
        result["filtered_campaign_ids"] = parsed_ids

    warning = direct.units_warning()
    if warning:
        result["_units_warning"] = warning

    return json.dumps(result, ensure_ascii=False, indent=2)
