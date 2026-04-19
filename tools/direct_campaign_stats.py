"""
Инструменты Яндекс.Директ — детальная статистика кампаний через Reports API.

Инструменты:
  - get_campaign_stats  — CAMPAIGN_PERFORMANCE_REPORT по конкретным campaign_ids
                          за произвольный период с суммирующей строкой totals
  - get_custom_report   — универсальный произвольный Reports-отчёт
                          (любой report_type + произвольные field_names)

Оба инструмента используют DirectClient.get_report, который уже реализует
polling-модель Reports API (201/202 → retryIn → 200), фильтр по CampaignId
через Filter[] и UUID-суффикс в ReportName.
"""

from __future__ import annotations

import json
import re
from typing import Optional

from mcp.server.fastmcp import Context

from app import mcp, resolve_direct_client
from direct_client import DirectAPIError

# Микро-рубли → рубли (÷1 000 000). Округляем проценты/коэффициенты до 2 знаков.
_MICRO_FIELDS = frozenset({"Cost", "CostPerConversion", "Revenue", "AvgCpc"})
_ROUND2_FIELDS = frozenset({"Ctr", "ConversionRate", "GoalsRoi"})

_VALID_REPORT_TYPES = frozenset({
    "CAMPAIGN_PERFORMANCE_REPORT",
    "AD_PERFORMANCE_REPORT",
    "ADGROUP_PERFORMANCE_REPORT",
    "CRITERIA_PERFORMANCE_REPORT",
    "SEARCH_QUERY_PERFORMANCE_REPORT",
    "CUSTOM_REPORT",
    "ACCOUNT_PERFORMANCE_REPORT",
    "REACH_AND_FREQUENCY_PERFORMANCE_REPORT",
})

# Подсказки для get_custom_report — основные поля по каждому типу отчёта.
# Claude использует их в ответе, чтобы предложить пользователю осмысленный набор.
_SUGGESTED_FIELDS: dict[str, str] = {
    "CAMPAIGN_PERFORMANCE_REPORT":
        "CampaignId, CampaignName, CampaignType, Impressions, Clicks, Ctr, Cost, AvgCpc, "
        "Conversions, CostPerConversion, ConversionRate, Revenue",
    "AD_PERFORMANCE_REPORT":
        "AdId, AdFormat, CampaignId, CampaignName, AdGroupId, AdGroupName, "
        "Impressions, Clicks, Ctr, Cost, AvgCpc",
    "ADGROUP_PERFORMANCE_REPORT":
        "AdGroupId, AdGroupName, CampaignId, CampaignName, Impressions, Clicks, "
        "Ctr, Cost, AvgCpc, Conversions, CostPerConversion",
    "CRITERIA_PERFORMANCE_REPORT":
        "Criterion, CriterionType, CriterionId, CampaignId, CampaignName, AdGroupId, "
        "Impressions, Clicks, Ctr, Cost, AvgCpc, Conversions",
    "SEARCH_QUERY_PERFORMANCE_REPORT":
        "Query, Criterion, CampaignId, CampaignName, AdGroupId, "
        "Impressions, Clicks, Ctr, Cost, AvgCpc",
    "CUSTOM_REPORT":
        "любые поля из документации Direct Reports API FieldNames "
        "(например: Date, Device, AdNetworkType + метрики)",
    "ACCOUNT_PERFORMANCE_REPORT":
        "Impressions, Clicks, Ctr, Cost, AvgCpc, Conversions (без группировки)",
    "REACH_AND_FREQUENCY_PERFORMANCE_REPORT":
        "CampaignId, CampaignName, Impressions, ImpressionReach, Frequency, AvgImpressionFrequency",
}

_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _no_direct_error(account: str | None = None) -> str:
    msg = "Клиент Яндекс.Директа не инициализирован."
    if account:
        msg += f" Аккаунт «{account}» не найден в YANDEX_DIRECT_ACCOUNTS."
    msg += " Проверьте переменные окружения YANDEX_DIRECT_ACCOUNTS или YANDEX_DIRECT_TOKEN."
    return json.dumps({"error": msg}, ensure_ascii=False)


def _parse_ids(raw: str | None, param: str) -> tuple[list[int] | None, str | None]:
    if not raw:
        return None, None
    try:
        ids = [int(x.strip()) for x in re.split(r"[,;]\s*", raw) if x.strip()]
        return (ids or None), None
    except ValueError:
        return None, f"Параметр {param} должен содержать целые числа через запятую (например, '123,456')."


def _parse_fields(raw: str | None) -> list[str]:
    if not raw:
        return []
    return [f.strip() for f in re.split(r"[,;]\s*", raw) if f.strip()]


def _fmt_val(key: str, val) -> float | int | str:
    """
    Форматирование значения из TSV-строки Reports API:
    - денежные поля (Cost, AvgCpc, ...) — конвертируем микро-рубли в рубли
    - проценты/коэффициенты — округляем до 2 знаков
    - целые/дробные — приводим к int/float, нечисловое — оставляем строкой
    """
    try:
        f = float(val if val not in (None, "") else 0)
    except (ValueError, TypeError):
        return val if val is not None else ""

    if key in _MICRO_FIELDS:
        return round(f / 1_000_000, 2)
    if key in _ROUND2_FIELDS:
        return round(f, 2)
    if f.is_integer():
        return int(f)
    return f


@mcp.tool()
async def get_campaign_stats(
    ctx: Context,
    campaign_ids: str,
    date_from: str,
    date_to: str,
    include_vat: bool = True,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Получить детальную статистику по указанным кампаниям Яндекс.Директа
    за произвольный период (CAMPAIGN_PERFORMANCE_REPORT).

    Возвращаемые поля (в рублях): CampaignId, CampaignName, Impressions, Clicks,
    Ctr (%), Cost (руб.), AvgCpc (руб.), Conversions, CostPerConversion (руб.),
    ConversionRate (%).

    Параметры:
    - campaign_ids: ID кампаний через запятую (обязательно, например '123,456')
    - date_from:    дата начала YYYY-MM-DD (обязательно)
    - date_to:      дата окончания YYYY-MM-DD (обязательно)
    - include_vat:  учитывать ли НДС в суммах (по умолчанию True = с НДС)
    - account:      псевдоним аккаунта Директа (необязательно)
    - client_login: логин клиента для агентских аккаунтов (необязательно)

    Возвращает JSON со списком campaigns + строка totals (сумма по всем
    возвращённым кампаниям с пересчитанными CTR и средним CPC).
    Суммы автоматически конвертированы из микро-рублей в рубли.

    При ответе пользователю ВСЕГДА форматируй данные в таблицу Markdown
    на русском с нумерацией и итоговой строкой внизу.
    """
    lc = ctx.request_context.lifespan_context
    direct = resolve_direct_client(account, lc)
    if direct is None:
        return _no_direct_error(account)

    parsed_ids, err = _parse_ids(campaign_ids, "campaign_ids")
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)
    if not parsed_ids:
        return json.dumps(
            {"error": "Параметр campaign_ids обязателен. Укажите хотя бы один ID."},
            ensure_ascii=False,
        )

    if not _DATE_RE.match(date_from or ""):
        return json.dumps(
            {"error": f"Неверный формат date_from: «{date_from}». Ожидается YYYY-MM-DD."},
            ensure_ascii=False,
        )
    if not _DATE_RE.match(date_to or ""):
        return json.dumps(
            {"error": f"Неверный формат date_to: «{date_to}». Ожидается YYYY-MM-DD."},
            ensure_ascii=False,
        )

    try:
        rows = await direct.get_report(
            field_names=[
                "CampaignId", "CampaignName",
                "Impressions", "Clicks", "Ctr",
                "Cost", "AvgCpc",
                "Conversions", "CostPerConversion", "ConversionRate",
            ],
            date_range_type="CUSTOM_DATE",
            report_name=f"campaign_stats_{date_from}_{date_to}",
            report_type="CAMPAIGN_PERFORMANCE_REPORT",
            date_from=date_from,
            date_to=date_to,
            campaign_ids=parsed_ids,
            order_by="Cost",
            include_vat="YES" if include_vat else "NO",
            client_login=client_login,
        )
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    if not rows:
        return json.dumps(
            {"error": f"Нет данных за период {date_from} — {date_to} по указанным кампаниям."},
            ensure_ascii=False,
        )

    # Агрегируем totals в сырых единицах (микро-рубли, целые клики),
    # конвертируем в конце — избегает накопления ошибок округления.
    campaigns: list[dict] = []
    total_clicks = 0
    total_impressions = 0
    total_cost_micros = 0.0
    total_conversions = 0

    for row in rows:
        formatted = {k: _fmt_val(k, v) for k, v in row.items()}
        try:
            total_clicks += int(float(row.get("Clicks") or 0))
            total_impressions += int(float(row.get("Impressions") or 0))
            total_cost_micros += float(row.get("Cost") or 0)
            total_conversions += int(float(row.get("Conversions") or 0))
        except (ValueError, TypeError):
            pass
        campaigns.append(formatted)

    total_cost = round(total_cost_micros / 1_000_000, 2)
    totals = {
        "CampaignId": "TOTAL",
        "CampaignName": "Итого",
        "Impressions": total_impressions,
        "Clicks": total_clicks,
        "Ctr": round(total_clicks / total_impressions * 100, 2) if total_impressions else 0.0,
        "Cost": total_cost,
        "AvgCpc": round(total_cost / total_clicks, 2) if total_clicks else 0.0,
        "Conversions": total_conversions,
        "CostPerConversion": round(total_cost / total_conversions, 2) if total_conversions else 0.0,
    }

    result: dict = {
        "account": account or "primary",
        "period": {"from": date_from, "to": date_to},
        "include_vat": include_vat,
        "filtered_campaign_ids": parsed_ids,
        "rows_count": len(campaigns),
        "campaigns": campaigns,
        "totals": totals,
    }

    warning = direct.units_warning()
    if warning:
        result["_units_warning"] = warning

    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
async def get_custom_report(
    ctx: Context,
    report_type: str,
    fields: str,
    date_from: str,
    date_to: str,
    campaign_ids: Optional[str] = None,
    limit: int = 500,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Запустить произвольный отчёт Яндекс.Директа через Reports API.
    Гибкий универсальный инструмент для случаев, не покрытых специализированными.

    Поддерживаемые report_type и типовые поля (можно комбинировать):
    - CAMPAIGN_PERFORMANCE_REPORT:    CampaignId, CampaignName, CampaignType,
                                      Impressions, Clicks, Ctr, Cost, AvgCpc,
                                      Conversions, CostPerConversion, ConversionRate, Revenue
    - AD_PERFORMANCE_REPORT:          AdId, AdFormat, CampaignId, CampaignName,
                                      AdGroupId, AdGroupName,
                                      Impressions, Clicks, Ctr, Cost, AvgCpc
    - ADGROUP_PERFORMANCE_REPORT:     AdGroupId, AdGroupName, CampaignId, CampaignName,
                                      Impressions, Clicks, Ctr, Cost, AvgCpc,
                                      Conversions, CostPerConversion
    - CRITERIA_PERFORMANCE_REPORT:    Criterion, CriterionType, CriterionId,
                                      CampaignId, CampaignName, AdGroupId,
                                      Impressions, Clicks, Ctr, Cost, AvgCpc, Conversions
    - SEARCH_QUERY_PERFORMANCE_REPORT: Query, Criterion, CampaignId, CampaignName,
                                       AdGroupId, Impressions, Clicks, Ctr, Cost, AvgCpc
    - ACCOUNT_PERFORMANCE_REPORT:     общие метрики аккаунта без группировки
    - CUSTOM_REPORT:                  произвольный набор полей (в т.ч. Date, Device,
                                      AdNetworkType — для разрезов)

    Параметры:
    - report_type:  тип отчёта (см. список выше; регистр не важен)
    - fields:       поля через запятую. ВАЖНО: имена полей чувствительны к регистру.
                    Для дат используйте Date, Week, Month, Year.
    - date_from/to: YYYY-MM-DD (обязательно)
    - campaign_ids: фильтр по ID кампаний через запятую (необязательно)
    - limit:        максимум строк в ответе (по умолчанию 500)
    - account:      псевдоним аккаунта Директа (необязательно)
    - client_login: логин клиента для агентских аккаунтов (необязательно)

    Возвращает JSON с массивом rows и блоком _meta (report_type, fields, period,
    row_count, limit, filtered_campaign_ids). Суммы в микро-рублях автоматически
    конвертируются в рубли. При ответе пользователю форматируй в таблицу
    Markdown на русском.
    """
    lc = ctx.request_context.lifespan_context
    direct = resolve_direct_client(account, lc)
    if direct is None:
        return _no_direct_error(account)

    rt = (report_type or "").strip().upper()
    if rt not in _VALID_REPORT_TYPES:
        return json.dumps(
            {
                "error": f"Неверный report_type: «{report_type}». "
                         f"Допустимые: {', '.join(sorted(_VALID_REPORT_TYPES))}.",
            },
            ensure_ascii=False,
        )

    parsed_fields = _parse_fields(fields)
    if not parsed_fields:
        hint = _SUGGESTED_FIELDS.get(rt, "")
        return json.dumps(
            {"error": f"Параметр fields пуст. Пример полей для {rt}: {hint}"},
            ensure_ascii=False,
        )

    if not _DATE_RE.match(date_from or ""):
        return json.dumps(
            {"error": f"Неверный формат date_from: «{date_from}». Ожидается YYYY-MM-DD."},
            ensure_ascii=False,
        )
    if not _DATE_RE.match(date_to or ""):
        return json.dumps(
            {"error": f"Неверный формат date_to: «{date_to}». Ожидается YYYY-MM-DD."},
            ensure_ascii=False,
        )

    parsed_ids, err = _parse_ids(campaign_ids, "campaign_ids")
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    try:
        rows = await direct.get_report(
            field_names=parsed_fields,
            date_range_type="CUSTOM_DATE",
            report_name=f"custom_{rt}_{date_from}",
            report_type=rt,
            date_from=date_from,
            date_to=date_to,
            campaign_ids=parsed_ids,
            top_n=max(1, limit),
            client_login=client_login,
        )
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    formatted_rows = [{k: _fmt_val(k, v) for k, v in r.items()} for r in rows]

    meta: dict = {
        "report_type": rt,
        "fields": parsed_fields,
        "period": {"from": date_from, "to": date_to},
        "row_count": len(formatted_rows),
        "limit": limit,
    }
    if parsed_ids:
        meta["filtered_campaign_ids"] = parsed_ids

    result: dict = {
        "account": account or "primary",
        "_meta": meta,
        "rows": formatted_rows,
    }

    warning = direct.units_warning()
    if warning:
        result["_units_warning"] = warning

    return json.dumps(result, ensure_ascii=False, indent=2)
