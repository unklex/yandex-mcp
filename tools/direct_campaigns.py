"""
Инструменты Яндекс.Директ — кампании и бюджет.

Инструменты:
  - get_direct_campaigns          — список кампаний со статусом и дневным бюджетом
  - get_direct_top_campaigns      — топ-N кампаний по расходу или кликам за период
  - get_direct_budget             — остаток баллов API и сводка дневных бюджетов
  - add_direct_negative_keywords  — добавить/заменить минус-фразы на уровне кампании
"""

from __future__ import annotations

import json
import re
from typing import Optional

from mcp.server.fastmcp import Context

from app import mcp, resolve_direct_client
from direct_client import DirectAPIError

# Поля из Reports API, которые хранятся в микро-рублях (÷1_000_000 = рубли)
_MICRO_FIELDS = frozenset({"Cost", "CostPerConversion", "Revenue", "AvgCpc"})
# Поля, которые округляем до 2 знаков
_ROUND2_FIELDS = frozenset({"Ctr", "ConversionRate", "GoalsRoi"})

_STATUS_LABELS: dict[str, str] = {
    "ON": "Активна",
    "OFF": "Остановлена",
    "SUSPENDED": "Приостановлена",
    "ENDED": "Завершена",
    "CONVERTED": "Конвертирована",
    "ARCHIVED": "В архиве",
    "UNKNOWN": "Неизвестно",
}

_STATE_LABELS: dict[str, str] = {
    "ON": "Показы идут",
    "SUSPENDED": "Приостановлены пользователем",
    "OFF": "Выключены",
    "ENDED": "Кампания завершена",
    "CONVERTED": "Конвертирована в новый тип",
    "ARCHIVED": "В архиве",
    "UNKNOWN": "Неизвестно",
}

_VALID_RANGES = frozenset({"LAST_7_DAYS", "LAST_30_DAYS", "THIS_MONTH", "LAST_MONTH", "CUSTOM_DATE"})


def format_metrics(row: dict[str, str]) -> dict[str, float | str]:
    """
    Конвертирует денежные поля из микро-рублей в рубли (÷1,000,000).
    Округляет процентные/коэффициентные поля до 2 знаков.
    Остальные числовые поля — int или float без изменений.
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


def _no_direct_error(account: str | None = None) -> str:
    msg = "Клиент Яндекс.Директа не инициализирован."
    if account:
        msg += f" Аккаунт «{account}» не найден в YANDEX_DIRECT_ACCOUNTS."
    msg += " Проверьте переменные окружения YANDEX_DIRECT_ACCOUNTS или YANDEX_DIRECT_TOKEN."
    return json.dumps({"error": msg}, ensure_ascii=False)


@mcp.tool()
async def get_direct_campaigns(
    ctx: Context,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Получить список рекламных кампаний в Яндекс.Директе:
    ID, название, статус (на русском), состояние, дневной бюджет в рублях.

    Параметры:
    - account:      псевдоним аккаунта Директа (например, 'promreo' или 'site2').
                    Если не указан — используется основной аккаунт из конфига.
    - client_login: логин клиента (для агентских аккаунтов, необязательно)

    Возвращает сырые данные в JSON. При ответе пользователю ВСЕГДА форматируй
    данные в виде читаемой таблицы Markdown на русском языке.
    Суммы дневных бюджетов указаны в рублях без НДС.
    """
    lc = ctx.request_context.lifespan_context
    direct = resolve_direct_client(account, lc)
    if direct is None:
        return _no_direct_error(account)

    try:
        data = await direct.get_campaigns(client_login=client_login)
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    raw_campaigns = data.get("result", {}).get("Campaigns", [])

    campaigns = []
    for c in raw_campaigns:
        status = c.get("Status", "UNKNOWN")
        state = c.get("State", "UNKNOWN")
        daily_budget = c.get("DailyBudget") or {}
        amount_micros = int(daily_budget.get("Amount") or 0)
        campaigns.append({
            "id": c.get("Id"),
            "name": c.get("Name", "—"),
            "status": status,
            "status_label": _STATUS_LABELS.get(status, status),
            "state": state,
            "state_label": _STATE_LABELS.get(state, state),
            "daily_budget_rub": round(amount_micros / 1_000_000, 2),
            "daily_budget_mode": daily_budget.get("Mode", "—"),
        })

    result: dict = {
        "account": account or "primary",
        "total_campaigns": len(campaigns),
        "campaigns": campaigns,
    }

    warning = direct.units_warning()
    if warning:
        result["_units_warning"] = warning

    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
async def get_direct_top_campaigns(
    ctx: Context,
    date_range: str = "LAST_30_DAYS",
    sort_by: str = "Cost",
    top_n: int = 10,
    date_from: Optional[str] = None,
    date_to: Optional[str] = None,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Получить топ-N рекламных кампаний Яндекс.Директа по расходу или кликам за период.

    Параметры:
    - date_range:   период. Допустимые значения:
                    LAST_7_DAYS, LAST_30_DAYS, THIS_MONTH, LAST_MONTH, CUSTOM_DATE
    - sort_by:      метрика сортировки: Cost (расход, руб.) или Clicks (клики).
                    По умолчанию Cost.
    - top_n:        количество кампаний в результате (по умолчанию 10)
    - date_from:    дата начала YYYY-MM-DD (только при date_range=CUSTOM_DATE)
    - date_to:      дата окончания YYYY-MM-DD (только при date_range=CUSTOM_DATE)
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

    if sort_by not in ("Cost", "Clicks"):
        return json.dumps(
            {"error": "Параметр sort_by должен быть 'Cost' (расход) или 'Clicks' (клики)."},
            ensure_ascii=False,
        )
    if date_range not in _VALID_RANGES:
        return json.dumps(
            {"error": f"Неверный date_range: «{date_range}». Допустимые: {', '.join(sorted(_VALID_RANGES))}."},
            ensure_ascii=False,
        )

    try:
        rows = await direct.get_report(
            field_names=["CampaignId", "CampaignName", "Clicks", "Impressions", "Cost", "Ctr", "AvgCpc"],
            date_range_type=date_range,
            report_name="top_campaigns",
            date_from=date_from,
            date_to=date_to,
            order_by=sort_by,
            top_n=top_n * 3,  # запрашиваем с запасом для надёжной сортировки
            client_login=client_login,
        )
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    if not rows:
        return json.dumps(
            {"error": f"Нет данных по кампаниям за период {date_range}. Возможно, не было показов."},
            ensure_ascii=False,
        )

    campaigns = []
    for row in rows:
        m = format_metrics(row)
        campaigns.append({
            "campaign_id": row.get("CampaignId", "—"),
            "campaign_name": row.get("CampaignName", "—"),
            "clicks": int(m.get("Clicks", 0) or 0),
            "impressions": int(m.get("Impressions", 0) or 0),
            "cost_rub": m.get("Cost", 0.0),
            "ctr_pct": m.get("Ctr", 0.0),
            "avg_cpc_rub": m.get("AvgCpc", 0.0),
        })

    sort_key = "cost_rub" if sort_by == "Cost" else "clicks"
    campaigns.sort(key=lambda x: x[sort_key], reverse=True)
    campaigns = campaigns[:top_n]

    result: dict = {
        "account": account or "primary",
        "date_range": date_range,
        "sort_by": sort_by,
        "returned_rows": len(campaigns),
        "campaigns": campaigns,
    }
    if date_from and date_to:
        result["period"] = {"from": date_from, "to": date_to}

    warning = direct.units_warning()
    if warning:
        result["_units_warning"] = warning

    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
async def get_direct_budget(
    ctx: Context,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Получить сводку по бюджетам кампаний Яндекс.Директа и остатку баллов API.

    Показывает:
    - Суммарный дневной бюджет всех активных кампаний
    - Дневной бюджет каждой активной кампании
    - Остаток баллов API (Units): потрачено / доступно / дневной лимит
    - Предупреждение, если остаток баллов меньше 10%

    Параметры:
    - account:      псевдоним аккаунта Директа (необязательно)
    - client_login: логин клиента (для агентских аккаунтов, необязательно)

    Возвращает сырые данные в JSON. При ответе пользователю ВСЕГДА форматируй
    данные в виде читаемой таблицы Markdown на русском языке.
    Суммы указаны в рублях без НДС.
    """
    lc = ctx.request_context.lifespan_context
    direct = resolve_direct_client(account, lc)
    if direct is None:
        return _no_direct_error(account)

    try:
        data = await direct.get_campaigns(client_login=client_login)
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    raw_campaigns = data.get("result", {}).get("Campaigns", [])
    active = [c for c in raw_campaigns if c.get("Status") == "ON"]

    budgets = []
    total_daily = 0.0
    for c in active:
        db = c.get("DailyBudget") or {}
        amount_rub = round(int(db.get("Amount") or 0) / 1_000_000, 2)
        total_daily += amount_rub
        budgets.append({
            "campaign_id": c.get("Id"),
            "campaign_name": c.get("Name", "—"),
            "daily_budget_rub": amount_rub,
            "daily_budget_mode": db.get("Mode", "—"),
        })

    result: dict = {
        "account": account or "primary",
        "total_campaigns": len(raw_campaigns),
        "active_campaigns": len(active),
        "total_daily_budget_rub": round(total_daily, 2),
        "campaign_budgets": budgets,
    }

    units = direct.last_units
    if units:
        result["api_units"] = {
            "spent": units["spent"],
            "available": units["available"],
            "daily_limit": units["daily"],
            "remaining_pct": round(units["available"] / units["daily"] * 100, 1) if units["daily"] > 0 else 0.0,
        }

    warning = direct.units_warning()
    if warning:
        result["_units_warning"] = warning

    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
async def add_direct_negative_keywords(
    ctx: Context,
    campaign_id: int,
    keywords: str,
    mode: str = "append",
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Добавить или заменить минус-фразы на уровне рекламной кампании Яндекс.Директа.

    Поддерживаются типы кампаний: TEXT_CAMPAIGN (Текстово-графические),
    DYNAMIC_TEXT_CAMPAIGN (Динамические), UNIFIED_CAMPAIGN (Мастер кампаний /
    единая перформанс с автотаргетингом), SMART_CAMPAIGN (Смарт-баннеры),
    MOBILE_APP_CAMPAIGN (Реклама приложений), MCBANNER_CAMPAIGN (Медийная).

    Параметры:
    - campaign_id:  ID кампании (число). Получить можно через get_direct_campaigns.
    - keywords:     минус-фразы через запятую или точку с запятой.
                    Пример: 'диван, пианино, ресторан, вывоз бытовых'.
                    Допускаются пробелы вокруг; дубли игнорируются.
    - mode:         'append' (по умолчанию) — добавить к существующим без дублей
                    (case-insensitive), 'replace' — перезаписать весь список.
    - account:      псевдоним аккаунта Директа (необязательно)
    - client_login: логин клиента для агентских аккаунтов (необязательно)

    Лимиты Директа: до 1000 минус-фраз на кампанию, суммарно до 20 000 символов,
    каждая фраза — не более 7 слов. При нарушении API вернёт ошибку 400 с деталями.

    Возвращает JSON со сводкой: тип кампании, было/стало/добавлено, полный новый
    список. При ответе пользователю ВСЕГДА форматируй как читаемый отчёт Markdown
    на русском — покажи было N → стало M, добавленные фразы отдельным блоком.
    """
    lc = ctx.request_context.lifespan_context
    direct = resolve_direct_client(account, lc)
    if direct is None:
        return _no_direct_error(account)

    if mode not in ("append", "replace"):
        return json.dumps(
            {"error": f"Параметр mode должен быть 'append' или 'replace', получено: «{mode}»."},
            ensure_ascii=False,
        )

    # Разбор фраз: поддерживаем и запятые, и точки с запятой. Пустые — отбрасываем,
    # дубли внутри входа — тоже (первое вхождение).
    raw_parts = re.split(r"[,;]\s*", keywords or "")
    seen: set[str] = set()
    parsed: list[str] = []
    for p in raw_parts:
        s = p.strip()
        if not s:
            continue
        low = s.lower()
        if low in seen:
            continue
        seen.add(low)
        parsed.append(s)

    if not parsed:
        return json.dumps(
            {"error": "Параметр keywords пуст. Укажите минус-фразы через запятую."},
            ensure_ascii=False,
        )

    try:
        summary = await direct.set_campaign_negative_keywords(
            campaign_id=campaign_id,
            keywords=parsed,
            mode=mode,
            client_login=client_login,
        )
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    result: dict = {
        "account": account or "primary",
        **summary,
    }

    warning = direct.units_warning()
    if warning:
        result["_units_warning"] = warning

    return json.dumps(result, ensure_ascii=False, indent=2)
