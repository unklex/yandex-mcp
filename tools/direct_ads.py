"""
Инструменты Яндекс.Директ — объявления, группы объявлений, ставки.

Инструменты:
  - get_direct_ads       — список объявлений с текстами и статусами
  - get_direct_adgroups  — список групп объявлений
  - get_direct_bids      — текущие ставки по ключевым словам
"""

from __future__ import annotations

import json
from typing import Optional

from mcp.server.fastmcp import Context

from app import mcp, resolve_direct_client
from direct_client import DirectAPIError

_AD_STATUS_LABELS: dict[str, str] = {
    "ACCEPTED": "Принято",
    "DRAFT": "Черновик",
    "MODERATION": "На модерации",
    "REJECTED": "Отклонено",
    "UNKNOWN": "Неизвестно",
}

_AD_STATE_LABELS: dict[str, str] = {
    "ON": "Активно",
    "OFF": "Остановлено",
    "SUSPENDED": "Приостановлено",
    "OFF_BY_MONITORING": "Остановлено мониторингом",
    "UNKNOWN": "Неизвестно",
}

_ADGROUP_STATUS_LABELS: dict[str, str] = {
    "ACCEPTED": "Принята",
    "DRAFT": "Черновик",
    "MODERATION": "На модерации",
    "REJECTED": "Отклонена",
    "UNKNOWN": "Неизвестно",
}

_SERVING_STATUS_LABELS: dict[str, str] = {
    "ELIGIBLE": "Может показываться",
    "RARELY_SERVED": "Показывается редко",
    "UNKNOWN": "Неизвестно",
}

_PRIORITY_LABELS: dict[str, str] = {
    "LOW": "Низкий",
    "NORMAL": "Средний",
    "HIGH": "Высокий",
}


def _no_direct_error(account: str | None = None) -> str:
    msg = "Клиент Яндекс.Директа не инициализирован."
    if account:
        msg += f" Аккаунт «{account}» не найден в YANDEX_DIRECT_ACCOUNTS."
    msg += " Проверьте переменные окружения YANDEX_DIRECT_ACCOUNTS или YANDEX_DIRECT_TOKEN."
    return json.dumps({"error": msg}, ensure_ascii=False)


def _parse_ids(raw: str | None, param_name: str) -> tuple[list[int] | None, str | None]:
    if not raw:
        return None, None
    try:
        ids = [int(x.strip()) for x in raw.split(",") if x.strip()]
        return ids, None
    except ValueError:
        return None, f"Параметр {param_name} должен содержать целые числа через запятую (например, '123,456')."


@mcp.tool()
async def get_direct_ads(
    ctx: Context,
    campaign_ids: Optional[str] = None,
    adgroup_ids: Optional[str] = None,
    ad_ids: Optional[str] = None,
    statuses: Optional[str] = None,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Получить список объявлений Яндекс.Директа с текстами, заголовками и статусами.

    Необходимо указать хотя бы один из фильтров: campaign_ids, adgroup_ids или ad_ids.

    Параметры:
    - campaign_ids: ID кампаний через запятую (например, '123,456')
    - adgroup_ids:  ID групп объявлений через запятую
    - ad_ids:       ID конкретных объявлений через запятую
    - statuses:     фильтр по статусам через запятую:
                    ACCEPTED, DRAFT, MODERATION, REJECTED
    - account:      псевдоним аккаунта Директа (необязательно)
    - client_login: логин клиента для агентских аккаунтов (необязательно)

    Возвращает JSON. При ответе форматируй в таблицу Markdown на русском языке.
    Для текстовых объявлений показывай: заголовок, текст, ссылку, статус.
    """
    lc = ctx.request_context.lifespan_context
    direct = resolve_direct_client(account, lc)
    if direct is None:
        return _no_direct_error(account)

    camp_ids, err = _parse_ids(campaign_ids, "campaign_ids")
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    group_ids, err = _parse_ids(adgroup_ids, "adgroup_ids")
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    a_ids, err = _parse_ids(ad_ids, "ad_ids")
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    if not camp_ids and not group_ids and not a_ids:
        return json.dumps(
            {"error": "Укажите хотя бы один из параметров: campaign_ids, adgroup_ids или ad_ids."},
            ensure_ascii=False,
        )

    # Парсим статусы
    status_list = None
    if statuses:
        status_list = [s.strip().upper() for s in statuses.split(",") if s.strip()]

    try:
        data = await direct.get_ads(
            campaign_ids=camp_ids,
            adgroup_ids=group_ids,
            ad_ids=a_ids,
            statuses=status_list,
            client_login=client_login,
        )
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    raw_ads = data.get("result", {}).get("Ads", [])

    ads = []
    for ad in raw_ads:
        status = ad.get("Status", "UNKNOWN")
        state = ad.get("State", "UNKNOWN")
        ad_type = ad.get("Type", "UNKNOWN")

        entry: dict = {
            "id": ad.get("Id"),
            "adgroup_id": ad.get("AdGroupId"),
            "campaign_id": ad.get("CampaignId"),
            "type": ad_type,
            "status": status,
            "status_label": _AD_STATUS_LABELS.get(status, status),
            "state": state,
            "state_label": _AD_STATE_LABELS.get(state, state),
        }

        # Текстовое объявление
        text_ad = ad.get("TextAd")
        if text_ad:
            entry["title"] = text_ad.get("Title", "—")
            entry["title2"] = text_ad.get("Title2", "")
            entry["text"] = text_ad.get("Text", "—")
            entry["href"] = text_ad.get("Href", "—")
            entry["display_domain"] = text_ad.get("DisplayDomain", "")
            entry["mobile"] = text_ad.get("Mobile", "NO")

        # Динамическое текстовое объявление
        dyn_ad = ad.get("DynamicTextAd")
        if dyn_ad:
            entry["text"] = dyn_ad.get("Text", "—")

        ads.append(entry)

    result: dict = {
        "account": account or "primary",
        "total_ads": len(ads),
        "ads": ads,
    }
    if camp_ids:
        result["filtered_campaign_ids"] = camp_ids

    warning = direct.units_warning()
    if warning:
        result["_units_warning"] = warning

    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
async def get_direct_adgroups(
    ctx: Context,
    campaign_ids: Optional[str] = None,
    adgroup_ids: Optional[str] = None,
    statuses: Optional[str] = None,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Получить список групп объявлений Яндекс.Директа.

    Необходимо указать хотя бы один из фильтров: campaign_ids или adgroup_ids.

    Параметры:
    - campaign_ids: ID кампаний через запятую (например, '123,456')
    - adgroup_ids:  ID конкретных групп через запятую
    - statuses:     фильтр по статусам через запятую:
                    ACCEPTED, DRAFT, MODERATION, REJECTED
    - account:      псевдоним аккаунта Директа (необязательно)
    - client_login: логин клиента для агентских аккаунтов (необязательно)

    Возвращает JSON. При ответе форматируй в таблицу Markdown на русском языке.
    Показывай: ID группы, название, кампания, статус, статус показов.
    """
    lc = ctx.request_context.lifespan_context
    direct = resolve_direct_client(account, lc)
    if direct is None:
        return _no_direct_error(account)

    camp_ids, err = _parse_ids(campaign_ids, "campaign_ids")
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    group_ids, err = _parse_ids(adgroup_ids, "adgroup_ids")
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    if not camp_ids and not group_ids:
        return json.dumps(
            {"error": "Укажите хотя бы один из параметров: campaign_ids или adgroup_ids."},
            ensure_ascii=False,
        )

    status_list = None
    if statuses:
        status_list = [s.strip().upper() for s in statuses.split(",") if s.strip()]

    try:
        data = await direct.get_adgroups(
            campaign_ids=camp_ids,
            adgroup_ids=group_ids,
            statuses=status_list,
            client_login=client_login,
        )
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    raw_groups = data.get("result", {}).get("AdGroups", [])

    groups = []
    for g in raw_groups:
        status = g.get("Status", "UNKNOWN")
        serving = g.get("ServingStatus", "UNKNOWN")
        groups.append({
            "id": g.get("Id"),
            "name": g.get("Name", "—"),
            "campaign_id": g.get("CampaignId"),
            "type": g.get("Type", "UNKNOWN"),
            "status": status,
            "status_label": _ADGROUP_STATUS_LABELS.get(status, status),
            "serving_status": serving,
            "serving_status_label": _SERVING_STATUS_LABELS.get(serving, serving),
            "region_ids": g.get("RegionIds", []),
        })

    result: dict = {
        "account": account or "primary",
        "total_adgroups": len(groups),
        "adgroups": groups,
    }
    if camp_ids:
        result["filtered_campaign_ids"] = camp_ids

    warning = direct.units_warning()
    if warning:
        result["_units_warning"] = warning

    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
async def get_direct_bids(
    ctx: Context,
    campaign_ids: Optional[str] = None,
    adgroup_ids: Optional[str] = None,
    keyword_ids: Optional[str] = None,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Получить текущие ставки по ключевым словам в Яндекс.Директе.

    Если не указан ни один фильтр — возвращает ставки по всем активным ключевым словам аккаунта.
    Для больших аккаунтов рекомендуется фильтровать по campaign_ids или adgroup_ids.

    Параметры:
    - campaign_ids: ID кампаний через запятую (необязательно)
    - adgroup_ids:  ID групп объявлений через запятую (необязательно)
    - keyword_ids:  ID конкретных ключевых слов через запятую (необязательно)
    - account:      псевдоним аккаунта Директа (необязательно)
    - client_login: логин клиента для агентских аккаунтов (необязательно)

    Возвращает JSON. При ответе форматируй в таблицу Markdown на русском языке.
    Ставки указаны в рублях без НДС. Поля:
    - bid_rub:         ставка для поиска (руб.)
    - context_bid_rub: ставка для РСЯ (руб.)
    - priority:        приоритет ключевого слова (Низкий/Средний/Высокий)
    """
    lc = ctx.request_context.lifespan_context
    direct = resolve_direct_client(account, lc)
    if direct is None:
        return _no_direct_error(account)

    camp_ids, err = _parse_ids(campaign_ids, "campaign_ids")
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    group_ids, err = _parse_ids(adgroup_ids, "adgroup_ids")
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    kw_ids, err = _parse_ids(keyword_ids, "keyword_ids")
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    try:
        data = await direct.get_bids(
            campaign_ids=camp_ids,
            adgroup_ids=group_ids,
            keyword_ids=kw_ids,
            client_login=client_login,
        )
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    raw_bids = data.get("result", {}).get("Bids", [])

    bids = []
    for b in raw_bids:
        priority = b.get("StrategyPriority", "NORMAL")

        # Ставки в микро-рублях → рубли
        bid_micros = int(b.get("Bid") or 0)
        context_bid_micros = int(b.get("ContextBid") or 0)

        # AuctionBids — конкурентные ставки (может отсутствовать)
        auction = b.get("AuctionBids") or {}

        entry: dict = {
            "keyword_id": b.get("KeywordId"),
            "adgroup_id": b.get("AdGroupId"),
            "campaign_id": b.get("CampaignId"),
            "bid_rub": round(bid_micros / 1_000_000, 2),
            "context_bid_rub": round(context_bid_micros / 1_000_000, 2),
            "priority": priority,
            "priority_label": _PRIORITY_LABELS.get(priority, priority),
        }

        # Конкурентные ставки из аукциона (если есть)
        if auction:
            search_prices = auction.get("SearchPrices", [])
            if search_prices:
                # Берём первую позицию (обычно топ-1)
                top1 = next((p for p in search_prices if p.get("Position") == "P11"), None)
                if top1:
                    entry["auction_top1_rub"] = round(
                        int(top1.get("Price") or 0) / 1_000_000, 2
                    )
                    entry["auction_top1_traffic_volume"] = top1.get("TrafficVolume", 0)

        bids.append(entry)

    # Сортируем по ставке убыванием
    bids.sort(key=lambda x: x["bid_rub"], reverse=True)

    result: dict = {
        "account": account or "primary",
        "total_bids": len(bids),
        "bids": bids,
    }
    if camp_ids:
        result["filtered_campaign_ids"] = camp_ids
    if group_ids:
        result["filtered_adgroup_ids"] = group_ids

    warning = direct.units_warning()
    if warning:
        result["_units_warning"] = warning

    return json.dumps(result, ensure_ascii=False, indent=2)
