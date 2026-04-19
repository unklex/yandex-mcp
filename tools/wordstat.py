"""
Инструменты Яндекс.Wordstat — исследование ключевых фраз.

Использует legacy Direct API v4 (https://api.direct.yandex.ru/live/v4/json/)
с асинхронной polling-моделью:
  1. CreateNewWordstatReport → возвращает report_id
  2. GetWordstatReport       → опрашиваем до статуса не-pending

Инструменты:
  - wordstat_top_requests  — топ смежных запросов (SearchedWith) по фразам
  - wordstat_dynamics      — помесячная динамика показов (MonthList)
  - wordstat_regions       — распределение показов по регионам (GeoList)

Лимиты Wordstat: ~1000 отчётов в сутки на токен; один отчёт принимает до
10 фраз и готовится 3–30 секунд.
"""

from __future__ import annotations

import json
import re
from typing import Any, Optional

from mcp.server.fastmcp import Context

from app import mcp, resolve_direct_client
from direct_client import DirectAPIError


def _no_direct_error(account: str | None = None) -> str:
    msg = "Клиент Яндекс.Директа не инициализирован."
    if account:
        msg += f" Аккаунт «{account}» не найден в YANDEX_DIRECT_ACCOUNTS."
    msg += " Проверьте переменные окружения YANDEX_DIRECT_ACCOUNTS или YANDEX_DIRECT_TOKEN."
    return json.dumps({"error": msg}, ensure_ascii=False)


def _parse_csv(raw: str | None) -> list[str]:
    """Разбор строковых CSV (запятая/точка с запятой), с удалением дублей."""
    if not raw:
        return []
    parts = re.split(r"[,;]\s*", raw)
    seen: set[str] = set()
    out: list[str] = []
    for p in parts:
        s = p.strip()
        if not s:
            continue
        low = s.lower()
        if low in seen:
            continue
        seen.add(low)
        out.append(s)
    return out


def _parse_int_list(raw: str | None, param: str) -> tuple[list[int] | None, str | None]:
    if not raw:
        return None, None
    try:
        ids = [int(x.strip()) for x in re.split(r"[,;]\s*", raw) if x.strip()]
        return (ids or None), None
    except ValueError:
        return None, f"Параметр {param} должен содержать целые числа через запятую (например, '213,2').";


async def _create_and_poll(
    direct,
    phrases: list[str],
    geo_ids: list[int] | None,
    client_login: str | None,
) -> list[dict[str, Any]]:
    """
    Создаёт Wordstat-отчёт и ожидает готовности. Возвращает data-массив.
    Обрабатывает обе формы ответа v4: {"data": report_id} на create.
    """
    param: dict[str, Any] = {"Phrases": phrases}
    if geo_ids:
        param["GeoID"] = geo_ids

    create_resp = await direct._wordstat_request(
        "CreateNewWordstatReport", param, client_login
    )
    report_id = create_resp.get("data") if isinstance(create_resp, dict) else None
    if not isinstance(report_id, (int, str)) or not str(report_id).strip():
        raise DirectAPIError(
            0,
            f"Wordstat: не удалось создать отчёт, некорректный ответ: {create_resp}",
        )

    try:
        report_id_int = int(report_id)
    except (TypeError, ValueError) as exc:
        raise DirectAPIError(0, f"Wordstat: некорректный report_id: {report_id}") from exc

    return await direct._wordstat_poll(report_id_int, client_login=client_login)


@mcp.tool()
async def wordstat_top_requests(
    ctx: Context,
    phrases: str,
    geo_ids: Optional[str] = None,
    limit: int = 50,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Получить топ смежных поисковых запросов по заданным фразам через Wordstat.

    Для каждой переданной фразы возвращает список «что ещё искали» (SearchedWith)
    с месячными показами. Полезно для расширения семантики и поиска минус-слов.

    Параметры:
    - phrases:      фразы через запятую (до 10 штук). Пример:
                    'купить диван, диван москва'
    - geo_ids:      ID регионов Яндекса через запятую (пример: '213' — Москва,
                    '225' — Россия). По умолчанию — все регионы.
    - limit:        макс. кол-во смежных запросов на каждую фразу (по умолчанию 50)
    - account:      псевдоним аккаунта Директа (необязательно)
    - client_login: логин клиента для агентских аккаунтов (необязательно)

    Лимиты Wordstat: ~1000 отчётов/сутки на токен; отчёт строится 3–30 секунд.
    Возвращает JSON со списком {phrase, shows, top_associated: [{phrase, shows}]}.
    """
    lc = ctx.request_context.lifespan_context
    direct = resolve_direct_client(account, lc)
    if direct is None:
        return _no_direct_error(account)

    parsed_phrases = _parse_csv(phrases)
    if not parsed_phrases:
        return json.dumps({"error": "Параметр phrases пуст."}, ensure_ascii=False)
    if len(parsed_phrases) > 10:
        return json.dumps(
            {"error": f"Wordstat принимает не более 10 фраз за запрос. Получено: {len(parsed_phrases)}."},
            ensure_ascii=False,
        )

    parsed_geo, err = _parse_int_list(geo_ids, "geo_ids")
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    try:
        data = await _create_and_poll(direct, parsed_phrases, parsed_geo, client_login)
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    entries: list[dict] = []
    for item in data:
        searched_with = item.get("SearchedWith") or []
        assoc = [
            {
                "phrase": sw.get("Phrase", ""),
                "shows": int(sw.get("Shows") or 0),
            }
            for sw in searched_with
            if isinstance(sw, dict)
        ]
        assoc.sort(key=lambda x: x["shows"], reverse=True)
        entries.append({
            "phrase": item.get("Phrase", ""),
            "shows": int(item.get("Shows") or 0),
            "associated_count": len(assoc),
            "top_associated": assoc[:limit],
        })
    entries.sort(key=lambda x: x["shows"], reverse=True)

    result: dict = {
        "account": account or "primary",
        "phrases": parsed_phrases,
        "geo_ids": parsed_geo if parsed_geo else "all_russia",
        "returned_phrases": len(entries),
        "results": entries,
    }
    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
async def wordstat_dynamics(
    ctx: Context,
    phrase: str,
    geo_ids: Optional[str] = None,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Получить помесячную динамику показов для одной фразы через Wordstat.

    Возвращает данные за последние ~24 месяца. Позволяет оценить сезонность
    и тренды спроса (например, «купить шины» — сезонный пик в октябре).

    Параметры:
    - phrase:       одна фраза для анализа. Пример: 'вывоз мусора москва'.
    - geo_ids:      ID регионов Яндекса через запятую (по умолчанию — все)
    - account:      псевдоним аккаунта Директа (необязательно)
    - client_login: логин клиента для агентских аккаунтов (необязательно)

    Возвращает JSON: {phrase, total_shows, months_count, monthly_data: [
    {year, month, shows}], summary: {min_shows, max_shows, avg_shows}}.
    При ответе форматируй динамику как таблицу по месяцам (хронологически).
    """
    lc = ctx.request_context.lifespan_context
    direct = resolve_direct_client(account, lc)
    if direct is None:
        return _no_direct_error(account)

    phrase_str = (phrase or "").strip()
    if not phrase_str:
        return json.dumps({"error": "Параметр phrase пуст."}, ensure_ascii=False)

    parsed_geo, err = _parse_int_list(geo_ids, "geo_ids")
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    try:
        data = await _create_and_poll(direct, [phrase_str], parsed_geo, client_login)
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    if not data:
        return json.dumps(
            {"error": f"Wordstat не вернул данных для фразы «{phrase_str}»."},
            ensure_ascii=False,
        )

    first = data[0] if isinstance(data[0], dict) else {}
    monthly: list[dict] = []
    for m in first.get("MonthList") or []:
        if not isinstance(m, dict):
            continue
        monthly.append({
            "year": int(m.get("Year") or 0),
            "month": int(m.get("Month") or 0),
            "shows": int(m.get("Shows") or 0),
        })
    monthly.sort(key=lambda x: (x["year"], x["month"]))

    shows_list = [m["shows"] for m in monthly]
    summary = {
        "min_shows": min(shows_list) if shows_list else 0,
        "max_shows": max(shows_list) if shows_list else 0,
        "avg_shows": round(sum(shows_list) / len(shows_list), 1) if shows_list else 0.0,
    }

    result: dict = {
        "account": account or "primary",
        "phrase": first.get("Phrase", phrase_str),
        "geo_ids": parsed_geo if parsed_geo else "all_russia",
        "total_shows": int(first.get("Shows") or 0),
        "months_count": len(monthly),
        "monthly_data": monthly,
        "summary": summary,
    }
    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
async def wordstat_regions(
    ctx: Context,
    phrase: str,
    limit: int = 30,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Получить распределение показов по регионам для одной фразы через Wordstat.

    Показывает топ регионов, из которых пользователи ищут данную фразу,
    с долей от общего числа показов (ShowsPercent). Полезно для географической
    оптимизации таргетинга.

    Параметры:
    - phrase:       одна фраза для анализа
    - limit:        топ-N регионов в результате (по умолчанию 30)
    - account:      псевдоним аккаунта Директа (необязательно)
    - client_login: логин клиента для агентских аккаунтов (необязательно)

    ВНИМАНИЕ: запрос делается БЕЗ geo-фильтра — Wordstat возвращает GeoList
    только в таком режиме. Для распределения в конкретном регионе сузьте
    фразу вручную (например, 'диван москва' вместо 'диван').

    Возвращает JSON со списком {region_id, region_name, shows, percent}
    по убыванию показов.
    """
    lc = ctx.request_context.lifespan_context
    direct = resolve_direct_client(account, lc)
    if direct is None:
        return _no_direct_error(account)

    phrase_str = (phrase or "").strip()
    if not phrase_str:
        return json.dumps({"error": "Параметр phrase пуст."}, ensure_ascii=False)

    try:
        data = await _create_and_poll(direct, [phrase_str], None, client_login)
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    if not data:
        return json.dumps(
            {"error": f"Wordstat не вернул данных для фразы «{phrase_str}»."},
            ensure_ascii=False,
        )

    first = data[0] if isinstance(data[0], dict) else {}
    geo_list = first.get("GeoList") or []
    regions: list[dict] = []
    for g in geo_list:
        if not isinstance(g, dict):
            continue
        regions.append({
            "region_id": int(g.get("GeoID") or 0),
            "region_name": g.get("GeoName") or g.get("RegionName", ""),
            "shows": int(g.get("Shows") or 0),
            "percent": round(float(g.get("ShowsPercent") or 0), 2),
        })
    regions.sort(key=lambda x: x["shows"], reverse=True)
    regions = regions[:limit]

    result: dict = {
        "account": account or "primary",
        "phrase": first.get("Phrase", phrase_str),
        "total_shows": int(first.get("Shows") or 0),
        "returned_regions": len(regions),
        "regions": regions,
    }
    return json.dumps(result, ensure_ascii=False, indent=2)
