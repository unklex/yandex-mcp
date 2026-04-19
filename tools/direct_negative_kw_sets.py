"""
Инструменты Яндекс.Директ — общие наборы минус-фраз (NegativeKeywordSharedSets).

Сервис: POST https://api.direct.yandex.com/json/v5/negativekeywordsharedsets

Инструменты:
  - get_negative_keyword_sets    — список наборов (все или по ID)
  - add_negative_keyword_set     — создать новый набор
  - update_negative_keyword_set  — изменить имя и/или полный список фраз
  - delete_negative_keyword_sets — удалить наборы по ID

Общие наборы позволяют задать минус-фразы один раз и привязать их к
нескольким кампаниям (через поле NegativeKeywordSharedSetIds у кампании).
Лимиты Директа: до 100 наборов на аккаунт, до 1000 фраз в наборе,
суммарно до 20 000 символов, каждая фраза — до 7 слов.
"""

from __future__ import annotations

import json
import re
from typing import Optional

from mcp.server.fastmcp import Context

from app import mcp, resolve_direct_client
from direct_client import DirectAPIError


def _no_direct_error(account: str | None = None) -> str:
    msg = "Клиент Яндекс.Директа не инициализирован."
    if account:
        msg += f" Аккаунт «{account}» не найден в YANDEX_DIRECT_ACCOUNTS."
    msg += " Проверьте переменные окружения YANDEX_DIRECT_ACCOUNTS или YANDEX_DIRECT_TOKEN."
    return json.dumps({"error": msg}, ensure_ascii=False)


def _parse_ids(raw: str | None, param: str) -> tuple[list[int] | None, str | None]:
    """Разбор 'id1, id2; id3' → [int, ...]. Возвращает (список_или_None, ошибка)."""
    if not raw:
        return None, None
    try:
        ids = [int(x.strip()) for x in re.split(r"[,;]\s*", raw) if x.strip()]
        return (ids or None), None
    except ValueError:
        return None, f"Параметр {param} должен содержать целые числа через запятую (например, '123,456')."


def _parse_keywords(raw: str | None) -> list[str]:
    """
    Разбор минус-фраз: поддерживаются запятые и точки с запятой.
    Дубли удаляются case-insensitive, порядок первого вхождения сохраняется.
    """
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


@mcp.tool()
async def get_negative_keyword_sets(
    ctx: Context,
    ids: Optional[str] = None,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Получить общие наборы минус-фраз (NegativeKeywordSharedSets) в аккаунте
    Яндекс.Директа.

    Параметры:
    - ids:          ID наборов через запятую для фильтрации (необязательно).
                    Если не указан — возвращаются все наборы.
    - account:      псевдоним аккаунта Директа (необязательно)
    - client_login: логин клиента для агентских аккаунтов (необязательно)

    Лимиты: до 100 наборов на аккаунт, до 1000 фраз в наборе, суммарно
    до 20 000 символов.

    Возвращает JSON со списком наборов: {id, name, keyword_count,
    associated_campaign_ids (привязанные кампании), associated_count,
    negative_keywords}. При ответе пользователю форматируй в таблицу
    Markdown на русском, показывая имя, кол-во фраз, кол-во привязанных
    кампаний и первые 5-10 фраз в столбце «Примеры».
    """
    lc = ctx.request_context.lifespan_context
    direct = resolve_direct_client(account, lc)
    if direct is None:
        return _no_direct_error(account)

    parsed_ids, err = _parse_ids(ids, "ids")
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)

    # FieldNames строго из enum Директа: Id, Name, NegativeKeywords, Associated.
    # SharedAccountId — невалидно (код 4000), keyword_count — локальная метрика.
    # Associated — массив ID кампаний, к которым привязан набор.
    params: dict = {
        "FieldNames": ["Id", "Name", "NegativeKeywords", "Associated"],
    }
    if parsed_ids:
        params["SelectionCriteria"] = {"Ids": parsed_ids}

    payload = {"method": "get", "params": params}

    try:
        data = await direct._post_negative_kw_sets(payload, client_login=client_login)
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    raw_sets = data.get("result", {}).get("NegativeKeywordSharedSets", []) or []
    sets_out = []
    for s in raw_sets:
        kws = s.get("NegativeKeywords") or []
        associated = s.get("Associated") or []
        sets_out.append({
            "id": s.get("Id"),
            "name": s.get("Name", ""),
            "keyword_count": len(kws),
            "associated_campaign_ids": associated,
            "associated_count": len(associated),
            "negative_keywords": kws,
        })

    result: dict = {
        "account": account or "primary",
        "total_sets": len(sets_out),
        "sets": sets_out,
    }
    if parsed_ids:
        result["filtered_ids"] = parsed_ids

    warning = direct.units_warning()
    if warning:
        result["_units_warning"] = warning

    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
async def add_negative_keyword_set(
    ctx: Context,
    name: str,
    keywords: str,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Создать новый общий набор минус-фраз в Яндекс.Директе.

    После создания набор можно привязать к кампаниям через поле
    NegativeKeywordSharedSetIds у кампании (отдельным вызовом campaigns.update).

    Параметры:
    - name:         имя набора (макс. 255 символов)
    - keywords:     минус-фразы через запятую или точку с запятой. Дубли
                    удаляются автоматически (case-insensitive). Пример:
                    'диван, пианино, ресторан, вывоз бытовых'.
    - account:      псевдоним аккаунта Директа (необязательно)
    - client_login: логин клиента для агентских аккаунтов (необязательно)

    Лимиты Директа: до 1000 фраз, суммарно до 20 000 символов, каждая фраза —
    до 7 слов. При нарушении API вернёт 400 с деталями.

    Возвращает JSON с {id, name, keyword_count, added_keywords} или ошибкой
    (в поле details — массив Errors от API).
    """
    lc = ctx.request_context.lifespan_context
    direct = resolve_direct_client(account, lc)
    if direct is None:
        return _no_direct_error(account)

    if not name or not name.strip():
        return json.dumps({"error": "Параметр name обязателен."}, ensure_ascii=False)

    parsed = _parse_keywords(keywords)
    if not parsed:
        return json.dumps({"error": "Параметр keywords пуст."}, ensure_ascii=False)

    payload = {
        "method": "add",
        "params": {
            "NegativeKeywordSharedSets": [
                {"Name": name.strip(), "NegativeKeywords": parsed}
            ]
        },
    }

    try:
        data = await direct._post_negative_kw_sets(payload, client_login=client_login)
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    results = data.get("result", {}).get("AddResults", []) or []
    if not results:
        return json.dumps({"error": "Нет AddResults в ответе API."}, ensure_ascii=False)

    first = results[0]
    if first.get("Errors"):
        return json.dumps(
            {"error": "Ошибка создания набора минус-фраз.", "details": first["Errors"]},
            ensure_ascii=False,
        )

    result: dict = {
        "account": account or "primary",
        "id": first.get("Id"),
        "name": name.strip(),
        "keyword_count": len(parsed),
        "added_keywords": parsed,
    }
    if first.get("Warnings"):
        result["warnings"] = first["Warnings"]

    warning = direct.units_warning()
    if warning:
        result["_units_warning"] = warning

    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
async def update_negative_keyword_set(
    ctx: Context,
    set_id: int,
    name: Optional[str] = None,
    keywords: Optional[str] = None,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Изменить общий набор минус-фраз.

    Позволяет обновить имя и/или полностью заменить список фраз. Минимум
    один из параметров name или keywords должен быть указан.

    ВНИМАНИЕ: keywords ПОЛНОСТЬЮ заменяет существующий список. Для частичного
    добавления сначала прочитайте текущий набор через get_negative_keyword_sets
    и передайте объединённый список.

    Параметры:
    - set_id:       ID набора (число, обязательно)
    - name:         новое имя (необязательно)
    - keywords:     новый список фраз через запятую — ПОЛНАЯ замена
                    (необязательно)
    - account:      псевдоним аккаунта Директа (необязательно)
    - client_login: логин клиента для агентских аккаунтов (необязательно)

    Возвращает JSON со сводкой: id, updated_fields, new_name (если менялось),
    new_keyword_count (если менялся список), warnings.
    """
    lc = ctx.request_context.lifespan_context
    direct = resolve_direct_client(account, lc)
    if direct is None:
        return _no_direct_error(account)

    name_val = name.strip() if (name and name.strip()) else None
    kw_val: list[str] | None = None
    if keywords is not None:
        kw_val = _parse_keywords(keywords)
        if not kw_val:
            return json.dumps(
                {"error": "Параметр keywords пуст — укажите хотя бы одну фразу или не передавайте этот параметр."},
                ensure_ascii=False,
            )

    if not name_val and kw_val is None:
        return json.dumps(
            {"error": "Укажите хотя бы один из параметров: name или keywords."},
            ensure_ascii=False,
        )

    item: dict = {"Id": int(set_id)}
    updated_fields: list[str] = []
    if name_val is not None:
        item["Name"] = name_val
        updated_fields.append("name")
    if kw_val is not None:
        item["NegativeKeywords"] = kw_val
        updated_fields.append("keywords")

    payload = {
        "method": "update",
        "params": {"NegativeKeywordSharedSets": [item]},
    }

    try:
        data = await direct._post_negative_kw_sets(payload, client_login=client_login)
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    results = data.get("result", {}).get("UpdateResults", []) or []
    if not results:
        return json.dumps({"error": "Нет UpdateResults в ответе API."}, ensure_ascii=False)

    first = results[0]
    if first.get("Errors"):
        return json.dumps(
            {"error": "Ошибка обновления набора.", "details": first["Errors"]},
            ensure_ascii=False,
        )

    result: dict = {
        "account": account or "primary",
        "id": first.get("Id", int(set_id)),
        "updated_fields": updated_fields,
    }
    if name_val is not None:
        result["new_name"] = name_val
    if kw_val is not None:
        result["new_keyword_count"] = len(kw_val)
    if first.get("Warnings"):
        result["warnings"] = first["Warnings"]

    warning = direct.units_warning()
    if warning:
        result["_units_warning"] = warning

    return json.dumps(result, ensure_ascii=False, indent=2)


@mcp.tool()
async def delete_negative_keyword_sets(
    ctx: Context,
    ids: str,
    account: Optional[str] = None,
    client_login: Optional[str] = None,
) -> str:
    """
    Удалить общие наборы минус-фраз по ID.

    ВНИМАНИЕ: удаление необратимо. Если набор привязан хотя бы к одной
    кампании, API отклонит запрос — сначала отвяжите набор через
    campaigns.update (очистите поле NegativeKeywordSharedSetIds у кампании).

    Параметры:
    - ids:          ID наборов через запятую (обязательно, например '123,456')
    - account:      псевдоним аккаунта Директа (необязательно)
    - client_login: логин клиента для агентских аккаунтов (необязательно)

    Возвращает JSON {requested_ids, deleted_count, deleted_ids, errors}.
    Если часть наборов удалилась, а часть — нет, в errors будут детали
    по проблемным ID.
    """
    lc = ctx.request_context.lifespan_context
    direct = resolve_direct_client(account, lc)
    if direct is None:
        return _no_direct_error(account)

    parsed_ids, err = _parse_ids(ids, "ids")
    if err:
        return json.dumps({"error": err}, ensure_ascii=False)
    if not parsed_ids:
        return json.dumps(
            {"error": "Параметр ids обязателен. Укажите ID наборов через запятую."},
            ensure_ascii=False,
        )

    payload = {
        "method": "delete",
        "params": {"SelectionCriteria": {"Ids": parsed_ids}},
    }

    try:
        data = await direct._post_negative_kw_sets(payload, client_login=client_login)
    except DirectAPIError as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)

    results = data.get("result", {}).get("DeleteResults", []) or []
    deleted_ids: list[int] = []
    errors: list[dict] = []
    for idx, r in enumerate(results):
        if r.get("Errors"):
            errors.append({
                "id": parsed_ids[idx] if idx < len(parsed_ids) else None,
                "errors": r["Errors"],
            })
        elif r.get("Id") is not None:
            deleted_ids.append(r["Id"])

    result: dict = {
        "account": account or "primary",
        "requested_ids": parsed_ids,
        "deleted_count": len(deleted_ids),
        "deleted_ids": deleted_ids,
    }
    if errors:
        result["errors"] = errors

    warning = direct.units_warning()
    if warning:
        result["_units_warning"] = warning

    return json.dumps(result, ensure_ascii=False, indent=2)
