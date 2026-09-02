"""
Инструменты Яндекс.Wordstat — исследование ключевых фраз.

Реализованы на Yandex Cloud Search API v2 (часть Yandex Cloud AI Studio),
пришедшем на смену legacy Direct API v4 (CreateNewWordstatReport + polling).

Отличия от старой реализации:
  - Синхронный REST: один POST → ответ сразу, без report_id и опроса.
  - Аутентификация одним API-ключом сервисного аккаунта (Authorization: Api-Key),
    без Директа, OAuth и Client-Login.
  - `wordstat_dynamics` снова работает: v2 штатно отдаёт динамику по периодам
    (день/неделя/месяц), в отличие от v4, переставшего возвращать MonthList.

Инструменты:
  - wordstat_top_requests  — топ запросов и ассоциаций по фразам
  - wordstat_dynamics      — динамика показов по периодам (день/неделя/месяц)
  - wordstat_regions       — распределение показов по регионам (окно 30 дней)

Конфигурация: YANDEX_SEARCH_API_KEY + YANDEX_FOLDER_ID (см. settings.py).
Лимит Search API: ~5–10 RPS (ретраи на 429 — в WordstatClient).
"""

from __future__ import annotations

import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from mcp.server.mcpserver import Context

from app import mcp
from wordstat_client import WordstatClient, WordstatAPIError

# Допустимые значения enum-ов Search API v2.
_DEVICES = {"DEVICE_ALL", "DEVICE_DESKTOP", "DEVICE_PHONE", "DEVICE_TABLET"}
_PERIODS = {"PERIOD_MONTHLY", "PERIOD_WEEKLY", "PERIOD_DAILY"}
_REGION_MODES = {"REGION_ALL", "REGION_CITIES", "REGION_REGIONS"}

_MAX_NUM_PHRASES = 2000
# topRequests в v2 принимает ОДНУ фразу. Чтобы не ломать существующих
# вызывающих (инструмент исторически принимал до 10 фраз через запятую),
# сохраняем multi-phrase эргономику: перебираем фразы на клиенте с учётом
# лимита RPS и агрегируем. Ограничение в 10 фраз оставляем прежним.
_MAX_PHRASES = 10


# ---------------------------------------------------------------------------
# Общие помощники
# ---------------------------------------------------------------------------

def _no_client_error() -> dict[str, Any]:
    return {
        "error": "Клиент Wordstat (Yandex Cloud Search API v2) не инициализирован. "
        "Задайте переменные окружения YANDEX_SEARCH_API_KEY и YANDEX_FOLDER_ID "
        "(сервисный аккаунт с ролью search-api.webSearch.user и ключом со scope "
        "yc.search-api.execute)."
    }


def _get_client(ctx: Context) -> WordstatClient | None:
    return ctx.request_context.lifespan_context.get("wordstat_client")


def _to_int(value: Any) -> int:
    """Приводит строковый count/totalCount к int (v2 отдаёт их строками)."""
    try:
        return int(str(value).strip())
    except (TypeError, ValueError):
        return 0


def _to_float(value: Any) -> float:
    """Приводит share/affinityIndex к float (иногда приходят строками)."""
    try:
        return float(str(value).strip())
    except (TypeError, ValueError):
        return 0.0


def _parse_csv(raw: str | None) -> list[str]:
    """Разбор CSV (запятая/точка с запятой) с удалением дублей и пустых."""
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


def _parse_regions(raw: str | None) -> list[str]:
    """geo_ids → список строковых ID регионов (v2 ждёт строки, напр. '213')."""
    return _parse_csv(raw)


def _parse_devices(raw: str | None) -> tuple[list[str] | None, str | None]:
    """devices → валидированный список enum-ов или ошибка."""
    if not raw:
        return None, None
    items = [d.strip().upper() for d in re.split(r"[,;]\s*", raw) if d.strip()]
    bad = [d for d in items if d not in _DEVICES]
    if bad:
        return None, (
            f"Недопустимые значения devices: {', '.join(bad)}. "
            f"Допустимо: {', '.join(sorted(_DEVICES))}."
        )
    return (items or None), None


# ---------------------------------------------------------------------------
# 1. wordstat_top_requests → POST /v2/wordstat/topRequests
# ---------------------------------------------------------------------------

@mcp.tool()
async def wordstat_top_requests(
    ctx: Context,
    phrases: str,
    geo_ids: Optional[str] = None,
    limit: int = 50,
    devices: Optional[str] = None,
) -> dict[str, Any]:
    """
    Топ похожих поисковых запросов и ассоциаций по фразам (Wordstat, Search API v2).

    Для каждой фразы возвращает:
      - results:      сами вхождения фразы и её уточнения с числом показов;
      - associations: «с этим также искали» (похожие запросы) с показами.
    Полезно для расширения семантики и поиска минус-слов.

    Параметры:
    - phrases:  одна или несколько фраз через запятую (до 10). Поддерживаются
                операторы Wordstat (кавычки, !, +, - и т.д.). Для каждой фразы
                делается отдельный синхронный запрос к API.
    - geo_ids:  ID регионов Яндекса через запятую ('213' — Москва, '225' —
                Россия). Пусто = все регионы.
    - limit:    макс. число строк results/associations на фразу (numPhrases),
                по умолчанию 50, максимум 2000.
    - devices:  типы устройств через запятую: DEVICE_ALL, DEVICE_DESKTOP,
                DEVICE_PHONE, DEVICE_TABLET. Пусто = все устройства.

    Возвращает JSON: {phrases, geo_ids, returned_phrases, results:[{phrase,
    total_count, results:[{phrase,count}], associations:[{phrase,count}]}]}.
    Все count/totalCount приведены к int.
    """
    ws = _get_client(ctx)
    if ws is None:
        return _no_client_error()

    parsed_phrases = _parse_csv(phrases)
    if not parsed_phrases:
        return {"error": "Параметр phrases пуст."}
    if len(parsed_phrases) > _MAX_PHRASES:
        return {"error": f"Не более {_MAX_PHRASES} фраз за вызов. Получено: {len(parsed_phrases)}."}

    num_phrases = max(1, min(int(limit or 50), _MAX_NUM_PHRASES))
    regions = _parse_regions(geo_ids)
    device_list, err = _parse_devices(devices)
    if err:
        return {"error": err}

    entries: list[dict] = []
    for phrase in parsed_phrases:
        try:
            data = await ws.top_requests(
                phrase,
                num_phrases=num_phrases,
                regions=regions or None,
                devices=device_list,
            )
        except WordstatAPIError as e:
            return {"error": str(e), "phrase": phrase}

        results = [
            {"phrase": r.get("phrase", ""), "count": _to_int(r.get("count"))}
            for r in (data.get("results") or [])
            if isinstance(r, dict)
        ]
        associations = [
            {"phrase": a.get("phrase", ""), "count": _to_int(a.get("count"))}
            for a in (data.get("associations") or [])
            if isinstance(a, dict)
        ]
        entries.append({
            "phrase": phrase,
            "total_count": _to_int(data.get("totalCount")),
            "results_count": len(results),
            "associations_count": len(associations),
            "results": results,
            "associations": associations,
        })

    entries.sort(key=lambda x: x["total_count"], reverse=True)
    result: dict = {
        "phrases": parsed_phrases,
        "geo_ids": regions if regions else "all_regions",
        "devices": device_list or "all_devices",
        "returned_phrases": len(entries),
        "results": entries,
    }
    return result


# ---------------------------------------------------------------------------
# 2. wordstat_dynamics → POST /v2/wordstat/dynamics
# ---------------------------------------------------------------------------

def _parse_rfc3339(raw: str) -> datetime:
    """Разбирает дату в RFC3339/ISO. Возвращает aware-datetime (UTC)."""
    s = raw.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
    except ValueError as exc:
        # Допускаем чистый YYYY-MM-DD
        try:
            dt = datetime.strptime(raw.strip(), "%Y-%m-%d")
        except ValueError:
            raise ValueError(
                f"Неверный формат даты «{raw}». Используйте RFC3339, например "
                "2025-12-29T00:00:00Z или 2025-12-29."
            ) from exc
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _to_rfc3339(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _is_last_day_of_month(dt: datetime) -> bool:
    return (dt + timedelta(days=1)).month != dt.month


def _align_error(period: str, which: str, dt: datetime) -> str | None:
    """Проверяет выравнивание даты под период. Возвращает текст ошибки или None.

    Требования Search API v2 (проверено на живом API):
      WEEKLY:  from_date — понедельник, to_date — воскресенье.
      MONTHLY: from_date — 1-е число месяца, to_date — последний день месяца.
      DAILY:   без ограничений.
    """
    if period == "PERIOD_WEEKLY":
        if which == "from_date" and dt.weekday() != 0:
            return (
                f"Для PERIOD_WEEKLY from_date должна быть понедельником "
                f"(получено {dt.date()}, это {dt.strftime('%A')})."
            )
        if which == "to_date" and dt.weekday() != 6:
            return (
                f"Для PERIOD_WEEKLY to_date должна быть воскресеньем "
                f"(получено {dt.date()}, это {dt.strftime('%A')})."
            )
    if period == "PERIOD_MONTHLY":
        if which == "from_date" and dt.day != 1:
            return f"Для PERIOD_MONTHLY from_date должна быть 1-м числом месяца (получено {dt.date()})."
        if which == "to_date" and not _is_last_day_of_month(dt):
            return f"Для PERIOD_MONTHLY to_date должна быть последним днём месяца (получено {dt.date()})."
    return None


def _default_window(period: str, now: datetime) -> tuple[datetime, datetime]:
    """Окно по умолчанию, выровненное под период (последние завершённые интервалы).

    Monthly: последние 12 завершённых месяцев (1-е число … последний день).
    Weekly:  последние 12 завершённых недель (понедельник … воскресенье).
    Daily:   последние 30 дней (по вчерашний день включительно).
    """
    today = now.replace(hour=0, minute=0, second=0, microsecond=0)
    if period == "PERIOD_MONTHLY":
        first_this_month = today.replace(day=1)
        to_dt = first_this_month - timedelta(days=1)          # последний день прошлого месяца
        year, month = to_dt.year, to_dt.month - 11            # 12 месяцев включительно
        while month <= 0:
            month += 12
            year -= 1
        from_dt = to_dt.replace(year=year, month=month, day=1)
        return from_dt, to_dt
    if period == "PERIOD_WEEKLY":
        this_monday = today - timedelta(days=today.weekday())
        to_dt = this_monday - timedelta(days=1)               # последнее воскресенье
        from_dt = this_monday - timedelta(weeks=12)           # понедельник 12 недель назад
        return from_dt, to_dt
    # PERIOD_DAILY
    to_dt = today - timedelta(days=1)                         # вчера
    return to_dt - timedelta(days=29), to_dt                  # 30 дней включительно


@mcp.tool()
async def wordstat_dynamics(
    ctx: Context,
    phrase: str,
    period: str = "PERIOD_MONTHLY",
    from_date: Optional[str] = None,
    to_date: Optional[str] = None,
    geo_ids: Optional[str] = None,
    devices: Optional[str] = None,
) -> dict[str, Any]:
    """
    Динамика показов фразы по периодам (Wordstat, Search API v2).

    В отличие от legacy Direct API v4 (который перестал отдавать MonthList),
    v2 штатно возвращает динамику по дням/неделям/месяцам. Для каждого периода
    отдаётся число показов и share — доля этой фразы среди ВСЕХ запросов Яндекса
    за период (удобно для нормализации трендов и сезонности).

    Параметры:
    - phrase:    одна фраза. По операторам: для дневной разбивки (PERIOD_DAILY)
                 поддерживаются все операторы Wordstat; для недельной/месячной —
                 только оператор `+`.
    - period:    PERIOD_MONTHLY (по умолч.) | PERIOD_WEEKLY | PERIOD_DAILY.
    - from_date: начало периода, RFC3339 (напр. 2025-01-01T00:00:00Z или
                 2025-01-01). Выравнивание обязательно, иначе вернётся ошибка:
                 WEEKLY — понедельник, MONTHLY — 1-е число месяца.
    - to_date:   конец периода, RFC3339. Выравнивание: WEEKLY — воскресенье,
                 MONTHLY — последний день месяца, DAILY — без ограничений.
                 Если from_date/to_date не заданы — берётся окно по умолчанию
                 (последние 12 завершённых месяцев для MONTHLY, 12 недель для
                 WEEKLY, 30 дней для DAILY).
    - geo_ids:   ID регионов через запятую ('213' — Москва). Пусто = все.
    - devices:   DEVICE_ALL | DEVICE_DESKTOP | DEVICE_PHONE | DEVICE_TABLET
                 через запятую. Пусто = все.

    Возвращает JSON: {phrase, period, from_date, to_date, points_count,
    dynamics:[{date, count(int), share(float)}], summary:{min_count, max_count,
    avg_count}}. При ответе удобно оформить таблицей по периодам.
    """
    ws = _get_client(ctx)
    if ws is None:
        return _no_client_error()

    phrase_str = (phrase or "").strip()
    if not phrase_str:
        return {"error": "Параметр phrase пуст."}

    period = (period or "PERIOD_MONTHLY").strip().upper()
    if period not in _PERIODS:
        return {"error": f"Недопустимый period: «{period}». Допустимо: {', '.join(sorted(_PERIODS))}."}

    now = datetime.now(timezone.utc)
    try:
        if from_date and to_date:
            from_dt = _parse_rfc3339(from_date)
            to_dt = _parse_rfc3339(to_date)
        elif from_date or to_date:
            return {"error": "Задайте и from_date, и to_date одновременно, либо оставьте оба пустыми."}
        else:
            from_dt, to_dt = _default_window(period, now)
    except ValueError as e:
        return {"error": str(e)}

    if from_dt >= to_dt:
        return {"error": "from_date должна быть раньше to_date."}

    for which, dt in (("from_date", from_dt), ("to_date", to_dt)):
        align_err = _align_error(period, which, dt)
        if align_err:
            return {"error": align_err}

    regions = _parse_regions(geo_ids)
    device_list, err = _parse_devices(devices)
    if err:
        return {"error": err}

    try:
        data = await ws.dynamics(
            phrase_str,
            period=period,
            from_date=_to_rfc3339(from_dt),
            to_date=_to_rfc3339(to_dt),
            regions=regions or None,
            devices=device_list,
        )
    except WordstatAPIError as e:
        return {"error": str(e)}

    points = [
        {
            "date": r.get("date", ""),
            "count": _to_int(r.get("count")),
            "share": _to_float(r.get("share")),
        }
        for r in (data.get("results") or [])
        if isinstance(r, dict)
    ]

    counts = [p["count"] for p in points]
    summary = {
        "min_count": min(counts) if counts else 0,
        "max_count": max(counts) if counts else 0,
        "avg_count": round(sum(counts) / len(counts), 1) if counts else 0.0,
    }

    result: dict = {
        "phrase": phrase_str,
        "period": period,
        "from_date": _to_rfc3339(from_dt),
        "to_date": _to_rfc3339(to_dt),
        "geo_ids": regions if regions else "all_regions",
        "devices": device_list or "all_devices",
        "points_count": len(points),
        "dynamics": points,
        "summary": summary,
    }
    return result


# ---------------------------------------------------------------------------
# 3. wordstat_regions → POST /v2/wordstat/regions
# ---------------------------------------------------------------------------

@mcp.tool()
async def wordstat_regions(
    ctx: Context,
    phrase: str,
    region: str = "REGION_ALL",
    limit: int = 50,
    devices: Optional[str] = None,
) -> dict[str, Any]:
    """
    Распределение показов фразы по регионам за последние 30 дней (Search API v2).

    Окно фиксировано (30 дней, не настраивается). Для каждого региона отдаётся:
      - count:         число показов;
      - share:         доля показов региона;
      - affinity_index: индекс интереса (%) — отношение доли фразы в регионе к
                        её доле по стране. >100 = регион ищет фразу активнее среднего.
    ID регионов дополняются человекочитаемыми названиями (region_name) через
    кэшируемое дерево регионов (getRegionsTree, кэш на диске, TTL 30 дней).

    Параметры:
    - phrase:  одна фраза (поддерживаются операторы Wordstat).
    - region:  срез регионов — одиночный enum, НЕ список ID:
               REGION_ALL (все, по умолч.) | REGION_CITIES (только города) |
               REGION_REGIONS (только субъекты федерации).
    - limit:   топ-N регионов в ответе (по убыванию показов), по умолчанию 50.
    - devices: DEVICE_ALL | DEVICE_DESKTOP | DEVICE_PHONE | DEVICE_TABLET
               через запятую. Пусто = все.

    Возвращает JSON: {phrase, region_mode, returned_regions,
    regions:[{region_id, region_name, count(int), share(float),
    affinity_index(float)}]}.
    """
    ws = _get_client(ctx)
    if ws is None:
        return _no_client_error()

    phrase_str = (phrase or "").strip()
    if not phrase_str:
        return {"error": "Параметр phrase пуст."}

    region_mode = (region or "REGION_ALL").strip().upper()
    if region_mode not in _REGION_MODES:
        return {"error": f"Недопустимый region: «{region_mode}». Допустимо: {', '.join(sorted(_REGION_MODES))}."}

    device_list, err = _parse_devices(devices)
    if err:
        return {"error": err}

    try:
        data = await ws.regions(phrase_str, region=region_mode, devices=device_list)
    except WordstatAPIError as e:
        return {"error": str(e)}

    # Маппинг id → название (best-effort: если дерево недоступно, оставим id).
    region_names: dict[str, str] = {}
    try:
        region_names = await ws.get_regions_map()
    except WordstatAPIError:
        region_names = {}

    rows: list[dict] = []
    for r in (data.get("results") or []):
        if not isinstance(r, dict):
            continue
        rid = str(r.get("region", "")).strip()
        rows.append({
            "region_id": rid,
            "region_name": region_names.get(rid, ""),
            "count": _to_int(r.get("count")),
            "share": _to_float(r.get("share")),
            "affinity_index": _to_float(r.get("affinityIndex")),
        })

    rows.sort(key=lambda x: x["count"], reverse=True)
    limit_n = max(1, int(limit or 50))
    rows = rows[:limit_n]

    result: dict = {
        "phrase": phrase_str,
        "region_mode": region_mode,
        "devices": device_list or "all_devices",
        "returned_regions": len(rows),
        "regions": rows,
    }
    return result
