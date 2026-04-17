"""
Standalone-валидатор всех read-only инструментов Yandex Direct API v5.

Использует синхронный `requests` (не httpx) — это позволяет проверить, что
исправления в `direct_client.py` не зависят от особенностей httpx, и подтверждает
совместимость с референсным примером из документации Яндекса.

Запуск:
    python test_direct.py                      # основной аккаунт (YANDEX_DIRECT_TOKEN)
    python test_direct.py --account site2      # псевдоним из YANDEX_DIRECT_ACCOUNTS
    python test_direct.py --client-login foo   # агентский аккаунт

6 инструментов (read-only):
    get_campaigns         — список кампаний  (JSON API: /json/v5/campaigns)
    get_performance       — сводка по аккаунту  (Reports: ACCOUNT_PERFORMANCE_REPORT)
    get_campaign_stats    — по кампаниям  (Reports: CAMPAIGN_PERFORMANCE_REPORT)
    get_keyword_stats     — топ ключевых фраз  (Reports: CUSTOM_REPORT с Keyword)
    get_search_queries    — реальные запросы  (Reports: SEARCH_QUERY_PERFORMANCE_REPORT)
    get_budget            — дневные бюджеты + остаток баллов API

Обрабатывает HTTP: 200, 201 (queued), 202, 400, 401, 403, 404, 429, 5xx.
"""

from __future__ import annotations

import argparse
import csv
import io
import os
import sys
import time
import uuid
from typing import Any

import requests
from dotenv import load_dotenv

# Windows console по умолчанию cp1251/cp866 — принудительно UTF-8 для вывода.
if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
        sys.stderr.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except Exception:
        pass

load_dotenv()

BASE_JSON = "https://api.direct.yandex.com/json/v5"
REPORTS_URL = f"{BASE_JSON}/reports"
CAMPAIGNS_URL = f"{BASE_JSON}/campaigns"

# Поля Reports API, которые возвращаются в микро-рублях.
MICRO_FIELDS = {"Cost", "AvgCpc", "CostPerConversion", "Revenue"}


# ---------------------------------------------------------------------------
# HTTP-обёртки
# ---------------------------------------------------------------------------

def _common_headers(token: str, client_login: str | None) -> dict[str, str]:
    """Заголовки, общие для JSON API и Reports API."""
    h = {
        "Authorization": f"Bearer {token}",
        "Accept-Language": "ru",
    }
    if client_login:
        h["Client-Login"] = client_login
    return h


def _report_headers(token: str, client_login: str | None) -> dict[str, str]:
    """Reports API требует processingMode. Без него — 400 error_code 8000."""
    h = _common_headers(token, client_login)
    h["processingMode"] = "auto"
    h["skipReportHeader"] = "true"
    h["skipReportSummary"] = "true"
    return h


def _handle_json_status(resp: requests.Response, kind: str) -> None:
    """Маппинг HTTP-кодов в человекочитаемые исключения для JSON API."""
    code = resp.status_code
    if code == 200:
        return
    if code == 401:
        raise RuntimeError(f"{kind}: 401 — токен невалиден/истёк (scope direct:api)")
    if code == 403:
        raise RuntimeError(f"{kind}: 403 — нет доступа. Проверьте Client-Login и права токена.")
    if code == 404:
        raise RuntimeError(f"{kind}: 404 — неверный URL (ожидался /json/v5/...)")
    if code == 429:
        raise RuntimeError(f"{kind}: 429 — превышен лимит запросов, повторите позже")
    if 500 <= code < 600:
        raise RuntimeError(f"{kind}: {code} — временная ошибка сервера, повторите")
    raise RuntimeError(f"{kind}: HTTP {code} — {resp.text[:300]}")


def _extract_report_error(text: str) -> str:
    """
    На практике Reports API v5 отдаёт 400 как JSON:
      {"error":{"error_code":"8000","error_detail":"...","error_string":"..."}}
    (документация упоминает XML для старых версий — обрабатываем оба варианта.)
    """
    import json as _json
    import re
    try:
        data = _json.loads(text)
        err = data.get("error", {}) if isinstance(data, dict) else {}
        code = err.get("error_code", "")
        detail = err.get("error_detail", "") or err.get("error_string", "")
        if code or detail:
            return f"code={code} | {detail}"
    except (ValueError, TypeError):
        pass
    parts = []
    for tag in ("error_code", "error_message", "error_detail"):
        m = re.search(rf"<{tag}>(.*?)</{tag}>", text, re.DOTALL)
        if m and m.group(1).strip():
            parts.append(f"{tag}={m.group(1).strip()}")
    return " | ".join(parts) or text[:300]


# ---------------------------------------------------------------------------
# Универсальный вызов Reports API (polling-модель)
# ---------------------------------------------------------------------------

def call_report(
    token: str,
    body: dict[str, Any],
    client_login: str | None = None,
    max_polls: int = 30,
) -> list[dict[str, str]]:
    """
    Выполняет запрос к Reports API и возвращает строки TSV как list[dict].

    Алгоритм: POST → если 200 → парсим; если 201/202 → ждём retryIn и повторяем.
    На 400 пытаемся достать <error_detail> из XML.
    """
    headers = _report_headers(token, client_login)

    for _ in range(max_polls):
        resp = requests.post(REPORTS_URL, json=body, headers=headers, timeout=60)

        if resp.status_code == 200:
            # TSV ответ — первая строка (если не отключили skipReportHeader)
            # содержит заголовки. Пропустим total-строку в конце.
            rows = _parse_tsv(resp.text)
            return rows

        if resp.status_code in (201, 202):
            retry_in = int(resp.headers.get("retryIn", "5"))
            time.sleep(min(retry_in, 30))
            continue

        if resp.status_code == 400:
            # requests угадывает кодировку по Content-Type; для русских error_detail
            # принудительно декодируем из UTF-8, иначе получаем mojibake.
            try:
                text = resp.content.decode("utf-8", errors="replace")
            except Exception:
                text = resp.text
            detail = _extract_report_error(text)
            raise RuntimeError(f"Reports API 400 — {detail}")

        _handle_json_status(resp, "Reports API")

    raise RuntimeError(f"Reports API: отчёт не готов после {max_polls} попыток")


def _parse_tsv(text: str) -> list[dict[str, str]]:
    """DictReader по TSV; пропускает строку 'Total rows:'."""
    reader = csv.DictReader(io.StringIO(text.strip()), delimiter="\t")
    rows = []
    for row in reader:
        first = next(iter(row.values()), "")
        if first == "Total" or (isinstance(first, str) and first.startswith("Total rows")):
            continue
        rows.append(dict(row))
    return rows


def _micro_to_rub(val: str | int | float) -> float:
    try:
        return round(float(val or 0) / 1_000_000, 2)
    except (ValueError, TypeError):
        return 0.0


# ---------------------------------------------------------------------------
# Tool 1: список кампаний
# ---------------------------------------------------------------------------

def get_campaigns(token: str, client_login: str | None = None) -> dict[str, Any]:
    """Список кампаний: id, name, status, daily budget. POST /json/v5/campaigns."""
    body = {
        "method": "get",
        "params": {
            "FieldNames": ["Id", "Name", "Status", "State", "DailyBudget"],
            "Page": {"Limit": 1000},
        },
    }
    resp = requests.post(
        CAMPAIGNS_URL,
        json=body,
        headers=_common_headers(token, client_login),
        timeout=30,
    )
    _handle_json_status(resp, "campaigns.get")
    data = resp.json()
    if "error" in data:
        err = data["error"]
        raise RuntimeError(f"campaigns.get: {err.get('error_detail') or err.get('error_string')}")
    units = resp.headers.get("Units", "")
    return {
        "campaigns": data.get("result", {}).get("Campaigns", []),
        "units": units,
    }


# ---------------------------------------------------------------------------
# Tool 2: сводка по аккаунту
# ---------------------------------------------------------------------------

def get_performance(
    token: str,
    date_from: str,
    date_to: str,
    client_login: str | None = None,
) -> list[dict[str, Any]]:
    """
    Overall account stats за период.
    Используем ACCOUNT_PERFORMANCE_REPORT — самый дешёвый по баллам вариант,
    поля автоматически агрегируются (без группировки).
    """
    body = {
        "params": {
            "SelectionCriteria": {"DateFrom": date_from, "DateTo": date_to},
            "FieldNames": ["Impressions", "Clicks", "Cost", "Ctr", "AvgCpc"],
            "ReportName": f"perf_{uuid.uuid4().hex[:8]}",
            "ReportType": "ACCOUNT_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "NO",
        }
    }
    rows = call_report(token, body, client_login)
    return [_normalize_row(r) for r in rows]


# ---------------------------------------------------------------------------
# Tool 3: статистика по кампаниям
# ---------------------------------------------------------------------------

def _campaign_filter(campaign_ids: list[int] | None) -> list[dict[str, Any]] | None:
    """
    Reports API v5 не принимает CampaignIds в SelectionCriteria (в отличие от
    JSON API). Фильтр — через Filter[].Field=CampaignId Operator=IN Values=[str].
    """
    if not campaign_ids:
        return None
    return [{
        "Field": "CampaignId",
        "Operator": "IN",
        "Values": [str(cid) for cid in campaign_ids],
    }]


def get_campaign_stats(
    token: str,
    date_from: str,
    date_to: str,
    campaign_ids: list[int] | None = None,
    client_login: str | None = None,
) -> list[dict[str, Any]]:
    """Разбивка по кампаниям. CAMPAIGN_PERFORMANCE_REPORT с фильтром по id."""
    selection: dict[str, Any] = {"DateFrom": date_from, "DateTo": date_to}
    f = _campaign_filter(campaign_ids)
    if f:
        selection["Filter"] = f

    body = {
        "params": {
            "SelectionCriteria": selection,
            "FieldNames": [
                "CampaignId", "CampaignName",
                "Impressions", "Clicks", "Cost", "Ctr", "AvgCpc",
            ],
            "OrderBy": [{"Field": "Cost", "SortOrder": "DESCENDING"}],
            "ReportName": f"camp_stats_{uuid.uuid4().hex[:8]}",
            "ReportType": "CAMPAIGN_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "NO",
        }
    }
    rows = call_report(token, body, client_login)
    return [_normalize_row(r) for r in rows]


# ---------------------------------------------------------------------------
# Tool 4: топ ключевых фраз
# ---------------------------------------------------------------------------

def get_keyword_stats(
    token: str,
    date_from: str,
    date_to: str,
    sort_by: str = "Cost",
    top_n: int = 20,
    campaign_ids: list[int] | None = None,
    client_login: str | None = None,
) -> list[dict[str, Any]]:
    """
    Топ-N ключевых фраз.

    В CUSTOM_REPORT поле "Keyword" невалидно (ошибка 4000). В современном
    Direct ключевая фраза — это "Criterion" с "CriterionType=KEYWORD".
    Фильтруем на стороне клиента, чтобы оставить только KEYWORD,
    отсечь автотаргеты, ретаргетинг и т.п.

    Top-N применяется пост-фактум — API Reports такого параметра не имеет.
    """
    selection: dict[str, Any] = {"DateFrom": date_from, "DateTo": date_to}
    f = _campaign_filter(campaign_ids)
    if f:
        selection["Filter"] = f

    body = {
        "params": {
            "SelectionCriteria": selection,
            "FieldNames": [
                "Criterion", "CriterionType", "CriterionId",
                "CampaignId", "CampaignName",
                "Impressions", "Clicks", "Cost", "Ctr", "AvgCpc",
            ],
            "OrderBy": [{"Field": sort_by, "SortOrder": "DESCENDING"}],
            "ReportName": f"kw_{uuid.uuid4().hex[:8]}",
            "ReportType": "CUSTOM_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "NO",
        }
    }
    rows = call_report(token, body, client_login)
    normalized = [_normalize_row(r) for r in rows]
    # Оставляем только KEYWORD-таргетинг (в отчёт попадают и автотаргеты).
    keywords_only = [r for r in normalized if r.get("CriterionType") == "KEYWORD"]
    return keywords_only[:top_n] if keywords_only else normalized[:top_n]


# ---------------------------------------------------------------------------
# Tool 5: поисковые запросы
# ---------------------------------------------------------------------------

def get_search_queries(
    token: str,
    date_from: str,
    date_to: str,
    top_n: int = 30,
    campaign_ids: list[int] | None = None,
    client_login: str | None = None,
) -> list[dict[str, Any]]:
    """Реальные поисковые запросы пользователей. SEARCH_QUERY_PERFORMANCE_REPORT."""
    selection: dict[str, Any] = {"DateFrom": date_from, "DateTo": date_to}
    f = _campaign_filter(campaign_ids)
    if f:
        selection["Filter"] = f

    body = {
        "params": {
            "SelectionCriteria": selection,
            "FieldNames": [
                "Query", "Criterion", "CampaignId", "CampaignName",
                "Impressions", "Clicks", "Cost", "Ctr", "AvgCpc",
            ],
            "OrderBy": [{"Field": "Cost", "SortOrder": "DESCENDING"}],
            "ReportName": f"sq_{uuid.uuid4().hex[:8]}",
            "ReportType": "SEARCH_QUERY_PERFORMANCE_REPORT",
            "DateRangeType": "CUSTOM_DATE",
            "Format": "TSV",
            "IncludeVAT": "NO",
        }
    }
    rows = call_report(token, body, client_login)
    return [_normalize_row(r) for r in rows][:top_n]


# ---------------------------------------------------------------------------
# Tool 6: бюджет + баллы API
# ---------------------------------------------------------------------------

def get_budget(token: str, client_login: str | None = None) -> dict[str, Any]:
    """
    Текущий дневной бюджет активных кампаний + остаток Units.
    Direct не даёт отдельного API для баллов — они возвращаются в заголовке
    Units любого JSON-запроса в формате "spent/available/daily".
    """
    info = get_campaigns(token, client_login)
    active = [c for c in info["campaigns"] if c.get("Status") == "ON"]
    total_rub = 0.0
    per_campaign = []
    for c in active:
        amount = int((c.get("DailyBudget") or {}).get("Amount") or 0)
        rub = round(amount / 1_000_000, 2)
        total_rub += rub
        per_campaign.append({
            "id": c.get("Id"),
            "name": c.get("Name", "-"),
            "daily_rub": rub,
            "mode": (c.get("DailyBudget") or {}).get("Mode", "-"),
        })

    units_parts = [p.strip() for p in info["units"].replace(" ", "").split("/") if p.strip().isdigit()]
    units_obj = None
    if len(units_parts) == 3:
        spent, available, daily = (int(p) for p in units_parts)
        units_obj = {"spent": spent, "available": available, "daily_limit": daily}

    return {
        "active_campaigns": len(active),
        "total_daily_budget_rub": round(total_rub, 2),
        "campaigns": per_campaign,
        "api_units": units_obj,
    }


# ---------------------------------------------------------------------------
# Вспомогательные
# ---------------------------------------------------------------------------

def _normalize_row(row: dict[str, str]) -> dict[str, Any]:
    """Микро-рубли → рубли; строки-числа → int/float."""
    out: dict[str, Any] = {}
    for k, v in row.items():
        if k in MICRO_FIELDS:
            out[k] = _micro_to_rub(v)
        else:
            try:
                if "." in (v or ""):
                    out[k] = float(v)
                else:
                    out[k] = int(v)
            except (ValueError, TypeError):
                out[k] = v or ""
    return out


def _print_table(title: str, rows: list[dict[str, Any]], max_rows: int = 10) -> None:
    print(f"\n=== {title} ===")
    if not rows:
        print("  (нет данных)")
        return
    cols = list(rows[0].keys())
    widths = {c: max(len(c), *(len(str(r.get(c, ""))) for r in rows[:max_rows])) for c in cols}
    header = "  ".join(c.ljust(widths[c]) for c in cols)
    print(header)
    print("  ".join("-" * widths[c] for c in cols))
    for r in rows[:max_rows]:
        print("  ".join(str(r.get(c, "")).ljust(widths[c]) for c in cols))
    if len(rows) > max_rows:
        print(f"  ... ещё {len(rows) - max_rows} строк")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--account", help="alias из YANDEX_DIRECT_ACCOUNTS (иначе — YANDEX_DIRECT_TOKEN)")
    ap.add_argument("--client-login", help="логин клиента (для агентских аккаунтов)")
    ap.add_argument("--days", type=int, default=7, help="период в днях (по умолчанию 7)")
    args = ap.parse_args()

    # Разрешение токена
    if args.account:
        accounts_raw = os.getenv("YANDEX_DIRECT_ACCOUNTS", "")
        token = None
        for part in accounts_raw.split(","):
            if ":" in part:
                alias, tok = part.split(":", 1)
                if alias.strip().lower() == args.account.lower():
                    token = tok.strip()
                    break
        if not token:
            print(f"ERROR: аккаунт '{args.account}' не найден в YANDEX_DIRECT_ACCOUNTS", file=sys.stderr)
            return 2
    else:
        token = os.getenv("YANDEX_DIRECT_TOKEN")
        if not token:
            print("ERROR: не задан YANDEX_DIRECT_TOKEN в .env", file=sys.stderr)
            return 2

    client_login = args.client_login or os.getenv("YANDEX_DIRECT_CLIENT_LOGIN") or None

    # Период (YYYY-MM-DD)
    from datetime import date, timedelta
    today = date.today()
    date_to = today.strftime("%Y-%m-%d")
    date_from = (today - timedelta(days=args.days)).strftime("%Y-%m-%d")
    print(f"Период: {date_from} … {date_to}  |  client_login={client_login or '—'}")

    # 1. campaigns
    try:
        info = get_campaigns(token, client_login)
        camps = info["campaigns"]
        print(f"\n=== get_campaigns: {len(camps)} кампаний ===")
        for c in camps[:5]:
            amount = int((c.get("DailyBudget") or {}).get("Amount") or 0)
            print(f"  {c.get('Id')}  [{c.get('Status')}]  «{c.get('Name')}»  daily={amount / 1_000_000:.2f} RUB")
        if info["units"]:
            print(f"  Units (spent/available/daily): {info['units']}")
    except Exception as e:
        print(f"get_campaigns FAILED: {e}")
        return 1

    campaign_ids = [c["Id"] for c in camps[:3]] if camps else None

    # 2. performance
    try:
        perf = get_performance(token, date_from, date_to, client_login)
        _print_table("get_performance (ACCOUNT_PERFORMANCE_REPORT)", perf)
    except Exception as e:
        print(f"get_performance FAILED: {e}")

    # 3. campaign_stats — без фильтра, чтобы увидеть реальные данные
    try:
        stats = get_campaign_stats(token, date_from, date_to, None, client_login)
        _print_table("get_campaign_stats (CAMPAIGN_PERFORMANCE_REPORT)", stats, max_rows=5)
    except Exception as e:
        print(f"get_campaign_stats FAILED: {e}")

    # 4. keyword_stats
    try:
        kws = get_keyword_stats(token, date_from, date_to, "Cost", 10, None, client_login)
        _print_table("get_keyword_stats (CUSTOM_REPORT + CriterionType=KEYWORD)", kws)
    except Exception as e:
        print(f"get_keyword_stats FAILED: {e}")

    # 5. search_queries
    try:
        q = get_search_queries(token, date_from, date_to, 10, None, client_login)
        _print_table("get_search_queries (SEARCH_QUERY_PERFORMANCE_REPORT)", q)
    except Exception as e:
        print(f"get_search_queries FAILED: {e}")

    # 6. budget
    try:
        b = get_budget(token, client_login)
        print(f"\n=== get_budget ===")
        print(f"  Активных кампаний:    {b['active_campaigns']}")
        print(f"  Суммарный дневной:    {b['total_daily_budget_rub']} RUB")
        if b["api_units"]:
            u = b["api_units"]
            print(f"  API units:            spent={u['spent']}  available={u['available']}  daily_limit={u['daily_limit']}")
    except Exception as e:
        print(f"get_budget FAILED: {e}")

    return 0


if __name__ == "__main__":
    sys.exit(main())
