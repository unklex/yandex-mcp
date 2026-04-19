"""
Асинхронный клиент Yandex Direct API v5.

Базовые URL (официальные, по документации https://yandex.ru/dev/direct/doc/):
  JSON API:  https://api.direct.yandex.com/json/v5/{campaigns,ads,adgroups,bids,...}
  Отчёты:    https://api.direct.yandex.com/json/v5/reports  (polling-TSV)

Отличие от Metrica:
  - Auth header: Authorization: Bearer {token}
  - Client-Login — HTTP-заголовок для агентских аккаунтов
  - Reports API возвращает TSV, использует polling (201/202 → 200)
  - Reports API требует заголовок processingMode (online/offline/auto)
  - Тело запроса Reports API: {"params": {"SelectionCriteria": ..., "FieldNames": ...}}
    БЕЗ обёртки ReportDefinition (типовая ошибка, приводящая к 400).
  - DateFrom/DateTo при CUSTOM_DATE идут ВНУТРЬ SelectionCriteria, не в корень.
  - Ошибки Reports API — XML или plain text (не JSON)
  - Суммы в микро-рублях (÷ 1_000_000 = рубли) при returnMoneyInMicros по умолчанию
"""

from __future__ import annotations

import asyncio
import csv
import io
import re
import uuid
from typing import Any

import httpx

_DIRECT_BASE = "https://api.direct.yandex.com/json/v5"
_REPORTS_URL = f"{_DIRECT_BASE}/reports"
_CAMPAIGNS_URL = f"{_DIRECT_BASE}/campaigns"
_ADS_URL = f"{_DIRECT_BASE}/ads"
_ADGROUPS_URL = f"{_DIRECT_BASE}/adgroups"
_BIDS_URL = f"{_DIRECT_BASE}/bids"
_NEGATIVE_KW_SETS_URL = f"{_DIRECT_BASE}/negativekeywordsharedsets"

# Wordstat — legacy API v4: https://yandex.ru/dev/direct/doc/dg-v4/reference/CreateNewWordstatReport
# Единый эндпоинт, методы переключаются через поле "method" в теле.
_WORDSTAT_URL = "https://api.direct.yandex.ru/live/v4/json/"
_WORDSTAT_MAX_POLLS = 10
_WORDSTAT_POLL_SLEEP = 3.0

_MAX_RETRIES = 3
_MAX_POLL_RETRIES = 30
_RETRY_STATUSES = {500, 502, 503, 504}


class DirectAPIError(Exception):
    """Ошибка Yandex Direct API с HTTP-статусом и сообщением на русском."""

    def __init__(self, status: int, message: str) -> None:
        self.status = status
        super().__init__(message)


class DirectClient:
    """
    Асинхронный клиент Yandex Direct API v5.

    Управление жизненным циклом через async context manager:
        async with DirectClient(token=...) as client:
            campaigns = await client.get_campaigns()

    Особенности:
    - Auth: Authorization: Bearer {token}  (НЕ OAuth — в отличие от MetricaClient)
    - Client-Login передаётся как HTTP-заголовок (не в payload)
    - Единый пул соединений httpx.AsyncClient
    - Polling для Reports API: 201/202 → sleep(retryIn) → повтор → 200 = TSV
    - TSV-ответы парсятся в list[dict] с early-exit по top_n для экономии памяти
    - 400 ошибки Reports API: XML или plain text → извлекаем <error_detail>
    - Отслеживание Units: spent/available/daily; предупреждение при < 10%
    """

    def __init__(self, token: str, client_login: str | None = None) -> None:
        self._token = token
        self._client_login = client_login
        # Bearer — НЕ OAuth. Client-Login — HTTP-заголовок, не параметр запроса.
        self._base_headers: dict[str, str] = {
            "Authorization": f"Bearer {token}",
            "Accept-Language": "ru",
            "Content-Type": "application/json; charset=utf-8",
        }
        if client_login:
            self._base_headers["Client-Login"] = client_login
        self._client: httpx.AsyncClient | None = None
        # Последние известные данные об остатке Units
        self.last_units: dict[str, int] | None = None

    async def __aenter__(self) -> "DirectClient":
        self._client = httpx.AsyncClient(
            headers=self._base_headers,
            timeout=60.0,
        )
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._client:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # Внутренние методы
    # ------------------------------------------------------------------

    def _parse_units(self, response: httpx.Response) -> None:
        """
        Разбирает заголовок Units: spent/available/daily
        и сохраняет в self.last_units для последующего мониторинга.
        Формат: "150 / 850 / 1000" или "150/850/1000"
        """
        units_header = response.headers.get("Units", "")
        parts = [p.strip() for p in units_header.replace(" ", "").split("/")]
        if len(parts) == 3 and all(p.isdigit() for p in parts):
            self.last_units = {
                "spent": int(parts[0]),
                "available": int(parts[1]),
                "daily": int(parts[2]),
            }

    def units_warning(self) -> str | None:
        """
        Возвращает предупреждение на русском, если остаток Units < 10% дневного лимита.
        """
        if not self.last_units:
            return None
        available = self.last_units["available"]
        daily = self.last_units["daily"]
        if daily > 0 and available / daily < 0.10:
            pct = round(available / daily * 100, 1)
            return (
                f"⚠️ Остаток баллов API Яндекс.Директа низкий: {available} из {daily} "
                f"({pct}%). Следующие запросы могут быть отклонены до сброса лимита (каждые 24 часа)."
            )
        return None

    @staticmethod
    def _status_message(resp: httpx.Response) -> str:
        messages: dict[int, str] = {
            401: "Ошибка аутентификации в Яндекс.Директе. Проверьте токен YANDEX_DIRECT_ACCOUNTS или YANDEX_DIRECT_TOKEN.",
            403: "Нет доступа к данным Яндекс.Директа. Проверьте права токена и Client-Login.",
            429: "Превышен лимит запросов к Яндекс.Директу. Попробуйте позже.",
            500: "Внутренняя ошибка сервера Яндекс.Директа. Попробуйте позже.",
        }
        return messages.get(
            resp.status_code,
            f"Неожиданный ответ Яндекс.Директа (HTTP {resp.status_code}): {resp.text[:200]}",
        )

    async def _post_json(self, url: str, payload: dict[str, Any]) -> dict[str, Any]:
        """
        POST JSON для синхронных сервисов (Campaigns и др.).
        Повторные попытки при 5xx (до 3 раз, экспоненциальный backoff).
        """
        assert self._client is not None, (
            "Клиент не инициализирован. Используйте: async with DirectClient(...) as client"
        )
        last_exc: Exception | None = None
        for attempt in range(_MAX_RETRIES):
            try:
                resp = await self._client.post(url, json=payload)
            except httpx.RequestError as exc:
                raise DirectAPIError(0, f"Сетевая ошибка при запросе к Яндекс.Директу: {exc}") from exc

            self._parse_units(resp)

            if resp.status_code == 200:
                data = resp.json()
                # Direct API может вернуть ошибку внутри 200-ответа
                if "error" in data:
                    err = data["error"]
                    raise DirectAPIError(
                        int(err.get("error_code", 0)),
                        f"Ошибка API Директа: {err.get('error_detail') or err.get('error_string', 'Неизвестная ошибка')}",
                    )
                return data

            if resp.status_code in _RETRY_STATUSES and attempt < _MAX_RETRIES - 1:
                await asyncio.sleep(2**attempt)
                last_exc = DirectAPIError(resp.status_code, self._status_message(resp))
                continue

            raise DirectAPIError(resp.status_code, self._status_message(resp))

        raise last_exc or DirectAPIError(0, "Неизвестная ошибка при запросе к Яндекс.Директу")

    @staticmethod
    def _report_headers(client_login: str | None) -> dict[str, str]:
        """
        Спец-заголовки Reports API. processingMode ОБЯЗАТЕЛЕН — без него API
        возвращает 400 "Invalid request" (error_code 8000) без деталей.

        processingMode=auto: сервер сам выбирает online/offline в зависимости от
        объёма; это снимает нужду в polling для маленьких отчётов и
        автоматически очередирует большие.
        """
        headers: dict[str, str] = {
            "processingMode": "auto",
            # Включаем описательные строки — _parse_tsv их фильтрует.
            # (оставляем skipReportHeader=false для удобной отладки;
            #  чтобы полностью выключить их — поставьте "true")
            "skipReportHeader": "true",
            "skipReportSummary": "true",
        }
        if client_login:
            headers["Client-Login"] = client_login
        return headers

    @staticmethod
    def _extract_report_error(resp: httpx.Response) -> str:
        """
        Вытаскивает error_detail/error_code из ответа Reports API.
        На практике 400 приходит в JSON: {"error":{"error_code":..., "error_detail":...}};
        старые версии документации упоминают XML — обрабатываем оба варианта.
        """
        # requests/httpx декодируют тело по Content-Type — но Direct иногда отдаёт
        # кириллический detail без явной кодировки; перечитываем bytes как UTF-8.
        try:
            text = resp.content.decode("utf-8", errors="replace")
        except Exception:
            text = resp.text

        try:
            import json as _json
            data = _json.loads(text)
            err = data.get("error", {}) if isinstance(data, dict) else {}
            code = err.get("error_code", "")
            detail = err.get("error_detail", "") or err.get("error_string", "")
            if code or detail:
                return f"код {code} — {detail}" if code else detail
        except (ValueError, TypeError):
            pass

        m_detail = re.search(r"<error_detail>(.*?)</error_detail>", text, re.DOTALL)
        m_code = re.search(r"<error_code>(.*?)</error_code>", text, re.DOTALL)
        m_msg = re.search(r"<error_message>(.*?)</error_message>", text, re.DOTALL)
        parts: list[str] = []
        if m_code:
            parts.append(f"код {m_code.group(1).strip()}")
        if m_msg:
            parts.append(m_msg.group(1).strip())
        if m_detail:
            d = m_detail.group(1).strip()
            if d:
                parts.append(d)
        if parts:
            return " — ".join(parts)
        return text[:400] if text else "пустое тело ответа"

    async def _post_report(
        self,
        payload: dict[str, Any],
        top_n: int | None = None,
        client_login: str | None = None,
    ) -> list[dict[str, str]]:
        """
        POST к Reports API с polling-моделью.

        Алгоритм:
          1. POST с заголовками processingMode/skipReportHeader/Client-Login
          2. 200 → TSV готов, парсим
          3. 201 (в очереди) или 202 (обрабатывается) → ждём retryIn секунд и повторяем
          4. 400 → извлекаем ошибку из XML (<error_code>, <error_detail>)
          5. 500/502/503/504 → retry с экспоненциальным backoff (до _MAX_RETRIES)
        """
        assert self._client is not None, (
            "Клиент не инициализирован. Используйте: async with DirectClient(...) as client"
        )

        extra_headers = self._report_headers(client_login)
        transient_retries = 0

        for attempt in range(_MAX_POLL_RETRIES):
            try:
                resp = await self._client.post(
                    _REPORTS_URL, json=payload, headers=extra_headers
                )
            except httpx.RequestError as exc:
                raise DirectAPIError(0, f"Сетевая ошибка при запросе отчёта Директа: {exc}") from exc

            self._parse_units(resp)

            if resp.status_code == 200:
                return self._parse_tsv(resp.text, top_n=top_n)

            if resp.status_code in (201, 202):
                # 201 = поставлен в очередь, 202 = обрабатывается
                retry_in = int(resp.headers.get("retryIn", "5"))
                await asyncio.sleep(min(retry_in, 60))
                continue

            if resp.status_code == 400:
                detail = self._extract_report_error(resp)
                raise DirectAPIError(400, f"Неверные параметры отчёта Яндекс.Директа: {detail}")

            if resp.status_code in _RETRY_STATUSES and transient_retries < _MAX_RETRIES - 1:
                transient_retries += 1
                await asyncio.sleep(2**transient_retries)
                continue

            raise DirectAPIError(resp.status_code, self._status_message(resp))

        raise DirectAPIError(
            0,
            f"Отчёт Яндекс.Директа не был готов после {_MAX_POLL_RETRIES} попыток. "
            "Попробуйте сократить период или повторите позже.",
        )

    @staticmethod
    def _parse_tsv(text: str, top_n: int | None = None) -> list[dict[str, str]]:
        """
        Парсит TSV-ответ Reports API в список словарей.

        Первая строка — заголовки (DictReader обрабатывает автоматически).
        Последняя строка "Total" — пропускается.
        Early-exit по top_n: не грузим в память больше нужного (отчёты могут быть 100k+ строк).
        """
        reader = csv.DictReader(io.StringIO(text.strip()), delimiter="\t")
        rows: list[dict[str, str]] = []
        for row in reader:
            first_val = next(iter(row.values()), "")
            if first_val == "Total":
                continue
            rows.append(dict(row))
            if top_n is not None and len(rows) >= top_n:
                break
        return rows

    # ------------------------------------------------------------------
    # Публичные методы
    # ------------------------------------------------------------------

    async def get_campaigns(
        self,
        client_login: str | None = None,
    ) -> dict[str, Any]:
        """
        Получить список рекламных кампаний.
        POST /v501/campaigns — синхронный JSON-ответ.

        client_login: переопределяет Client-Login для этого запроса (агентский аккаунт).
        """
        payload: dict[str, Any] = {
            "method": "get",
            "params": {
                "FieldNames": ["Id", "Name", "Status", "State", "DailyBudget"],
                "Page": {"Limit": 1000},
            },
        }

        if client_login:
            # Временно добавляем заголовок для этого запроса
            extra_headers = {"Client-Login": client_login}
            assert self._client is not None
            resp = await self._client.post(_CAMPAIGNS_URL, json=payload, headers=extra_headers)
            self._parse_units(resp)
            if resp.status_code != 200:
                raise DirectAPIError(resp.status_code, self._status_message(resp))
            data = resp.json()
            if "error" in data:
                err = data["error"]
                raise DirectAPIError(
                    int(err.get("error_code", 0)),
                    f"Ошибка API Директа (кампании): {err.get('error_detail') or err.get('error_string', '?')}",
                )
            return data

        return await self._post_json(_CAMPAIGNS_URL, payload)

    async def _post_json_with_login(
        self,
        url: str,
        payload: dict[str, Any],
        client_login: str | None = None,
    ) -> dict[str, Any]:
        """
        Обёртка над _post_json с опциональным per-request переопределением Client-Login.
        Повторяет логику error-in-200 и единой обработки ошибок.
        """
        if not client_login:
            return await self._post_json(url, payload)

        assert self._client is not None
        extra_headers = {"Client-Login": client_login}
        resp = await self._client.post(url, json=payload, headers=extra_headers)
        self._parse_units(resp)
        if resp.status_code != 200:
            raise DirectAPIError(resp.status_code, self._status_message(resp))
        data = resp.json()
        if "error" in data:
            err = data["error"]
            raise DirectAPIError(
                int(err.get("error_code", 0)),
                f"Ошибка API Директа: {err.get('error_detail') or err.get('error_string', '?')}",
            )
        return data

    async def _post_negative_kw_sets(
        self,
        payload: dict[str, Any],
        client_login: str | None = None,
    ) -> dict[str, Any]:
        """
        POST к сервису NegativeKeywordSharedSets (общие наборы минус-фраз).
        Синхронный JSON, тот же формат ошибок, что у campaigns.
        """
        return await self._post_json_with_login(_NEGATIVE_KW_SETS_URL, payload, client_login)

    async def _wordstat_request(
        self,
        method: str,
        param: Any,
        client_login: str | None = None,
    ) -> dict[str, Any]:
        """
        Вызов legacy Wordstat API (Direct v4).

        Эндпоинт: POST https://api.direct.yandex.ru/live/v4/json/
        Тело: {"method": "...", "param": ..., "token": "...", "locale": "ru"}.
        v4 принимает токен в теле (не в Authorization), но мы дублируем и в
        заголовке — на случай, если Яндекс перейдёт на единый Bearer.

        На успех: {"data": ...}. На ошибку: {"error_code": N, "error_str": "..."}.
        """
        assert self._client is not None, (
            "Клиент не инициализирован. Используйте: async with DirectClient(...) as client"
        )
        body: dict[str, Any] = {
            "method": method,
            "param": param,
            "token": self._token,
            "locale": "ru",
        }
        headers: dict[str, str] = {}
        if client_login:
            headers["Client-Login"] = client_login

        try:
            resp = await self._client.post(
                _WORDSTAT_URL,
                json=body,
                headers=headers or None,
            )
        except httpx.RequestError as exc:
            raise DirectAPIError(0, f"Сетевая ошибка при запросе к Wordstat: {exc}") from exc

        if resp.status_code != 200:
            raise DirectAPIError(
                resp.status_code,
                f"Wordstat HTTP {resp.status_code}: {resp.text[:200]}",
            )

        try:
            data = resp.json()
        except ValueError as exc:
            raise DirectAPIError(0, f"Wordstat: некорректный JSON-ответ: {exc}") from exc

        if isinstance(data, dict) and data.get("error_code"):
            raise DirectAPIError(
                int(data.get("error_code", 0)),
                f"Wordstat: {data.get('error_str', 'неизвестная ошибка')}"
                + (f" — {data.get('error_detail')}" if data.get("error_detail") else ""),
            )
        return data

    # Коды Wordstat v4, означающие «отчёт ещё не готов» — не ошибка, повтори позже.
    # 31/32 — исторические «Отчёт в процессе подготовки» / «Отчёт ещё не готов».
    # Дополнительно страхуемся текстовым матчем: Яндекс иногда меняет коды.
    _WORDSTAT_PENDING_CODES: frozenset[int] = frozenset({31, 32})
    _WORDSTAT_PENDING_MARKERS: tuple[str, ...] = (
        "процесс",    # «в процессе подготовки»
        "не готов",   # «отчёт ещё не готов»
        "pending",
        "not ready",
    )

    @classmethod
    def _is_wordstat_pending(cls, exc: DirectAPIError) -> bool:
        """True если ошибка Wordstat означает «отчёт ещё строится»."""
        if exc.status in cls._WORDSTAT_PENDING_CODES:
            return True
        msg = str(exc).lower()
        return any(marker in msg for marker in cls._WORDSTAT_PENDING_MARKERS)

    async def _wordstat_poll(
        self,
        report_id: int,
        client_login: str | None = None,
    ) -> list[dict[str, Any]]:
        """
        Опрос GetWordstatReport до готовности.

        Wordstat v4 сигнализирует «ещё не готово» ДВУМЯ способами:
          1. data = [{"StatusReport": "Pending"}] (старая документация)
          2. {"error_code": 31|32, "error_str": "Отчёт в процессе подготовки"}
             — несмотря на поле error_code, это НЕ ошибка, а «приходи позже».
        Оба варианта здесь трактуются как pending → sleep + continue.

        Готово: data = [{"Phrase": ..., "Shows": ..., "SearchedWith": [...],
                         "GeoID": [...], "MonthList": [...], "GeoList": [...]}]

        Лимит _WORDSTAT_MAX_POLLS попыток с паузой _WORDSTAT_POLL_SLEEP секунд.
        """
        for _ in range(_WORDSTAT_MAX_POLLS):
            try:
                resp = await self._wordstat_request(
                    "GetWordstatReport", report_id, client_login
                )
            except DirectAPIError as exc:
                # Pending, замаскированный под «ошибку»: повторяем.
                if self._is_wordstat_pending(exc):
                    await asyncio.sleep(_WORDSTAT_POLL_SLEEP)
                    continue
                raise

            inner = resp.get("data") if isinstance(resp, dict) else None

            if isinstance(inner, list) and inner:
                first = inner[0] if isinstance(inner[0], dict) else None
                # Статус Pending в теле data — «отчёт ещё строится»
                if first and first.get("StatusReport") == "Pending":
                    await asyncio.sleep(_WORDSTAT_POLL_SLEEP)
                    continue
                return inner

            await asyncio.sleep(_WORDSTAT_POLL_SLEEP)

        raise DirectAPIError(
            0,
            f"Wordstat: отчёт {report_id} не готов после {_WORDSTAT_MAX_POLLS} попыток. "
            "Попробуйте сократить число фраз или повторить позже.",
        )

    # Подтипы кампаний, поддерживающие NegativeKeywords на уровне кампании.
    # Тип возвращается в Campaigns[].Type как UPPER_SNAKE, а сам объект подтипа
    # лежит под PascalCase-ключом (TextCampaign, UnifiedCampaign и т.д.).
    _NEGATIVE_KEYWORDS_SUBTYPES: dict[str, str] = {
        "TEXT_CAMPAIGN": "TextCampaign",
        "DYNAMIC_TEXT_CAMPAIGN": "DynamicTextCampaign",
        "UNIFIED_CAMPAIGN": "UnifiedCampaign",
        "SMART_CAMPAIGN": "SmartCampaign",
        "MOBILE_APP_CAMPAIGN": "MobileAppCampaign",
        "MCBANNER_CAMPAIGN": "MCBannerCampaign",
    }

    async def set_campaign_negative_keywords(
        self,
        campaign_id: int,
        keywords: list[str],
        mode: str = "append",
        client_login: str | None = None,
    ) -> dict[str, Any]:
        """
        Установить минус-фразы на уровне кампании Яндекс.Директа.

        Алгоритм:
          1. campaigns.get с *FieldNames: [NegativeKeywords] по всем поддерживаемым
             подтипам — получаем Type и текущий список фраз.
          2. mode='append': объединяем без дублей (case-insensitive).
             mode='replace': заменяем весь список на переданный.
          3. campaigns.update с корректным PascalCase-ключом подтипа.

        Лимиты Директа: до 1000 минус-фраз на кампанию, до 20 000 символов суммарно,
        каждая фраза — до 7 слов. Если нарушить — API вернёт 400 с деталями.

        Возвращает сводку: type, previous_count, new_count, added_keywords.
        """
        if mode not in ("append", "replace"):
            raise DirectAPIError(
                0, f"Неверный mode: «{mode}». Используйте 'append' или 'replace'."
            )

        get_payload: dict[str, Any] = {
            "method": "get",
            "params": {
                "SelectionCriteria": {"Ids": [campaign_id]},
                "FieldNames": ["Id", "Name", "Type"],
                "TextCampaignFieldNames": ["NegativeKeywords"],
                "DynamicTextCampaignFieldNames": ["NegativeKeywords"],
                "UnifiedCampaignFieldNames": ["NegativeKeywords"],
                "SmartCampaignFieldNames": ["NegativeKeywords"],
                "MobileAppCampaignFieldNames": ["NegativeKeywords"],
                "MCBannerCampaignFieldNames": ["NegativeKeywords"],
            },
        }
        get_data = await self._post_json_with_login(_CAMPAIGNS_URL, get_payload, client_login)
        camps = get_data.get("result", {}).get("Campaigns", []) or []
        if not camps:
            raise DirectAPIError(0, f"Кампания {campaign_id} не найдена в аккаунте.")

        camp = camps[0]
        c_type = camp.get("Type", "")
        subtype_key = self._NEGATIVE_KEYWORDS_SUBTYPES.get(c_type)
        if not subtype_key:
            raise DirectAPIError(
                0,
                f"Тип кампании «{c_type}» не поддерживает минус-фразы на уровне кампании. "
                f"Поддерживаются: {', '.join(self._NEGATIVE_KEYWORDS_SUBTYPES)}.",
            )

        sub = camp.get(subtype_key) or {}
        existing: list[str] = (sub.get("NegativeKeywords") or {}).get("Items") or []

        if mode == "append":
            existing_lower = {k.lower() for k in existing}
            added = [k for k in keywords if k.lower() not in existing_lower]
            new_list = existing + added
        else:
            added = list(keywords)
            new_list = list(keywords)

        update_payload: dict[str, Any] = {
            "method": "update",
            "params": {
                "Campaigns": [
                    {
                        "Id": campaign_id,
                        subtype_key: {
                            "NegativeKeywords": {"Items": new_list}
                        },
                    }
                ]
            },
        }
        update_data = await self._post_json_with_login(
            _CAMPAIGNS_URL, update_payload, client_login
        )

        return {
            "campaign_id": campaign_id,
            "campaign_name": camp.get("Name", ""),
            "campaign_type": c_type,
            "mode": mode,
            "previous_count": len(existing),
            "new_count": len(new_list),
            "added_count": len(added),
            "added_keywords": added,
            "existing_keywords": existing,
            "new_keywords": new_list,
            "update_result": update_data.get("result", {}),
        }

    async def get_ads(
        self,
        campaign_ids: list[int] | None = None,
        adgroup_ids: list[int] | None = None,
        ad_ids: list[int] | None = None,
        statuses: list[str] | None = None,
        client_login: str | None = None,
    ) -> dict[str, Any]:
        """
        Получить список объявлений.
        POST /v501/ads — синхронный JSON-ответ.

        Параметры SelectionCriteria (хотя бы один обязателен):
        - campaign_ids:  фильтр по кампаниям
        - adgroup_ids:   фильтр по группам объявлений
        - ad_ids:        фильтр по ID объявлений
        - statuses:      фильтр по статусам: ACCEPTED, DRAFT, MODERATION, REJECTED, UNKNOWN
        - client_login:  переопределяет Client-Login для агентских аккаунтов

        Возвращает Ads[].TextAd / Ads[].DynamicTextAd и т.д. в зависимости от типа.
        """
        criteria: dict[str, Any] = {}
        if campaign_ids:
            criteria["CampaignIds"] = campaign_ids
        if adgroup_ids:
            criteria["AdGroupIds"] = adgroup_ids
        if ad_ids:
            criteria["Ids"] = ad_ids
        if statuses:
            criteria["Statuses"] = statuses

        if not criteria:
            raise DirectAPIError(0, "get_ads: необходимо указать хотя бы один фильтр (campaign_ids, adgroup_ids или ad_ids).")

        payload: dict[str, Any] = {
            "method": "get",
            "params": {
                "SelectionCriteria": criteria,
                "FieldNames": ["Id", "AdGroupId", "CampaignId", "Status", "State", "Type"],
                "TextAdFieldNames": [
                    "Title", "Title2", "Text", "Href", "DisplayDomain",
                    "DisplayUrlPath", "Mobile",
                ],
                "DynamicTextAdFieldNames": ["Text"],
                "Page": {"Limit": 1000},
            },
        }

        if client_login:
            assert self._client is not None
            extra_headers = {"Client-Login": client_login}
            resp = await self._client.post(_ADS_URL, json=payload, headers=extra_headers)
            self._parse_units(resp)
            if resp.status_code != 200:
                raise DirectAPIError(resp.status_code, self._status_message(resp))
            data = resp.json()
            if "error" in data:
                err = data["error"]
                raise DirectAPIError(
                    int(err.get("error_code", 0)),
                    f"Ошибка API Директа (объявления): {err.get('error_detail') or err.get('error_string', '?')}",
                )
            return data

        return await self._post_json(_ADS_URL, payload)

    async def get_adgroups(
        self,
        campaign_ids: list[int] | None = None,
        adgroup_ids: list[int] | None = None,
        statuses: list[str] | None = None,
        client_login: str | None = None,
    ) -> dict[str, Any]:
        """
        Получить список групп объявлений.
        POST /v501/adgroups — синхронный JSON-ответ.

        Параметры SelectionCriteria (хотя бы один обязателен):
        - campaign_ids:  фильтр по кампаниям
        - adgroup_ids:   фильтр по ID групп
        - statuses:      ACCEPTED, DRAFT, MODERATION, REJECTED, UNKNOWN
        - client_login:  переопределяет Client-Login
        """
        criteria: dict[str, Any] = {}
        if campaign_ids:
            criteria["CampaignIds"] = campaign_ids
        if adgroup_ids:
            criteria["Ids"] = adgroup_ids
        if statuses:
            criteria["Statuses"] = statuses

        if not criteria:
            raise DirectAPIError(0, "get_adgroups: необходимо указать хотя бы один фильтр (campaign_ids или adgroup_ids).")

        payload: dict[str, Any] = {
            "method": "get",
            "params": {
                "SelectionCriteria": criteria,
                "FieldNames": [
                    "Id", "Name", "CampaignId", "Status", "ServingStatus",
                    "Type", "RegionIds",
                ],
                "Page": {"Limit": 1000},
            },
        }

        if client_login:
            assert self._client is not None
            extra_headers = {"Client-Login": client_login}
            resp = await self._client.post(_ADGROUPS_URL, json=payload, headers=extra_headers)
            self._parse_units(resp)
            if resp.status_code != 200:
                raise DirectAPIError(resp.status_code, self._status_message(resp))
            data = resp.json()
            if "error" in data:
                err = data["error"]
                raise DirectAPIError(
                    int(err.get("error_code", 0)),
                    f"Ошибка API Директа (группы): {err.get('error_detail') or err.get('error_string', '?')}",
                )
            return data

        return await self._post_json(_ADGROUPS_URL, payload)

    async def get_bids(
        self,
        campaign_ids: list[int] | None = None,
        adgroup_ids: list[int] | None = None,
        keyword_ids: list[int] | None = None,
        client_login: str | None = None,
    ) -> dict[str, Any]:
        """
        Получить текущие ставки по ключевым словам.
        POST /v501/bids — синхронный JSON-ответ.

        Параметры SelectionCriteria:
        - campaign_ids:  фильтр по кампаниям
        - adgroup_ids:   фильтр по группам объявлений
        - keyword_ids:   фильтр по ID ключевых слов
        - client_login:  переопределяет Client-Login

        Ставки возвращаются в микро-рублях (÷ 1_000_000 = рубли).
        """
        criteria: dict[str, Any] = {}
        if campaign_ids:
            criteria["CampaignIds"] = campaign_ids
        if adgroup_ids:
            criteria["AdGroupIds"] = adgroup_ids
        if keyword_ids:
            criteria["KeywordIds"] = keyword_ids

        payload: dict[str, Any] = {
            "method": "get",
            "params": {
                "SelectionCriteria": criteria,
                "FieldNames": [
                    "KeywordId", "AdGroupId", "CampaignId",
                    "Bid", "ContextBid", "StrategyPriority",
                    "AuctionBids",
                ],
                "Page": {"Limit": 10000},
            },
        }

        if client_login:
            assert self._client is not None
            extra_headers = {"Client-Login": client_login}
            resp = await self._client.post(_BIDS_URL, json=payload, headers=extra_headers)
            self._parse_units(resp)
            if resp.status_code != 200:
                raise DirectAPIError(resp.status_code, self._status_message(resp))
            data = resp.json()
            if "error" in data:
                err = data["error"]
                raise DirectAPIError(
                    int(err.get("error_code", 0)),
                    f"Ошибка API Директа (ставки): {err.get('error_detail') or err.get('error_string', '?')}",
                )
            return data

        return await self._post_json(_BIDS_URL, payload)

    async def get_report(
        self,
        field_names: list[str],
        date_range_type: str,
        report_name: str,
        *,
        report_type: str = "CUSTOM_REPORT",
        date_from: str | None = None,
        date_to: str | None = None,
        campaign_ids: list[int] | None = None,
        order_by: str | None = None,
        sort_order: str = "DESCENDING",
        top_n: int | None = None,
        include_vat: str = "NO",
        client_login: str | None = None,
    ) -> list[dict[str, str]]:
        """
        Запросить статистический отчёт через Reports API (polling).

        Параметры:
        - field_names:      список полей отчёта (измерения + метрики)
        - date_range_type:  TODAY | YESTERDAY | LAST_7_DAYS | LAST_30_DAYS |
                            THIS_MONTH | LAST_MONTH | ALL_TIME | CUSTOM_DATE ...
        - report_name:      человекочитаемое имя; к нему добавляется uuid — Direct
                            кэширует ответы по ReportName и требует уникальности
                            при изменении параметров.
        - report_type:      CUSTOM_REPORT (по умолчанию — максимальная гибкость),
                            ACCOUNT_PERFORMANCE_REPORT, CAMPAIGN_PERFORMANCE_REPORT,
                            ADGROUP_PERFORMANCE_REPORT, AD_PERFORMANCE_REPORT,
                            SEARCH_QUERY_PERFORMANCE_REPORT, REACH_AND_FREQUENCY_PERFORMANCE_REPORT
        - date_from/to:     обязательны при CUSTOM_DATE, формат YYYY-MM-DD
        - campaign_ids:     фильтр по ID кампаний (внутри SelectionCriteria)
        - order_by:         поле сортировки
        - sort_order:       ASCENDING или DESCENDING
        - top_n:            ограничение строк при парсинге TSV
        - include_vat:      YES/NO — учитывать ли НДС в суммах
        - client_login:     логин клиента для агентских аккаунтов
        """
        if date_range_type == "CUSTOM_DATE":
            if not date_from or not date_to:
                raise DirectAPIError(
                    0,
                    "При date_range_type=CUSTOM_DATE необходимо указать date_from и date_to (YYYY-MM-DD).",
                )

        # DateFrom/DateTo + фильтры по кампаниям — ВНУТРИ SelectionCriteria.
        # (старый код клал их в корень ReportDefinition → 400 error_code 8000.)
        #
        # Внимание: у Reports API v5 СВОЯ SelectionCriteria — не такая, как у
        # JSON API (campaigns.get принимает CampaignIds напрямую, Reports — нет).
        # Фильтрация по кампаниям делается через Filter[].Field=CampaignId,
        # Operator=IN, Values=[строки-id]. Values должны быть строками.
        selection: dict[str, Any] = {}
        if date_range_type == "CUSTOM_DATE":
            selection["DateFrom"] = date_from
            selection["DateTo"] = date_to
        if campaign_ids:
            selection["Filter"] = [{
                "Field": "CampaignId",
                "Operator": "IN",
                "Values": [str(cid) for cid in campaign_ids],
            }]

        # Уникальное имя — иначе Direct может вернуть кэшированный отчёт
        # с несовпадающей схемой полей.
        unique_name = f"{report_name}_{uuid.uuid4().hex[:8]}"

        # ВАЖНО: поля идут напрямую под params, без обёртки ReportDefinition.
        # Старый код: {"params": {"ReportDefinition": {...}}}  ← вызывал 400
        # Правильно: {"params": {"SelectionCriteria": ..., "FieldNames": ..., ...}}
        params: dict[str, Any] = {
            "SelectionCriteria": selection,
            "FieldNames": field_names,
            "ReportName": unique_name,
            "ReportType": report_type,
            "DateRangeType": date_range_type,
            "Format": "TSV",
            "IncludeVAT": include_vat,
        }

        if order_by:
            params["OrderBy"] = [{"Field": order_by, "SortOrder": sort_order}]

        payload: dict[str, Any] = {"params": params}

        return await self._post_report(payload, top_n=top_n, client_login=client_login)
