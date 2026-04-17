"""
Асинхронный клиент Yandex Metrica Reporting API.

Базовые URL:
  Статистика:  https://api-metrika.yandex.net/stat/v1/data
  По времени:  https://api-metrika.yandex.net/stat/v1/data/bytime
  Сравнение:   https://api-metrika.yandex.net/stat/v1/data/comparison
  Управление:  https://api-metrika.yandex.net/management/v1
"""

from __future__ import annotations

import asyncio
import re
from typing import Any

import httpx

BASE_URL = "https://api-metrika.yandex.net/stat/v1/data"
MANAGEMENT_BASE = "https://api-metrika.yandex.net/management/v1"

_RETRY_STATUSES = {429, 500, 502, 503, 504}
_MAX_RETRIES = 3

# Допустимые форматы дат: YYYY-MM-DD, today, yesterday, NdaysAgo
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$|^today$|^yesterday$|^\d+daysAgo$")


class MetricaAPIError(Exception):
    """Ошибка Yandex Metrica API с HTTP-статусом и сообщением на русском."""

    def __init__(self, status: int, message: str) -> None:
        self.status = status
        super().__init__(message)


def validate_date(date: str) -> None:
    """
    Проверяет формат даты. Допустимые форматы:
      YYYY-MM-DD  — конкретная дата (например, 2024-01-15)
      today       — сегодня
      yesterday   — вчера
      NdaysAgo    — N дней назад (например, 7daysAgo, 30daysAgo)

    Raises:
        ValueError: если формат не соответствует ни одному из допустимых.
    """
    if not _DATE_RE.match(date):
        raise ValueError(
            f"Неверный формат даты: «{date}». "
            "Используйте: YYYY-MM-DD, today, yesterday или NdaysAgo (например, 7daysAgo)."
        )


class MetricaClient:
    """
    Асинхронный клиент Yandex Metrica Reporting API.

    Управление жизненным циклом через async context manager:
        async with MetricaClient(token, counter_id) as client:
            data = await client.get_data(...)

    Особенности:
    - Единый пул соединений httpx.AsyncClient (не создаётся заново на каждый запрос)
    - Автоматические повторные попытки при 429/5xx (до 3 раз, экспоненциальная задержка)
    - Уважает заголовок Retry-After при 429
    - Понятные сообщения об ошибках на русском языке
    - Определение семплирования и добавление предупреждения в результат
    """

    def __init__(self, token: str, counter_id: str) -> None:
        self._token = token
        self.default_counter_id = counter_id
        self._headers = {
            "Authorization": f"OAuth {token}",
            "Accept": "application/json",
        }
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "MetricaClient":
        self._client = httpx.AsyncClient(
            headers=self._headers,
            timeout=30.0,
        )
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._client:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # Внутренние методы
    # ------------------------------------------------------------------

    async def _get(self, url: str, params: dict[str, Any]) -> dict[str, Any]:
        """
        GET-запрос с повторными попытками.
        Задержки: 1с → 2с → 4с (экспоненциальный backoff).
        """
        assert self._client is not None, (
            "Клиент не инициализирован. Используйте: async with MetricaClient(...) as client"
        )

        last_exc: Exception | None = None

        for attempt in range(_MAX_RETRIES):
            try:
                resp = await self._client.get(url, params=params)
            except httpx.RequestError as exc:
                raise MetricaAPIError(0, f"Сетевая ошибка: {exc}") from exc

            if resp.status_code == 200:
                return resp.json()

            # 400 — неверные параметры: извлекаем сообщение из тела ответа
            if resp.status_code == 400:
                try:
                    error_body = resp.json()
                    api_message = error_body.get("message", resp.text[:400])
                except Exception:
                    api_message = resp.text[:400]
                raise MetricaAPIError(
                    400,
                    f"Неверные параметры запроса: {api_message}. "
                    "Проверьте совместимость метрик и измерений (нельзя смешивать ym:s: и ym:pv: в одном запросе).",
                )

            # Повторяемые ошибки
            if resp.status_code in _RETRY_STATUSES and attempt < _MAX_RETRIES - 1:
                wait = 2**attempt  # 1с, 2с, 4с
                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After", "")
                    if retry_after.isdigit():
                        wait = min(int(retry_after), 60)
                last_exc = MetricaAPIError(resp.status_code, self._status_message(resp))
                await asyncio.sleep(wait)
                continue

            # Окончательная ошибка
            raise MetricaAPIError(resp.status_code, self._status_message(resp))

        raise last_exc or MetricaAPIError(0, "Неизвестная ошибка при выполнении запроса")

    @staticmethod
    def _status_message(resp: httpx.Response) -> str:
        messages: dict[int, str] = {
            401: "Ошибка аутентификации. Проверьте токен YANDEX_METRICA_TOKEN.",
            403: (
                "Нет доступа к счётчику. Убедитесь, что токен имеет разрешение "
                "metrika:read и аккаунт имеет доступ к этому счётчику."
            ),
            404: "Счётчик не найден. Проверьте YANDEX_METRICA_COUNTER_ID.",
            429: "Превышен лимит запросов к API Яндекс.Метрики. Попробуйте позже.",
            500: "Внутренняя ошибка сервера Яндекс.Метрики. Попробуйте позже.",
        }
        return messages.get(resp.status_code, f"Неожиданный ответ API (HTTP {resp.status_code}): {resp.text[:200]}")

    def _stat_params(self, counter_id: str, extra: dict[str, Any]) -> dict[str, Any]:
        """Добавляет обязательные параметры ids и lang=ru ко всем запросам статистики."""
        return {"ids": counter_id, "lang": "ru", **extra}

    @staticmethod
    def _maybe_add_sampling_warning(api_response: dict[str, Any], result: dict[str, Any]) -> dict[str, Any]:
        """
        Если API вернул данные с семплированием — добавляет предупреждение в результат.
        Поле _sampling_warning будет видно LLM и может быть передано пользователю.
        """
        if api_response.get("containsSampledData"):
            sample_share = api_response.get("sampleShare", 0)
            accuracy = round(sample_share * 100, 1)
            result["_sampling_warning"] = (
                f"⚠️ Данные выборочные (семплирование). "
                f"Точность: {accuracy}% от реального трафика. "
                "Для получения точных данных сократите период или запросите меньший объём данных."
            )
        return result

    def _resolve_counter(self, counter_id: str | int | None) -> str:
        """Возвращает counter_id из аргумента или из конфига по умолчанию."""
        return str(counter_id) if counter_id else self.default_counter_id

    # ------------------------------------------------------------------
    # Публичные методы запросов
    # ------------------------------------------------------------------

    async def get_data(
        self,
        metrics: str,
        *,
        counter_id: str | int | None = None,
        dimensions: str | None = None,
        date1: str = "7daysAgo",
        date2: str = "yesterday",
        sort: str | None = None,
        limit: int = 20,
        filters: str | None = None,
    ) -> dict[str, Any]:
        """
        Табличный отчёт: GET /stat/v1/data

        Возвращает raw-ответ API, дополненный _sampling_warning при необходимости.
        """
        validate_date(date1)
        validate_date(date2)

        cid = self._resolve_counter(counter_id)
        params: dict[str, Any] = {
            "metrics": metrics,
            "date1": date1,
            "date2": date2,
            "limit": limit,
        }
        if dimensions:
            params["dimensions"] = dimensions
        if sort:
            params["sort"] = sort
        if filters:
            params["filters"] = filters

        data = await self._get(BASE_URL, self._stat_params(cid, params))
        self._maybe_add_sampling_warning(data, data)
        return data

    async def get_bytime(
        self,
        metrics: str,
        *,
        counter_id: str | int | None = None,
        date1: str = "7daysAgo",
        date2: str = "today",
        group: str = "day",
        dimensions: str | None = None,
        limit: int = 10,
    ) -> dict[str, Any]:
        """
        Временной ряд: GET /stat/v1/data/bytime
        """
        validate_date(date1)
        validate_date(date2)

        cid = self._resolve_counter(counter_id)
        params: dict[str, Any] = {
            "metrics": metrics,
            "date1": date1,
            "date2": date2,
            "group": group,
            "limit": limit,
        }
        if dimensions:
            params["dimensions"] = dimensions

        data = await self._get(f"{BASE_URL}/bytime", self._stat_params(cid, params))
        self._maybe_add_sampling_warning(data, data)
        return data

    async def get_comparison(
        self,
        metrics: str,
        date1_a: str,
        date2_a: str,
        date1_b: str,
        date2_b: str,
        *,
        counter_id: str | int | None = None,
        dimensions: str | None = None,
    ) -> dict[str, Any]:
        """
        Сравнение двух периодов: GET /stat/v1/data/comparison
        Возвращает totals_a и totals_b для сравниваемых периодов.
        """
        for d in (date1_a, date2_a, date1_b, date2_b):
            validate_date(d)

        cid = self._resolve_counter(counter_id)
        params: dict[str, Any] = {
            "metrics": metrics,
            "date1_a": date1_a,
            "date2_a": date2_a,
            "date1_b": date1_b,
            "date2_b": date2_b,
        }
        if dimensions:
            params["dimensions"] = dimensions

        data = await self._get(f"{BASE_URL}/comparison", self._stat_params(cid, params))
        self._maybe_add_sampling_warning(data, data)
        return data

    async def get_goals_list(self, counter_id: int | None = None) -> dict[str, Any]:
        """
        Список целей счётчика: GET /management/v1/counter/{id}/goals
        Использует Management API (не Reporting API).
        """
        cid = self._resolve_counter(counter_id)
        url = f"{MANAGEMENT_BASE}/counter/{cid}/goals"
        return await self._get(url, {})
