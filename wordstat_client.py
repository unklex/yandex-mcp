"""
Асинхронный клиент Yandex Cloud Search API v2 (Wordstat), часть Yandex Cloud
AI Studio. Заменяет legacy Direct API v4 (CreateNewWordstatReport / polling).

Особенности API v2:
  - Синхронный REST: один POST → ответ сразу, без создания отчёта и polling.
  - Аутентификация: заголовок `Authorization: Api-Key <key>` (НЕ IAM/OAuth).
  - В каждом теле запроса обязателен `folderId` — ID каталога Yandex Cloud
    того сервисного аккаунта, которому принадлежит ключ.
  - Имена полей в теле — camelCase (folderId, numPhrases, fromDate, toDate).
  - Числовые поля (count / totalCount, иногда share) приходят строками —
    приведение к int/float делается на уровне инструментов.

Эндпоинты:
  POST /v2/wordstat/topRequests    — топ запросов + ассоциации
  POST /v2/wordstat/dynamics       — динамика показов по периодам
  POST /v2/wordstat/regions        — распределение по регионам (окно 30 дней)
  POST /v2/wordstat/getRegionsTree — дерево регионов (для маппинга ID → имя)

Лимиты: ~5–10 RPS. Ретраи на 429/5xx с экспоненциальным backoff и учётом
заголовка Retry-After (по образцу MetricaClient).

Wordstat через Search API на данный момент бесплатен — биллинг не считаем.
"""

from __future__ import annotations

import asyncio
import json
import time
from pathlib import Path
from typing import Any

import httpx

BASE_URL = "https://searchapi.api.cloud.yandex.net"

_MAX_RETRIES = 3
_RETRY_STATUSES = {429, 500, 502, 503, 504}

# Кэш дерева регионов: id → название. Дерево ~200 КБ, обновляется редко,
# поэтому кэшируем на диск и перечитываем не чаще, чем раз в TTL.
_CACHE_DIR = Path(__file__).parent / ".cache"
_REGIONS_CACHE_FILE = _CACHE_DIR / "wordstat_regions_tree.json"
_REGIONS_CACHE_TTL_SECONDS = 30 * 24 * 3600  # 30 дней


class WordstatAPIError(Exception):
    """Ошибка Yandex Cloud Search API (Wordstat) с HTTP-статусом.

    Тело ответа сохраняем verbatim (без усечения смысловой части) — для
    отладки проблем с folderId / scope ключа сервер часто кладёт детали
    именно в тело.
    """

    def __init__(self, status: int, message: str) -> None:
        self.status = status
        super().__init__(message)


class WordstatClient:
    """
    Асинхронный клиент Search API v2 (Wordstat).

    Жизненный цикл — через async context manager (открывается в lifespan):
        async with WordstatClient(api_key, folder_id) as ws:
            data = await ws.top_requests("купить собаку")

    Один пул соединений httpx.AsyncClient переиспользуется между запросами.
    """

    def __init__(self, api_key: str, folder_id: str) -> None:
        self._api_key = api_key
        self._folder_id = folder_id
        self._headers = {
            "Authorization": f"Api-Key {api_key}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> "WordstatClient":
        self._client = httpx.AsyncClient(headers=self._headers, timeout=30.0)
        return self

    async def __aexit__(self, *_: Any) -> None:
        if self._client:
            await self._client.aclose()

    # ------------------------------------------------------------------
    # Транспорт
    # ------------------------------------------------------------------

    async def _post(self, path: str, body: dict[str, Any]) -> dict[str, Any]:
        """
        POST JSON с ретраями. folderId добавляется автоматически.
        Задержки: 1с → 2с → 4с; при 429 уважаем Retry-After (до 60с).
        """
        assert self._client is not None, (
            "Клиент не инициализирован. Используйте: async with WordstatClient(...) as ws"
        )

        payload = {"folderId": self._folder_id, **body}
        url = f"{BASE_URL}{path}"
        last_exc: Exception | None = None

        for attempt in range(_MAX_RETRIES):
            try:
                resp = await self._client.post(url, json=payload)
            except httpx.RequestError as exc:
                raise WordstatAPIError(
                    0, f"Сетевая ошибка при запросе к Search API (Wordstat): {exc}"
                ) from exc

            if resp.status_code == 200:
                return resp.json()

            if resp.status_code in _RETRY_STATUSES and attempt < _MAX_RETRIES - 1:
                wait = 2**attempt  # 1с, 2с, 4с
                if resp.status_code == 429:
                    retry_after = resp.headers.get("Retry-After", "")
                    if retry_after.isdigit():
                        wait = min(int(retry_after), 60)
                last_exc = WordstatAPIError(resp.status_code, self._error_message(resp))
                await asyncio.sleep(wait)
                continue

            raise WordstatAPIError(resp.status_code, self._error_message(resp))

        raise last_exc or WordstatAPIError(0, "Неизвестная ошибка Search API (Wordstat)")

    @staticmethod
    def _error_message(resp: httpx.Response) -> str:
        """Сообщение об ошибке с HTTP-статусом и телом ответа verbatim.

        Тело сохраняем целиком (до 1000 символов) — там бывают детали про
        неверный folderId или недостаточный scope ключа
        (yc.search-api.execute / роль search-api.webSearch.user).
        """
        hints = {
            401: "Проверьте YANDEX_SEARCH_API_KEY (заголовок Authorization: Api-Key).",
            403: (
                "Нет прав. Убедитесь, что у сервисного аккаунта есть роль "
                "search-api.webSearch.user, у ключа scope yc.search-api.execute, "
                "и что YANDEX_FOLDER_ID соответствует этому аккаунту."
            ),
            404: "Эндпоинт не найден. Проверьте путь/версию API.",
            429: "Превышен лимит запросов к Search API (~5–10 RPS). Попробуйте позже.",
        }
        body = resp.text[:1000]
        base = f"Search API (Wordstat) вернул HTTP {resp.status_code}."
        hint = hints.get(resp.status_code)
        parts = [base]
        if hint:
            parts.append(hint)
        if body.strip():
            parts.append(f"Тело ответа: {body}")
        return " ".join(parts)

    # ------------------------------------------------------------------
    # Публичные методы (возвращают raw-JSON; приведение типов — в инструментах)
    # ------------------------------------------------------------------

    async def top_requests(
        self,
        phrase: str,
        *,
        num_phrases: int = 50,
        regions: list[str] | None = None,
        devices: list[str] | None = None,
    ) -> dict[str, Any]:
        """POST /v2/wordstat/topRequests для одной фразы."""
        body: dict[str, Any] = {"phrase": phrase, "numPhrases": num_phrases}
        if regions:
            body["regions"] = regions
        if devices:
            body["devices"] = devices
        return await self._post("/v2/wordstat/topRequests", body)

    async def dynamics(
        self,
        phrase: str,
        *,
        period: str,
        from_date: str,
        to_date: str,
        regions: list[str] | None = None,
        devices: list[str] | None = None,
    ) -> dict[str, Any]:
        """POST /v2/wordstat/dynamics. Даты — RFC3339 (например, 2025-12-29T00:00:00Z)."""
        body: dict[str, Any] = {
            "phrase": phrase,
            "period": period,
            "fromDate": from_date,
            "toDate": to_date,
        }
        if regions:
            body["regions"] = regions
        if devices:
            body["devices"] = devices
        return await self._post("/v2/wordstat/dynamics", body)

    async def regions(
        self,
        phrase: str,
        *,
        region: str = "REGION_ALL",
        devices: list[str] | None = None,
    ) -> dict[str, Any]:
        """POST /v2/wordstat/regions. region — одиночный enum, не список ID."""
        body: dict[str, Any] = {"phrase": phrase, "region": region}
        if devices:
            body["devices"] = devices
        return await self._post("/v2/wordstat/regions", body)

    async def get_regions_tree(self) -> dict[str, Any]:
        """POST /v2/wordstat/getRegionsTree — рекурсивное дерево регионов."""
        return await self._post("/v2/wordstat/getRegionsTree", {})

    # ------------------------------------------------------------------
    # Кэш дерева регионов (id → название)
    # ------------------------------------------------------------------

    @staticmethod
    def _flatten_regions_tree(tree: dict[str, Any]) -> dict[str, str]:
        """Разворачивает рекурсивное дерево в плоский маппинг {id: label}.

        Ожидаемая форма: {"regions": [{id, label, children: [...]}, ...]}.
        Устойчив к отсутствию children и к разным написаниям ключей.
        """
        result: dict[str, str] = {}

        def walk(nodes: Any) -> None:
            if not isinstance(nodes, list):
                return
            for node in nodes:
                if not isinstance(node, dict):
                    continue
                node_id = node.get("id") or node.get("geoId") or node.get("regionId")
                label = node.get("label") or node.get("name") or node.get("title")
                if node_id is not None and label:
                    result[str(node_id)] = str(label)
                walk(node.get("children") or node.get("regions"))

        walk(tree.get("regions") or tree.get("children") or [])
        return result

    def _read_cached_regions_map(self, ttl_seconds: int) -> dict[str, str] | None:
        """Читает кэш с диска, если файл существует и не старше TTL."""
        try:
            if not _REGIONS_CACHE_FILE.exists():
                return None
            age = time.time() - _REGIONS_CACHE_FILE.stat().st_mtime
            if age > ttl_seconds:
                return None
            with _REGIONS_CACHE_FILE.open("r", encoding="utf-8") as f:
                payload = json.load(f)
            mapping = payload.get("map")
            if isinstance(mapping, dict) and mapping:
                return {str(k): str(v) for k, v in mapping.items()}
        except (OSError, ValueError, json.JSONDecodeError):
            return None
        return None

    def _write_cached_regions_map(self, mapping: dict[str, str]) -> None:
        """Пишет кэш на диск (best-effort; ошибки записи не фатальны)."""
        try:
            _CACHE_DIR.mkdir(parents=True, exist_ok=True)
            with _REGIONS_CACHE_FILE.open("w", encoding="utf-8") as f:
                json.dump({"fetched_at": time.time(), "map": mapping}, f, ensure_ascii=False)
        except OSError:
            pass

    async def get_regions_map(
        self, *, ttl_seconds: int = _REGIONS_CACHE_TTL_SECONDS
    ) -> dict[str, str]:
        """Возвращает маппинг {region_id: name}, кэшируя дерево на диск.

        Порядок: свежий кэш → иначе запросить getRegionsTree, развернуть и
        записать кэш. Если запрос упал, но есть просроченный кэш — используем
        его как fallback (лучше устаревшие имена, чем никакие).
        """
        cached = self._read_cached_regions_map(ttl_seconds)
        if cached is not None:
            return cached

        try:
            tree = await self.get_regions_tree()
        except WordstatAPIError:
            stale = self._read_cached_regions_map(ttl_seconds=10**12)  # игнорируем TTL
            if stale is not None:
                return stale
            raise

        mapping = self._flatten_regions_tree(tree)
        if mapping:
            self._write_cached_regions_map(mapping)
        return mapping
