"""
Создаёт единственный экземпляр FastMCP-сервера и управляет жизненным циклом
клиентов Yandex Metrica и Yandex Direct.

Этот модуль импортируется из tools/*.py для регистрации инструментов (@mcp.tool()),
и из server.py для запуска сервера (mcp.run()).

Порядок инициализации:
  1. server.py вызывает load_dotenv()
  2. server.py импортирует этот модуль → создаётся mcp = FastMCP(...)
  3. server.py импортирует все модули tools/* → инструменты регистрируются статически
  4. mcp.run() запускает сервер; lifespan открывает клиентов перед первым вызовом
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, AsyncExitStack
from typing import TYPE_CHECKING

from mcp.server.fastmcp import FastMCP

import settings as cfg
from metrica_client import MetricaClient
from direct_client import DirectClient

if TYPE_CHECKING:
    pass


def resolve_counter(counter: str | None, lc: dict) -> str:
    """
    Резолвит имя счётчика или его ID в числовой ID.

    Поддерживает:
    - None → возвращает default_counter_id из конфига
    - числовой ID → возвращает как есть
    - алиас (например, 'main') → ищет в counters_map
    """
    if not counter:
        return lc["default_counter_id"]

    counter_lower = counter.lower()
    counters_map: dict[str, str] = lc.get("counters_map", {})

    if counter_lower in counters_map:
        return counters_map[counter_lower]

    return counter


def resolve_direct_client(account: str | None, lc: dict) -> "DirectClient | None":
    """
    Возвращает DirectClient для указанного аккаунта или основной клиент.

    Поддерживает:
    - None → возвращает primary direct_client
    - алиас (например, 'main') → ищет в direct_clients по имени
    - если алиас не найден → возвращает primary direct_client как fallback
    """
    if not account:
        return lc.get("direct_client")
    clients: dict[str, DirectClient] = lc.get("direct_clients", {})
    return clients.get(account.lower()) or lc.get("direct_client")


@asynccontextmanager
async def lifespan(server: FastMCP) -> AsyncIterator[dict]:
    """
    Открывает HTTP-клиенты Yandex Metrica и Yandex Direct при старте сервера.

    Конфигурация читается через settings.load() — единственное место, где
    обращаемся к переменным окружения.

    Контекст lifespan доступен в инструментах через:
        ctx.request_context.lifespan_context["client"]             # MetricaClient
        ctx.request_context.lifespan_context["direct_client"]      # DirectClient (primary)
        ctx.request_context.lifespan_context["direct_clients"]     # dict[alias, DirectClient]
        ctx.request_context.lifespan_context["default_counter_id"]
        ctx.request_context.lifespan_context["counters_map"]
    """
    s = cfg.load()

    async with MetricaClient(token=s.metrica_token, counter_id=s.metrica_counter_id) as metrica_client:
        async with AsyncExitStack() as stack:
            direct_clients: dict[str, DirectClient] = {}
            for alias, token in s.direct_accounts.items():
                dc = await stack.enter_async_context(
                    DirectClient(token=token, client_login=s.direct_client_login)
                )
                direct_clients[alias] = dc

            # Первый аккаунт — основной (primary)
            primary_name = next(iter(direct_clients))
            primary_client = direct_clients[primary_name]

            yield {
                "client": metrica_client,
                "direct_client": primary_client,     # для обратной совместимости
                "direct_clients": direct_clients,     # все именованные аккаунты
                "default_counter_id": s.metrica_counter_id,
                "counters_map": s.metrica_counters,
            }


mcp = FastMCP(
    name="Яндекс.Метрика + Директ",
    lifespan=lifespan,
)
