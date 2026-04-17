"""
Централизованная конфигурация MCP-сервера.

Все переменные окружения читаются и валидируются здесь.
Остальные модули импортируют объект Settings, не обращаясь к os.getenv() напрямую.

Обязательные переменные:
    YANDEX_METRICA_TOKEN      — OAuth-токен Яндекс.Метрики
    YANDEX_METRICA_COUNTER_ID — ID основного счётчика Метрики

Необязательные:
    YANDEX_METRICA_COUNTERS      — несколько счётчиков: alias1:id1,alias2:id2
    YANDEX_DIRECT_ACCOUNTS       — несколько аккаунтов Директа: alias1:token1,alias2:token2
    YANDEX_DIRECT_TOKEN          — один аккаунт Директа (упрощённый вариант)
    YANDEX_DIRECT_CLIENT_LOGIN   — логин клиента для агентских аккаунтов
"""

from __future__ import annotations

import os
import sys
from dataclasses import dataclass, field


@dataclass(frozen=True)
class Settings:
    # ── Яндекс.Метрика ──────────────────────────────────────────────────────
    metrica_token: str
    """OAuth-токен для Yandex Metrica Reporting API."""

    metrica_counter_id: str
    """ID основного счётчика Метрики (используется по умолчанию)."""

    metrica_counters: dict[str, str]
    """Маппинг псевдоним → counter_id для всех счётчиков.
    Всегда содержит хотя бы один элемент: основной счётчик."""

    # ── Яндекс.Директ ───────────────────────────────────────────────────────
    direct_accounts: dict[str, str]
    """Маппинг псевдоним → OAuth-токен для аккаунтов Директа.
    Гарантированно непустой (при отсутствии конфигурации использует
    YANDEX_DIRECT_TOKEN или fallback на metrica_token)."""

    direct_client_login: str | None
    """Логин клиента для агентских аккаунтов (Client-Login заголовок)."""


def _require(name: str) -> str:
    """Возвращает значение переменной или завершает процесс с сообщением об ошибке."""
    value = os.getenv(name, "").strip()
    if not value:
        print(
            f"ОШИБКА: Переменная окружения {name} не задана.\n"
            f"Добавьте её в файл .env или передайте через окружение запуска.",
            file=sys.stderr,
        )
        sys.exit(1)
    return value


def _parse_aliases(raw: str) -> dict[str, str]:
    """
    Парсит строку псевдонимов формата: alias1:value1,alias2:value2

    Примеры:
        YANDEX_METRICA_COUNTERS=site1:12345678,site2:87654321
        YANDEX_DIRECT_ACCOUNTS=main:AQAAAABxxx,agency:AQAAAAByyy
    """
    result: dict[str, str] = {}
    for part in raw.split(","):
        part = part.strip()
        if ":" in part:
            alias, value = part.split(":", 1)
            result[alias.strip().lower()] = value.strip()
    return result


def load() -> Settings:
    """
    Читает переменные окружения и возвращает объект Settings.

    Вызывать после load_dotenv() — т.е. из server.py, не из модулей инструментов.
    """
    metrica_token = _require("YANDEX_METRICA_TOKEN")
    metrica_counter_id = _require("YANDEX_METRICA_COUNTER_ID")

    # Счётчики Метрики: основной + дополнительные алиасы
    counters_raw = os.getenv("YANDEX_METRICA_COUNTERS", "").strip()
    metrica_counters: dict[str, str] = _parse_aliases(counters_raw) if counters_raw else {}
    # Всегда добавляем основной счётчик по его числовому ID
    metrica_counters[metrica_counter_id] = metrica_counter_id

    # Аккаунты Директа
    direct_accounts_raw = os.getenv("YANDEX_DIRECT_ACCOUNTS", "").strip()
    if direct_accounts_raw:
        direct_accounts = _parse_aliases(direct_accounts_raw)
    else:
        # Один аккаунт: YANDEX_DIRECT_TOKEN или fallback на токен Метрики
        fallback_token = os.getenv("YANDEX_DIRECT_TOKEN", "").strip() or metrica_token
        direct_accounts = {"default": fallback_token}

    direct_client_login = os.getenv("YANDEX_DIRECT_CLIENT_LOGIN", "").strip() or None

    return Settings(
        metrica_token=metrica_token,
        metrica_counter_id=metrica_counter_id,
        metrica_counters=metrica_counters,
        direct_accounts=direct_accounts,
        direct_client_login=direct_client_login,
    )
