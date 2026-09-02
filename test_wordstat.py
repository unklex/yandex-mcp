"""
Быстрая проверка Wordstat через Yandex Cloud Search API v2.

Читает YANDEX_SEARCH_API_KEY и YANDEX_FOLDER_ID из окружения (.env) —
секреты в коде не хранятся. Прогоняет все три эндпоинта + дерево регионов
напрямую через WordstatClient, поэтому подтверждает связку «ключ + folder»
независимо от перезапуска MCP-сервера.

Запуск:
    python test_wordstat.py
    python test_wordstat.py "своя фраза"
"""

from __future__ import annotations

import asyncio
import os
import sys

from dotenv import load_dotenv

from wordstat_client import WordstatClient, WordstatAPIError

# На Windows консоль часто не в UTF-8 — принудительно, чтобы русский не «плыл».
try:
    sys.stdout.reconfigure(encoding="utf-8")  # type: ignore[attr-defined]
except (AttributeError, ValueError):
    pass

load_dotenv()


def _int(v) -> int:
    try:
        return int(str(v).strip())
    except (TypeError, ValueError):
        return 0


async def run(phrase: str) -> int:
    key = os.getenv("YANDEX_SEARCH_API_KEY", "").strip()
    folder = os.getenv("YANDEX_FOLDER_ID", "").strip()
    if not key or not folder:
        print("ОШИБКА: задайте YANDEX_SEARCH_API_KEY и YANDEX_FOLDER_ID в .env")
        return 1

    async with WordstatClient(key, folder) as ws:
        # 1. topRequests
        print(f"\n=== 1. topRequests: «{phrase}» (Москва, 213) ===")
        try:
            d = await ws.top_requests(phrase, num_phrases=5, regions=["213"])
            print(f"  totalCount = {_int(d.get('totalCount'))}")
            for r in (d.get("results") or [])[:5]:
                print(f"    {r.get('phrase')}: {_int(r.get('count'))}")
            for a in (d.get("associations") or [])[:5]:
                print(f"    ~ {a.get('phrase')}: {_int(a.get('count'))}")
        except WordstatAPIError as e:
            print(f"  ОШИБКА {e.status}: {e}")

        # 2. dynamics (месячная динамика, 2025 год)
        print(f"\n=== 2. dynamics: «{phrase}» PERIOD_MONTHLY 2025 ===")
        try:
            d = await ws.dynamics(
                phrase,
                period="PERIOD_MONTHLY",
                from_date="2025-01-01T00:00:00Z",
                to_date="2025-12-31T00:00:00Z",  # ВАЖНО: последний день месяца
                regions=["213"],
            )
            rows = d.get("results") or []
            print(f"  точек: {len(rows)}")
            for r in rows:
                print(f"    {r.get('date')}: count={_int(r.get('count'))}, share={r.get('share')}")
        except WordstatAPIError as e:
            print(f"  ОШИБКА {e.status}: {e}")

        # 3. regions
        print(f"\n=== 3. regions: «{phrase}» REGION_ALL (топ-5) ===")
        try:
            d = await ws.regions(phrase, region="REGION_ALL")
            names = await ws.get_regions_map()
            rows = sorted(
                (d.get("results") or []),
                key=lambda r: _int(r.get("count")),
                reverse=True,
            )[:5]
            for r in rows:
                rid = str(r.get("region", ""))
                print(f"    {rid} {names.get(rid, '')}: count={_int(r.get('count'))}, "
                      f"share={r.get('share')}, affinity={r.get('affinityIndex')}")
        except WordstatAPIError as e:
            print(f"  ОШИБКА {e.status}: {e}")

    print("\nГотово.")
    return 0


if __name__ == "__main__":
    phrase = sys.argv[1] if len(sys.argv) > 1 else "купить собаку"
    sys.exit(asyncio.run(run(phrase)))
