"""
Yandex Metrica MCP Server — точка входа.

Запуск:
    python server.py

Транспорт: stdio (для интеграции с Claude Desktop / Cowork).

Порядок инициализации:
  1. load_dotenv() загружает .env
  2. Импорт app.py создаёт mcp = FastMCP(...) с lifespan
  3. Импорт каждого модуля tools/* регистрирует инструменты через @mcp.tool()
     (статическая регистрация — инструменты видны Claude сразу при подключении)
  4. mcp.run() запускает stdio-сервер; lifespan открывает MetricaClient
"""

from dotenv import load_dotenv

# Загружаем .env ДО импорта app.py, чтобы os.getenv() видел переменные
load_dotenv()

from app import mcp  # noqa: E402  (после load_dotenv)

# Импортируем все модули инструментов — это регистрирует @mcp.tool() декораторы
import tools.traffic   # noqa: F401, E402
import tools.sources   # noqa: F401, E402
import tools.pages     # noqa: F401, E402
import tools.goals     # noqa: F401, E402
import tools.audience  # noqa: F401, E402
import tools.realtime  # noqa: F401, E402
import tools.compare          # noqa: F401, E402
import tools.direct_campaigns  # noqa: F401, E402
import tools.direct_stats      # noqa: F401, E402
import tools.direct_reports    # noqa: F401, E402
import tools.direct_ads        # noqa: F401, E402

if __name__ == "__main__":
    mcp.run(transport="stdio")
