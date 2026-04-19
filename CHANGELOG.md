# Changelog

Формат основан на [Keep a Changelog](https://keepachangelog.com/ru/1.0.0/).

---

## [1.3.0] — 2026-04-19

### Добавлено

#### Яндекс.Директ — 10 новых инструментов (write-операции + Wordstat + гибкие отчёты)

**`tools/direct_campaigns.py`** — один новый write-инструмент:
- `add_direct_negative_keywords` — добавить/заменить минус-фразы на уровне кампании.
  Сам определяет подтип кампании (TextCampaign / DynamicTextCampaign / UnifiedCampaign /
  SmartCampaign / MobileAppCampaign / MCBannerCampaign), читает существующий список,
  делает case-insensitive merge (`append`) или полную замену (`replace`).

**`tools/direct_negative_kw_sets.py`** — общие наборы минус-фраз (`NegativeKeywordSharedSets`),
полный CRUD:
- `get_negative_keyword_sets` — список наборов с фразами и `SharedAccountId`
- `add_negative_keyword_set` — создание нового набора (с дедупликацией фраз)
- `update_negative_keyword_set` — смена имени и/или полная замена списка фраз
  (хотя бы один параметр обязателен)
- `delete_negative_keyword_sets` — удаление по списку ID, с per-ID детализацией ошибок

**`tools/wordstat.py`** — исследование ключевых фраз через legacy API v4
(`https://api.direct.yandex.ru/live/v4/json/`), polling-модель:
- `wordstat_top_requests` — топ смежных запросов (`SearchedWith`) по 1–10 фразам,
  с фильтром по `GeoID`
- `wordstat_dynamics` — помесячная динамика показов (`MonthList`) за ~24 месяца,
  с min/max/avg в summary
- `wordstat_regions` — распределение показов по регионам (`GeoList`) с `ShowsPercent`

**`tools/direct_campaign_stats.py`** — углублённая статистика через Reports API:
- `get_campaign_stats` — `CAMPAIGN_PERFORMANCE_REPORT` по заданным `campaign_ids`
  за произвольный период с суммирующей строкой `totals` (пересчитанные CTR/CPC)
- `get_custom_report` — универсальный отчёт: любой `report_type`
  (CAMPAIGN/AD/ADGROUP/CRITERIA/SEARCH_QUERY/ACCOUNT/CUSTOM/REACH_AND_FREQUENCY),
  произвольные `fields`, фильтр по кампаниям, подсказки по полям для каждого типа

**`direct_client.py`** — новые методы и константы:
- `_post_json_with_login(url, payload, client_login)` — общая обёртка с per-request
  Client-Login override (устраняет дублирование в `get_campaigns`/`get_ads`/…)
- `set_campaign_negative_keywords(campaign_id, keywords, mode, client_login)` —
  read-modify-write для кампаний с определением подтипа и режимами `append`/`replace`
- `_post_negative_kw_sets(payload, client_login)` — обёртка для сервиса
  `/json/v5/negativekeywordsharedsets`
- `_wordstat_request(method, param, client_login)` — единый POST к Wordstat v4
  (обрабатывает `{"error_code": N, "error_str": ...}` внутри 200-ответа)
- `_wordstat_poll(report_id, client_login)` — опрос `GetWordstatReport` до готовности
  (до 10 попыток × 3 с)
- Новые URL/лимиты: `_NEGATIVE_KW_SETS_URL`, `_WORDSTAT_URL`, `_WORDSTAT_MAX_POLLS`,
  `_WORDSTAT_POLL_SLEEP`
- Словарь `_NEGATIVE_KEYWORDS_SUBTYPES` — маппинг `Type` → PascalCase-ключ подтипа

**`server.py`** — зарегистрированы модули `direct_negative_kw_sets`, `wordstat`,
`direct_campaign_stats`

**`test_direct.py`** — три новых standalone-валидатора:
- `list_negative_keyword_sets(token, client_login)` — read-only probe сервиса
  общих наборов
- `wordstat_top(token, phrases, geo_ids, client_login)` — полный create→poll цикл
  Wordstat v4
- `custom_report(token, report_type, fields, ...)` — произвольный отчёт через
  существующий `call_report`
- Вызовы добавлены в `main()` (шаги 7, 8, 9)

### Итого инструментов

| Категория | Инструменты |
|-----------|-------------|
| Метрика | 7 |
| Директ — кампании и бюджет | 3 |
| Директ — эффективность и ключи | 3 |
| Директ — разрезы статистики | 4 |
| Директ — объявления, группы, ставки | 3 |
| Директ — write-операции с минус-фразами | 1 (новый) |
| Директ — общие наборы минус-фраз (CRUD) | 4 (новые) |
| Директ — Wordstat v4 | 3 (новые) |
| Директ — гибкие отчёты | 2 (новые) |
| **Всего** | **30** |

### Архитектурные заметки

- **Write-операции**: первые в проекте. Паттерн read-modify-write для кампаний —
  сначала получаем `Type` и текущий `NegativeKeywords.Items` через `campaigns.get`
  со всеми `*FieldNames: [NegativeKeywords]`, затем шлём `campaigns.update` с
  корректным PascalCase-ключом подтипа.
- **Wordstat — отдельная legacy-подсистема**: другой домен (`.yandex.ru`, не `.com`),
  другая версия API (v4, не v5), другой транспорт ошибок (JSON-поля в 200-ответе,
  а не HTTP-коды). Реализовано поверх того же `httpx.AsyncClient`, чтобы не плодить
  пулы соединений.
- **Parser utilities**: во всех новых файлах общий паттерн разбора входных строк
  через `re.split(r"[,;]\s*", ...)` с case-insensitive дедупликацией. Поддерживаются
  и запятые, и точки с запятой.

---

## [1.2.0] — 2026-04-17

### Добавлено

#### Яндекс.Директ — 7 новых инструментов

**`tools/direct_reports.py`** — разрезы статистики (Reports API):
- `get_direct_stats_by_day` — динамика по дням: клики, показы, расход, CTR, CPC, конверсии
- `get_direct_stats_by_region` — топ-N регионов/городов по расходу с CTR и CPC
- `get_direct_stats_by_device` — разбивка по устройствам: desktop/mobile/tablet (с русскими метками)
- `get_direct_stats_by_placement` — поиск vs РСЯ: сравнение клики/расход/конверсии/стоимость конверсии

**`tools/direct_ads.py`** — объявления, группы, ставки (Campaigns API):
- `get_direct_ads` — список объявлений: заголовки, текст, ссылка, статус, мобильность. Требует хотя бы один фильтр (campaign_ids / adgroup_ids / ad_ids)
- `get_direct_adgroups` — список групп объявлений: название, статус, serving_status, регионы. Требует campaign_ids или adgroup_ids
- `get_direct_bids` — текущие ставки: Bid (поиск), ContextBid (сеть), приоритет, данные аукциона (MinBid, RecommendedBid) в рублях

**`direct_client.py`** — три новых метода:
- `get_ads(campaign_ids, adgroup_ids, ad_ids, statuses, client_login)` — POST /v501/ads
- `get_adgroups(campaign_ids, adgroup_ids, statuses, client_login)` — POST /v501/adgroups
- `get_bids(campaign_ids, adgroup_ids, keyword_ids, client_login)` — POST /v501/bids
- Добавлены URL-константы: `_ADS_URL`, `_ADGROUPS_URL`, `_BIDS_URL`

**`server.py`** — добавлены импорты `tools.direct_reports` и `tools.direct_ads`

### Итого инструментов

| Категория | Инструменты |
|-----------|-------------|
| Метрика | 7 |
| Директ — кампании и бюджет | 3 |
| Директ — эффективность и ключи | 2 |
| Директ — разрезы статистики | 4 (новые) |
| Директ — объявления, группы, ставки | 3 (новые) |
| **Всего** | **19** |

---

## [1.1.0] — 2026-04-13 ⚠️ DIRECT TOKENS PENDING

### Добавлено

#### Яндекс.Директ — новый клиент и 5 инструментов

- **`direct_client.py`** — асинхронный клиент Yandex Direct API v5:
  - Auth: `Authorization: Bearer {token}` (отличается от Metrica, где используется `OAuth`)
  - `Client-Login` передаётся как HTTP-заголовок (не в payload) для агентских аккаунтов
  - **Polling-модель** для Reports API: 201 (в очереди) / 202 (обрабатывается) → sleep(`retryIn`) → 200 (готово)
  - Динамический интервал polling из заголовка `retryIn` (не хардкод)
  - **XML/text парсинг 400-ошибок**: Reports API возвращает ошибки в XML, не JSON — извлекаем `<error_detail>` через regex
  - **Memory-safe TSV парсинг**: `_parse_tsv(text, top_n)` с early-exit — не загружает 100k+ строк в память
  - Отслеживание баллов API через заголовок `Units: spent/available/daily`
  - Предупреждение `_units_warning` при остатке < 10% от дневного лимита
  - Повторные попытки при 5xx (3 попытки, экспоненциальный backoff)

- **`tools/direct_campaigns.py`** — три MCP-инструмента:
  - `get_direct_campaigns` — список кампаний: ID, название, статус (рус.), дневной бюджет (руб.)
  - `get_direct_top_campaigns` — топ-N кампаний по расходу или кликам за период
  - `get_direct_budget` — остаток баллов API + сводка дневных бюджетов активных кампаний

- **`tools/direct_stats.py`** — два MCP-инструмента:
  - `get_direct_performance` — сводка эффективности: клики, показы, расход, CTR, CPC, конверсии, ROI
  - `get_direct_keywords` — топ ключевых фраз по расходу или кликам

- **Несколько аккаунтов Директа**: переменная `YANDEX_DIRECT_ACCOUNTS=alias:token,alias2:token2`
  - `resolve_direct_client(account, lc)` в `app.py` — резолвинг по псевдониму
  - Параметр `account: Optional[str]` во всех Direct-инструментах
  - Первый аккаунт в списке — основной (primary)

- **`format_metrics(row)`** — хелпер в каждом модуле Direct-инструментов:
  - Денежные поля (`Cost`, `CostPerConversion`, `Revenue`, `AvgCpc`) ÷ 1,000,000 → рубли
  - Процентные поля (`Ctr`, `ConversionRate`, `GoalsRoi`) округляются до 2 знаков
  - Предотвращает передачу LLM длинных float-хвостов

### Изменено

- **`app.py`**:
  - Lifespan расширен: открывает один или несколько `DirectClient` через `AsyncExitStack`
  - Добавлены хелперы `_parse_direct_accounts()` и `resolve_direct_client()`
  - Сервер переименован с `"Яндекс.Метрика"` → `"Яндекс.Метрика + Директ"`
  - Lifespan context теперь содержит `direct_client` (primary) и `direct_clients` (все)

- **`server.py`**:
  - Добавлены импорты `tools.direct_campaigns` и `tools.direct_stats`

- **`.env.example`**:
  - Добавлены секции для `YANDEX_DIRECT_ACCOUNTS`, `YANDEX_DIRECT_TOKEN`, `YANDEX_DIRECT_CLIENT_LOGIN`
  - Добавлены комментарии для `YANDEX_METRICA_COUNTERS`

### Статус конфигурации

> ⚠️ **Direct-токены не настроены.** Оба аккаунта (`promreo`, `site2`) сейчас используют
> токен Яндекс.Метрики как временный placeholder. Direct-инструменты будут возвращать
> ошибку 401 до замены токенов. См. файл [`DIRECT_TOKENS_TODO.md`](./DIRECT_TOKENS_TODO.md).

### Конфигурация

Новые переменные окружения:

| Переменная | Обязательно | Описание |
|------------|-------------|----------|
| `YANDEX_DIRECT_ACCOUNTS` | Нет | Несколько аккаунтов: `alias:token,alias2:token2` |
| `YANDEX_DIRECT_TOKEN` | Нет | Один аккаунт (упрощённый вариант) |
| `YANDEX_DIRECT_CLIENT_LOGIN` | Нет | Логин клиента для агентских аккаунтов |

Если ни одна из Direct-переменных не задана — используется `YANDEX_METRICA_TOKEN` как fallback.

---

## [1.0.0] — 2026-04-13

### Добавлено

- **`metrica_client.py`** — асинхронный клиент Yandex Metrica Reporting API:
  - Auth: `Authorization: OAuth {token}`
  - Повторные попытки при 429/5xx (3 попытки, экспоненциальный backoff, уважает `Retry-After`)
  - Определение семплирования (`containsSampledData`) → поле `_sampling_warning`
  - Валидация дат: YYYY-MM-DD, today, yesterday, NdaysAgo
  - Методы: `get_data`, `get_bytime`, `get_comparison`, `get_goals_list`

- **7 MCP-инструментов Метрики**:
  - `get_traffic_summary` — сводка трафика за период
  - `get_traffic_sources` — разбивка по источникам/каналам
  - `get_top_pages` — топ страниц по просмотрам
  - `get_goals` — цели и конверсии (двухфазный запрос: Management API + Reporting API)
  - `get_audience` — аудитория по устройствам, городам, регионам, браузерам
  - `get_realtime` — активность сайта сегодня по часам (bytime API)
  - `compare_periods` — сравнение метрики между двумя периодами

- **Несколько счётчиков Метрики**: `YANDEX_METRICA_COUNTERS=alias:id,alias2:id2`
  - `resolve_counter(counter, lc)` в `app.py`
  - Параметр `counter_id: Optional[str]` во всех инструментах Метрики

- **`app.py`** — FastMCP + lifespan с поддержкой множественных клиентов

- **`server.py`** — точка входа: статическая регистрация всех инструментов, stdio транспорт

- **`README.md`** — инструкция на русском: установка, получение OAuth-токена, конфиг Claude Desktop
