# МАСТЕР-ПРОМТ ДЛЯ ЧАТА TG BOT
# Вставь этот текст в новый чат при работе над C:\Users\gafit\auto-search-tg-bot\auto-search-tg-bot

---

Ты работаешь над Telegram ботом для поиска автомобилей. Рабочий каталог:
C:\Users\gafit\auto-search-tg-bot\auto-search-tg-bot

═══════════════════════════════════════════════════════════════════
КОНТЕКСТ СИСТЕМЫ
═══════════════════════════════════════════════════════════════════

Это ЧАСТЬ большой системы из двух проектов:

[Search Core] auto-search-mvp — FastAPI + Qdrant + PostgreSQL + Redis
  Container: auto-search-api
  Порт внутри Docker: 8000
  Nginx: порт 80 → http://65.21.159.255/api/v1/...
  API_KEY: ***API_KEY_REDACTED***

[TG Bot] auto-search-tg-bot — aiogram 3 + FastAPI bot-api + worker
  tg-bot:   polling Telegram
  bot-api:  порт 8100, прокси между ботом и search core
  worker:   алерты по сохранённым поискам

DOCKER СЕТЬ:
  auto-search-shared (external) — связывает search core и bot-api
  auto-search-api:8000 — так bot-api видит search core через Docker DNS

═══════════════════════════════════════════════════════════════════
СТРУКТУРА ПРОЕКТА
═══════════════════════════════════════════════════════════════════

apps/
  bot/src/
    handlers/
      search.py          — обработка текстовых запросов → поиск
      start.py           — /start, регистрация пользователя
      favorites.py       — /favorites, избранное
      saved_searches.py  — /saved, сохранённые поиски + алерты
      subscriptions.py   — /subscription, тарифы
      admin.py           — /admin (только для ADMIN_TELEGRAM_IDS)
      callbacks.py       — все inline кнопки
    formatters/
      listing_card.py    — форматирование карточки авто
    middlewares/
      throttling.py      — лимит запросов (1 сек между поисками)
      user_context.py    — upsert пользователя на каждый запрос
    config.py            — BOT_TOKEN, BOT_API_BASE_URL, ADMIN_IDS
    utils/internal_api.py — заголовок X-INTERNAL-KEY для bot-api

  bot_api/src/
    api/
      search_proxy.py    — POST /search-proxy/search → search core
      users.py           — POST /users/telegram/upsert
      favorites.py       — CRUD избранного
      saved_searches.py  — CRUD сохранённых поисков
      internal.py        — /internal/... только для worker
    clients/
      search_api.py      — HTTP клиент к http://auto-search-api:8000
    services/
      search_gateway.py  — оркестрация поиска
      usage_limits.py    — лимиты free пользователей
    db/models.py         — User, Favorite, SavedSearch, Subscription, Payment
    config.py            — SEARCH_API_BASE_URL, INTERNAL_API_KEY, JWT_SECRET

  worker/src/
    scheduler.py         — запуск задач каждые N секунд
    jobs/
      saved_search_alerts.py   — проверяет новые авто по сохр. поискам
      subscription_expiry.py   — отключает истёкшие подписки
    services/
      search_matcher.py        — запускает поиск в search core
      alert_dispatcher.py      — отправляет Telegram уведомления

infra/
  docker-compose.prod.yml    — prod конфиг (настроен на shared network)
  nginx.conf                 — unified nginx (search + bot-api)

═══════════════════════════════════════════════════════════════════
.ENV ФАЙЛЫ (актуальные значения)
═══════════════════════════════════════════════════════════════════

apps/bot/.env:
  BOT_TOKEN=***BOT_TOKEN_REDACTED***
  BOT_API_BASE_URL=http://bot-api:8100
  BOT_API_PREFIX=/api/v1
  INTERNAL_API_KEY=***INTERNAL_API_KEY_REDACTED***
  ADMIN_TELEGRAM_IDS_RAW=34456629
  SEARCH_API_BASE_URL=http://auto-search-api:8000

apps/bot_api/.env:
  SEARCH_API_BASE_URL=http://auto-search-api:8000   ← container name!
  SEARCH_API_PREFIX=/api/v1
  SEARCH_API_KEY=***API_KEY_REDACTED***
  INTERNAL_API_KEY=***INTERNAL_API_KEY_REDACTED***
  DATABASE_URL=postgresql+psycopg2://auto:auto133@postgres:5432/auto_search
  JWT_SECRET=7eb212cb6e19e8e9706bbbd6ee85a7512fc087c2b554522d5943a4b09a485876...

apps/worker/.env:
  BOT_API_BASE_URL=http://bot-api:8100
  SEARCH_API_BASE_URL=http://auto-search-api:8000
  INTERNAL_API_KEY=***INTERNAL_API_KEY_REDACTED***

═══════════════════════════════════════════════════════════════════
АУТЕНТИФИКАЦИЯ
═══════════════════════════════════════════════════════════════════

bot → bot-api:       заголовок X-INTERNAL-KEY (принимает оба варианта)
bot-api → search:    заголовок X-API-Key: <SEARCH_API_KEY>
worker → bot-api:    заголовок X-INTERNAL-KEY

═══════════════════════════════════════════════════════════════════
ФОРМАТ ОТВЕТА SEARCH CORE
═══════════════════════════════════════════════════════════════════

POST http://auto-search-api:8000/api/v1/search
Headers: X-API-Key: 5636418...
Body: {"query": "BMW до 3 млн", "page": 1, "limit": 20}

Response:
{
  "results": [
    {
      "listing_id": "abc123",
      "title": "BMW X5 2020",
      "brand": "bmw",
      "model": "x5",
      "year": 2020,
      "price": 2900000,
      "currency": "RUB",
      "mileage": 45000,
      "fuel": "petrol",
      "city": "москва",
      "region": "москва",
      "source_name": "drom",
      "source_url": "https://...",
      "photos": ["https://..."],
      "score": 0.87,
      "why_match": "BMW X5, бензин, 2.9 млн, 45 тыс км"
    }
  ],
  "structured_query": {"brand": "bmw", "model": "x5", "price_max": 3000000},
  "pagination": {"page": 1, "limit": 20, "total": 45, "has_more": true},
  "debug": {"latency_ms": 234, "empty_result": false}
}

GET http://auto-search-api:8000/api/v1/listings/{listing_id}
— детали конкретного объявления

═══════════════════════════════════════════════════════════════════
VPS ДЕПЛОЙ (новый сервер: 65.21.159.255)
═══════════════════════════════════════════════════════════════════

На VPS оба проекта лежат в:
  /opt/auto-search-mvp/
  /opt/auto-search-tg-bot/auto-search-tg-bot/

Команды для запуска tg-bot на VPS:
  ssh root@65.21.159.255
  cd /opt/auto-search-tg-bot/auto-search-tg-bot/infra
  docker compose -f docker-compose.prod.yml down --remove-orphans
  docker compose -f docker-compose.prod.yml up -d --build

Проверка:
  docker logs auto-search-tg-bot --tail 30
  docker logs auto-search-bot-api --tail 30
  curl http://localhost:8100/api/v1/health
  docker exec auto-search-bot-api python3 -c \
    "import httpx; r=httpx.get('http://auto-search-api:8000/api/v1/health',timeout=5); print(r.status_code)"

═══════════════════════════════════════════════════════════════════
ИЗВЕСТНЫЕ БАГИ (приоритет для исправления)
═══════════════════════════════════════════════════════════════════

1. [ВЫСОКИЙ] Callback ID при пагинации ломается
   Файл: apps/bot/src/handlers/search.py
   Проблема: при отсутствии listing_id использует "item-{index}" — меняется при смене страницы
   Решение: всегда использовать listing_id из search core, добавить fallback на hash(source_url)

2. [СРЕДНИЙ] Лимит избранного не проверяется до INSERT
   Файл: apps/bot_api/src/repositories/favorites.py
   Решение: COUNT перед INSERT и return 403 если превышено

3. [СРЕДНИЙ] Rate limiting в памяти (сбрасывается при рестарте)
   Файл: apps/bot_api/src/main.py
   Решение: перенести в Redis

4. [НИЗКИЙ] Нет пагинации сохранённых поисков при >10 записей
   Файл: apps/bot/src/handlers/saved_searches.py

═══════════════════════════════════════════════════════════════════
КОМАНДЫ ДЛЯ РАЗРАБОТКИ ЛОКАЛЬНО
═══════════════════════════════════════════════════════════════════

# Запустить только бот (search core уже на VPS)
# В apps/bot_api/.env временно поменяй:
# SEARCH_API_BASE_URL=http://65.21.159.255

# Запустить bot-api локально
cd apps/bot_api
pip install -r requirements.txt
uvicorn src.main:app --host 0.0.0.0 --port 8100 --reload

# Запустить бот
cd apps/bot
pip install -r requirements.txt
python -m src.main

═══════════════════════════════════════════════════════════════════
ЗАДАЧИ ДЛЯ ЭТОЙ СЕССИИ (укажи конкретную)
═══════════════════════════════════════════════════════════════════

[Опиши что нужно сделать — бот будет знать весь контекст системы]
