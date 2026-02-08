# Auto Search MVP

Semantic search engine for car listings with real data sources.

Проект предназначен для поиска автомобилей по смыслу запроса
(марка, пробег, цена, топливо и т.д.), а не по жёстким фильтрам.

---

## 🚀 Стек

### Backend
- Python 3.11
- FastAPI
- SQLAlchemy
- PostgreSQL
- Qdrant (vector database)
- Telethon (Telegram ingestion)

### Frontend
- Next.js (App Router)
- TypeScript

### Infra
- Docker
- Docker Compose

---

## 🗂 Архитектура (упрощённо)

Telegram / Mock / Playwright
↓
RawDocument (Postgres)
↓
NormalizedDocument
↓
DocumentChunk
↓
Qdrant (vectors)
↓
SearchService (ranking)
↓
API (/api/v1/search)
↓
Next.js UI


---

## ⚙️ Запуск локально

### 1️⃣ Клонирование репозитория

git clone https://github.com/PonchoGAD/auto-search-mvp.git
cd auto-search-mvp

2️⃣ ENV переменные

Создай файл .env в корне проекта:

# =========================
# DATABASE
# =========================
DATABASE_URL=postgresql+psycopg2://auto:auto133@postgres:5432/auto_search

# =========================
# TELEGRAM
# =========================
TG_API_ID=123456
TG_API_HASH=PASTE_API_HASH_HERE
TG_SESSION_STRING=PASTE_SESSION_STRING_HERE

# Список Telegram-каналов
TG_CHANNELS=@cars_ru

# Сколько сообщений забирать с канала
TG_FETCH_LIMIT=50

# =========================
# API
# =========================
API_BASE_URL=http://localhost:8000


⚠️ Важно

TG_SESSION_STRING генерируется заранее через get_session.py

Использовать реальные Telegram-каналы

Если канал не содержит авто-объявлений  данных может быть 0 (это нормально)

3️⃣ Запуск Docker
docker compose -f infra/compose.yml up --build -d


Проверка:

docker compose -f infra/compose.yml ps


Все контейнеры должны быть Up.

🔁 Прогон data pipeline (ОБЯЗАТЕЛЬНО)
Ingest (Telegram / mock)
docker compose -f infra/compose.yml exec api \
python -c "from data_pipeline.ingest import run_ingest; run_ingest()"

Normalize
docker compose -f infra/compose.yml exec api \
python -c "from data_pipeline.normalize import run_normalize; run_normalize()"

Chunk
docker compose -f infra/compose.yml exec api \
python -c "from data_pipeline.chunk import run_chunk; run_chunk()"

Index (Qdrant)
docker compose -f infra/compose.yml exec api \
python -c "from data_pipeline.index import run_index; run_index()"


Ожидаемо:

Telegram может вернуть 0, если нет релевантных постов

Qdrant не очищается, данные накапливаются

🔎 Поиск (API)
Endpoint
POST http://localhost:8000/api/v1/search

Пример запроса
{
  "query": "BMW до 50 000 км, бензин"
}

Пример ответа
{
  "structuredQuery": {...},
  "results": [...],
  "sources": [...],
  "debug": {
    "latency_ms": 32,
    "vector_hits": 12,
    "final_results": 10,
    "empty_result": false
  }
}

🖥 UI

Открыть в браузере:

http://localhost:3000


Функционал:

строка поиска

карточки результатов

пустое состояние без ошибок

debug-панель (structuredQuery, latency)

🧠 Примечания

Ranking основан на:

совпадении бренда

whitelist марок

цене / пробеге

Архитектура расширяема:

Playwright (auto.ru / drom.ru)

Retention и аналитика

SaaS-модель


---

