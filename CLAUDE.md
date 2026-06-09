# Auto Search MVP — CLAUDE.md

Mono-repo: семантический поисковик по объявлениям о продаже авто (FastAPI + Qdrant + Telegram ingest) + Telegram бот для пользователей (aiogram 3 + bot-api + worker).

---

## Architecture

```
Docker network: auto-search-shared (external, bridge)
│
├── [SEARCH CORE — docker-compose.prod.yml]
│   ├── nginx:80             public entry point
│   ├── api:8000             FastAPI — shared network
│   ├── ingest-worker        scrapes sources every 15 min
│   ├── postgres:5432        internal only
│   ├── redis:6379           internal only
│   └── qdrant:6333          internal only
│
└── [TG BOT — docker-compose.tgbot.yml]
    ├── bot-api:8100         FastAPI proxy (shared network → search core)
    ├── tg-bot               aiogram 3 polling
    ├── worker               saved search alerts
    └── bot-postgres:5432    internal only (separate DB)
```

Data flow:
- **Search:** Raw source → RawDocument (Postgres) → normalize → chunk → index (Qdrant) → Search API
- **Bot:** User → tg-bot → bot-api → Search API (X-API-Key) → response → format → user

---

## Project Structure

```
apps/
  api/              FastAPI search backend
  ingest-worker/    Scraper (8 sources, 15-min cycle)
  bot/              aiogram 3 Telegram bot
  bot_api/          FastAPI proxy + user/favorites/payments DB
  worker/           Alerts worker (saved searches)

infra/
  docker/
    docker-compose.prod.yml    Search core (api, ingest, pg, redis, qdrant, nginx)
    docker-compose.tgbot.yml   TG bot (bot-api, tg-bot, worker, bot-postgres)
    .env.prod                  Search core secrets (NOT committed)
    .env.tgbot                 TG bot secrets (NOT committed)
    .env.prod.example          Template for search core
    .env.tgbot.example         Template for tg-bot
    api.Dockerfile             Python 3.11, PyTorch CPU-only
    nginx.conf                 Reverse proxy for search core
  MASTER_PROMPT_TG_BOT.md    Context prompt for TG bot dev sessions
  deploy.sh
```

---

## Key Files — Search Core

| Path | Role |
|------|------|
| `apps/api/src/main.py` | FastAPI app, middleware, lifespan |
| `apps/api/src/core/settings.py` | Pydantic-settings — single config source |
| `apps/api/src/services/search_service.py` | embed → Qdrant → BM25 → reranker |
| `apps/api/src/services/query_parser.py` | Free-text → StructuredQuery |
| `apps/api/src/services/taxonomy_service.py` | Brand/model resolution |
| `apps/api/src/integrations/vector_db/qdrant.py` | Qdrant client wrapper |
| `apps/api/src/api/v1/search.py` | Search endpoint |
| `apps/api/src/api/v1/listings.py` | Listing detail endpoint |
| `apps/api/src/api/v1/health.py` | `/health` and `/ready` |
| `apps/api/src/config/brands.yaml` | Brand taxonomy |
| `apps/api/src/config/models.yaml` | Model taxonomy |
| `apps/ingest-worker/main.py` | Long-running ingest loop |
| `infra/docker/docker-compose.prod.yml` | Search core orchestration |
| `infra/docker/nginx.conf` | nginx config (envsubst for API key) |
| `infra/docker/.env.prod` | Search core secrets |

## Key Files — TG Bot

| Path | Role |
|------|------|
| `apps/bot/src/main.py` | aiogram polling entry point |
| `apps/bot/src/handlers/search.py` | Text query → search → format results |
| `apps/bot/src/handlers/start.py` | /start, user registration |
| `apps/bot/src/handlers/favorites.py` | Favorites CRUD |
| `apps/bot/src/handlers/saved_searches.py` | Saved searches + alerts |
| `apps/bot/src/formatters/listing_card.py` | Card formatter for results |
| `apps/bot/src/config.py` | BOT_TOKEN, BOT_API_BASE_URL |
| `apps/bot_api/src/main.py` | FastAPI entry point |
| `apps/bot_api/src/api/search_proxy.py` | Proxy to search core |
| `apps/bot_api/src/clients/search_api.py` | HTTP client → auto-search-api:8000 |
| `apps/bot_api/src/common/result_mapper.py` | Maps search response to bot schema |
| `apps/bot_api/src/config.py` | All settings (Pydantic-settings) |
| `apps/bot_api/alembic/` | DB migrations (0001–0003) |
| `apps/worker/src/scheduler.py` | Job scheduler |
| `apps/worker/src/jobs/saved_search_alerts.py` | Alert job |
| `infra/docker/docker-compose.tgbot.yml` | TG bot orchestration |
| `infra/docker/.env.tgbot` | TG bot secrets |

---

## Development Commands

```bash
# Search core
docker compose -f infra/docker/docker-compose.prod.yml up -d --build
docker compose -f infra/docker/docker-compose.prod.yml logs -f

# TG Bot
docker compose -f infra/docker/docker-compose.tgbot.yml up -d --build
docker compose -f infra/docker/docker-compose.tgbot.yml logs -f

# Logs (individual containers)
docker logs auto-search-api -f --tail 100
docker logs auto-search-bot-api -f --tail 50
docker logs auto-search-tg-bot -f --tail 50
docker logs auto-search-ingest-worker -f --tail 50

# Test search
curl -s -X POST http://localhost/api/v1/search \
  -H "Content-Type: application/json" \
  -d '{"query":"toyota camry"}' | jq

# Generate secrets
python -c "import secrets; print(secrets.token_hex(32))"

# Check Qdrant points
docker exec auto-search-api python -c "
import sys; sys.path.insert(0, '/app/src')
from integrations.vector_db.qdrant import QdrantStore
from core.settings import settings
store = QdrantStore()
print('COUNT=', store.client.count(collection_name=settings.qdrant_collection, exact=True))
"
```

---

## Authentication Chain

```
Telegram → tg-bot (aiogram polling)
tg-bot → bot-api:  X-INTERNAL-KEY header
bot-api → api:     X-API-Key: <API_KEY from .env.prod>
worker → bot-api:  X-INTERNAL-KEY header
```

nginx injects `X-API-Key` automatically via `envsubst` at startup — no hardcoded keys in git.

---

## Fixed Bugs (2026-06-07 – 2026-06-09)

| Commit | Fix |
|--------|-----|
| `a7b4b7e` | Score normalization: weights sum to 1.0, fuel boost capped at 1.0 |
| `013958e` | Alembic deadlock: migrations moved to `alembic/versions/` |
| `4b0e4ba` | `python-jose` → `PyJWT` in `auth.py` |
| `123ff13` | Docker project isolation: `name:` added to both compose files |
| `10e0214` | Score capping: model boost `*=1.4` now uses `min(1.0, ...)` |
| `10e0214` | Favorites limit: `count_by_user()` enforced before INSERT (HTTP 429 if at limit) |
| `10e0214` | Listing card: null price/mileage fields hidden instead of showing "не указан" |

---

## Known Bugs (ordered by severity)

### SEARCH CORE

#### 1. Listings fallback full-scan (performance)
`apps/api/src/api/v1/listings.py:181` — fallback scans all points when filter yields no results.
`LISTINGS_FALLBACK_MAX_SCAN=500` is set in docker-compose.prod.yml; verify it's respected.

#### 2. CORS is fully open
`main.py` has `allow_origins=["*"]`. Restrict to bot-api origin in production.

#### 3. DB schema not auto-created in production
`Base.metadata.create_all` only runs when `DEBUG=True`. Workaround:
```bash
docker exec auto-search-api python -c "from db.session import engine, Base; Base.metadata.create_all(bind=engine)"
```

#### 4. `print()` instead of structured logging
`main.py` and `search_service.py` use `print()`. Should use `logging` with JSON formatter.

### TG BOT

#### 5. [MEDIUM] Rate limiting is in-memory (resets on restart)
Move to Redis (REDIS_URL is already in config).

#### 6. [LOW] No pagination for saved searches at >5 records

---

## Production Checklist

### Search Core
```
[x] docker network create --driver bridge auto-search-shared
[x] start_period: 120s in api healthcheck
[x] .env.prod filled with secrets
[x] API_KEY injected into nginx via envsubst (no key in git)
[x] QDRANT port NOT exposed externally
[ ] DB schema init (run Base.metadata.create_all once)
[ ] Qdrant backup: cron + snapshot API
```

### TG Bot (first deploy)
```
[x] Get BOT_TOKEN from @BotFather → @semantikauto_bot (deployed)
[x] Copy .env.tgbot.example → .env.tgbot on VPS, filled all values
[x] BOT_TOKEN + BOT_USERNAME (bot/config.py) AND TELEGRAM_BOT_TOKEN + TELEGRAM_BOT_USERNAME (bot_api/worker) — both sets required!
[x] SEARCH_API_KEY = API_KEY value from .env.prod
[x] INTERNAL_API_KEY ≥ 32 chars (same value in all 3 services)
[x] JWT_SECRET ≥ 32 chars
[x] auto-search-shared network exists (already created for search core)
[x] Alembic migrations in alembic/versions/ (fixed: was causing deadlock)
[x] auth.py uses PyJWT not python-jose (fixed: was ModuleNotFoundError)
[ ] Set ADMIN_TELEGRAM_IDS_RAW to your Telegram user ID
```

---

## Deployment Sequence (VPS — /opt/auto-search-mvp)

```bash
# 1. Create shared network (once, idempotent)
docker network create --driver bridge auto-search-shared || true

# 2. Deploy search core
cd /opt/auto-search-mvp
git pull origin main
docker compose -f infra/docker/docker-compose.prod.yml up -d --build

# 3. Wait for health
sleep 60
curl http://localhost/api/v1/health

# 4. Create .env.tgbot (if not exists) — fill from .env.tgbot.example
# cp infra/docker/.env.tgbot.example infra/docker/.env.tgbot
# nano infra/docker/.env.tgbot

# 5. Deploy tg-bot (same repo, different compose file)
docker compose -f infra/docker/docker-compose.tgbot.yml up -d --build

# 6. Verify all containers
docker ps --format 'table {{.Names}}\t{{.Status}}'
```

---

## Environment Variables

### Search Core (.env.prod)

| Variable | Required | Notes |
|----------|----------|-------|
| `DATABASE_URL` | yes | `postgresql+psycopg2://auto:<pass>@postgres:5432/auto_search` |
| `API_KEY` | yes | Key for nginx → api (injected via envsubst) |
| `POSTGRES_PASSWORD` | yes | |
| `REDIS_URL` | yes | `redis://redis:6379/0` |
| `OPENAI_API_KEY` | yes | Used for embeddings (`text-embedding-3-small`) |
| `QDRANT_URL` | no | defaults to `http://qdrant:6333` |
| `ENV` | yes | `production` |
| `ALLOW_DEV_SEED` | no | `1` to include dev_seed data in results |

### TG Bot (.env.tgbot)

| Variable | Required | Notes |
|----------|----------|-------|
| `BOT_TOKEN` | yes | From @BotFather — read by `bot/src/config.py` |
| `BOT_USERNAME` | yes | Username without @ — read by `bot/src/config.py` |
| `TELEGRAM_BOT_TOKEN` | yes | Same token — read by `bot_api/config.py` and `worker/config.py` |
| `TELEGRAM_BOT_USERNAME` | yes | Same username — read by `bot_api/config.py` |
| `SEARCH_API_KEY` | yes | = API_KEY from .env.prod |
| `INTERNAL_API_KEY` | yes | Shared secret bot↔bot-api↔worker (≥ 32 chars) |
| `JWT_SECRET` | yes | ≥ 32 chars |
| `DATABASE_URL` | yes | `postgresql+psycopg2://bot:<pass>@bot-postgres:5432/auto_search_bot` |
| `POSTGRES_PASSWORD` | yes | Bot postgres password |
| `ADMIN_TELEGRAM_IDS_RAW` | no | Comma-separated Telegram user IDs for admin access |
| `PAYMENT_PROVIDER` | no | `stub` (default) / `yookassa` / `stars` |
| `FREE_FAVORITES_LIMIT` | no | Default 50 — hard limit before HTTP 429 |
| `SCHEDULER_POLL_INTERVAL_SEC` | no | Default 300 (5 min) |
