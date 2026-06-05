#!/usr/bin/env bash
# =============================================================================
# deploy.sh — единый скрипт запуска всей системы на VPS
#
# Использование:
#   chmod +x deploy.sh
#   ./deploy.sh                          # полный деплой
#   ./deploy.sh search                   # только search core
#   ./deploy.sh bot                      # только tg-bot (search core уже запущен)
#   ./deploy.sh restart-bot              # перезапуск бота без пересборки
#   ./deploy.sh rebuild-bot              # пересборка + рестарт бота (фикс SyntaxError)
#   ./deploy.sh logs                     # логи всех контейнеров
#   ./deploy.sh logs-search              # логи только search core
#   ./deploy.sh logs-bot                 # логи только tg-bot
#   ./deploy.sh status                   # статус контейнеров
#   ./deploy.sh health                   # проверка health endpoints
#   ./deploy.sh test-search              # тестовый поиск
#   ./deploy.sh stop                     # остановить всё
#
# Требования:
#   - docker >= 24
#   - docker compose v2 (compose plugin)
#   - curl
#   - Оба репозитория на сервере:
#       /opt/auto-search-mvp/
#       /opt/auto-search-tg-bot/auto-search-tg-bot/
# =============================================================================

set -euo pipefail

# ─── Пути ───────────────────────────────────────────────────────────────────
SEARCH_DIR="$(cd "$(dirname "$0")/.." && pwd)"
BOT_DIR="$(dirname "$SEARCH_DIR")/auto-search-tg-bot/auto-search-tg-bot"

SEARCH_COMPOSE="$SEARCH_DIR/infra/docker/docker-compose.prod.yml"
SEARCH_ENV="$SEARCH_DIR/infra/docker/.env.prod"
BOT_COMPOSE="$BOT_DIR/infra/docker-compose.prod.yml"

SHARED_NETWORK="auto-search-shared"
API_KEY="***API_KEY_REDACTED***"

# ─── Цвета ──────────────────────────────────────────────────────────────────
RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()    { echo -e "${GREEN}[INFO]${NC} $*"; }
warn()    { echo -e "${YELLOW}[WARN]${NC} $*"; }
error()   { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ─── Проверки ────────────────────────────────────────────────────────────────
check_deps() {
    command -v docker  >/dev/null 2>&1 || error "docker не найден"
    docker compose version >/dev/null 2>&1 || error "docker compose plugin не найден"
    command -v curl    >/dev/null 2>&1 || warn "curl не найден — health-check будет пропущен"
    [[ -f "$SEARCH_COMPOSE" ]] || error "Не найден: $SEARCH_COMPOSE"
    [[ -f "$SEARCH_ENV" ]]     || error ".env.prod не найден: $SEARCH_ENV"
    info "Зависимости Search Core: OK"
}

check_deps_all() {
    check_deps
    [[ -f "$BOT_COMPOSE" ]] || error "Не найден: $BOT_COMPOSE — загрузи tg-bot в /opt/auto-search-tg-bot/"
    info "Зависимости Bot: OK"
}

# ─── Создание shared network (ВАЖНО: до запуска compose) ─────────────────────
ensure_shared_network() {
    if ! docker network inspect "$SHARED_NETWORK" >/dev/null 2>&1; then
        info "Создаю Docker network: $SHARED_NETWORK"
        docker network create "$SHARED_NETWORK"
    else
        info "Docker network '$SHARED_NETWORK': уже существует"
    fi
}

# ─── Ожидание health ─────────────────────────────────────────────────────────
wait_for_health() {
    local url="$1"
    local name="$2"
    local max_attempts="${3:-40}"
    local attempt=0

    info "Ожидаю готовности $name..."
    while [[ $attempt -lt $max_attempts ]]; do
        if curl -sf "$url" >/dev/null 2>&1; then
            info "$name — готов!"
            return 0
        fi
        attempt=$((attempt + 1))
        printf "."
        sleep 3
    done
    echo ""
    error "$name не ответил за $((max_attempts * 3)) секунд. Проверьте логи: ./deploy.sh logs-search"
}

# ─── Запуск Search Core ───────────────────────────────────────────────────────
start_search() {
    info "=== Запускаю Search Core ==="
    docker compose -f "$SEARCH_COMPOSE" --env-file "$SEARCH_ENV" up -d --build
    info "Ждём пока API пройдёт health check (до 2 мин для загрузки моделей)..."
    wait_for_health "http://localhost:8000/api/v1/health" "Search Core API" 50
}

# ─── Запуск Telegram Bot ──────────────────────────────────────────────────────
start_bot() {
    info "=== Запускаю Telegram Bot ==="
    # --build обязателен: без него может использоваться старый образ со SyntaxError
    docker compose -f "$BOT_COMPOSE" up -d --build
    info "Ожидаю запуска bot-api (30 сек)..."
    sleep 15
    if docker inspect auto-search-bot-api --format "{{.State.Health.Status}}" 2>/dev/null | grep -q "healthy"; then
        info "Telegram Bot — запущен успешно"
    else
        warn "bot-api ещё стартует. Проверьте: ./deploy.sh logs-bot"
    fi
}

# ─── Пересборка бота (фикс после обновления кода) ────────────────────────────
rebuild_bot() {
    info "=== Пересборка Telegram Bot (принудительная) ==="
    docker compose -f "$BOT_COMPOSE" down --remove-orphans || true
    docker compose -f "$BOT_COMPOSE" build --no-cache
    docker compose -f "$BOT_COMPOSE" up -d
    info "Готово. Логи: ./deploy.sh logs-bot"
}

# ─── Перезапуск бота без пересборки ──────────────────────────────────────────
restart_bot() {
    info "=== Перезапуск Telegram Bot (без rebuild) ==="
    docker compose -f "$BOT_COMPOSE" restart tg-bot worker
    info "Готово"
}

# ─── Health check ─────────────────────────────────────────────────────────────
check_health() {
    info "=== Health Check ==="
    echo ""
    echo -n "Search Core API (/health): "
    curl -sf "http://localhost:8000/api/v1/health" && echo " OK" || echo " FAIL"

    echo -n "Search Core API (/ready):  "
    curl -sf "http://localhost:8000/api/v1/ready" && echo " OK" || echo " FAIL"

    echo -n "Bot API (/health):         "
    curl -sf "http://localhost:8100/api/v1/health" && echo " OK" || echo " FAIL"
    echo ""
}

# ─── Тестовый поиск ───────────────────────────────────────────────────────────
test_search() {
    info "=== Тестовый поиск через Search Core ==="
    curl -s -X POST "http://localhost:8000/api/v1/search" \
        -H "Content-Type: application/json" \
        -H "X-API-Key: $API_KEY" \
        -d '{"query": "BMW до 3 млн пробег 50 тыс бензин", "page": 1, "limit": 3}' \
        | python3 -c "
import json, sys
data = json.load(sys.stdin)
results = data.get('results', [])
print(f'Найдено: {len(results)} результатов')
print(f'Empty result: {data.get(\"debug\", {}).get(\"empty_result\", \"?\")}')
print(f'Latency: {data.get(\"debug\", {}).get(\"latency_ms\", \"?\")} ms')
for r in results[:3]:
    print(f'  - {r.get(\"brand\",\"?\")} {r.get(\"model\",\"?\")} {r.get(\"year\",\"?\")} | {r.get(\"price\",\"?\")} руб | {r.get(\"source_name\",\"?\")}')
" 2>&1 || echo "Search Core недоступен или вернул ошибку"

    echo ""
    info "=== Тестовый поиск через Bot API ==="
    curl -s -X POST "http://localhost:8100/api/v1/search-proxy/search" \
        -H "Content-Type: application/json" \
        -H "X-INTERNAL-KEY: ***INTERNAL_API_KEY_REDACTED***" \
        -d '{"raw_query": "BMW до 3 млн", "page": 1, "limit": 3}' \
        | python3 -c "
import json, sys
data = json.load(sys.stdin)
results = data.get('results', [])
print(f'Bot API → Search Core: {len(results)} результатов')
" 2>&1 || echo "Bot API недоступен"
}

# ─── Логи ────────────────────────────────────────────────────────────────────
show_logs() {
    info "=== Логи Search Core (последние 50 строк) ==="
    docker compose -f "$SEARCH_COMPOSE" --env-file "$SEARCH_ENV" logs --tail=50 2>&1 || true
    echo ""
    info "=== Логи Telegram Bot (последние 50 строк) ==="
    docker compose -f "$BOT_COMPOSE" logs --tail=50 2>&1 || true
}

show_logs_search() {
    docker compose -f "$SEARCH_COMPOSE" --env-file "$SEARCH_ENV" logs -f --tail=100
}

show_logs_bot() {
    docker compose -f "$BOT_COMPOSE" logs -f --tail=100
}

# ─── Статус ──────────────────────────────────────────────────────────────────
show_status() {
    info "=== Search Core контейнеры ==="
    docker compose -f "$SEARCH_COMPOSE" --env-file "$SEARCH_ENV" ps 2>&1 || true
    echo ""
    info "=== Telegram Bot контейнеры ==="
    docker compose -f "$BOT_COMPOSE" ps 2>&1 || true
    echo ""
    info "=== Docker Networks ==="
    docker network ls | grep auto-search || echo "  (сети не найдены)"
    echo ""
    info "=== Все auto-search контейнеры ==="
    docker ps -a --filter "name=auto-search" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}" 2>&1 || true
}

# ─── Остановка всего ─────────────────────────────────────────────────────────
stop_all() {
    info "=== Остановка всего ==="
    docker compose -f "$BOT_COMPOSE" down --remove-orphans 2>/dev/null || true
    docker compose -f "$SEARCH_COMPOSE" --env-file "$SEARCH_ENV" down --remove-orphans 2>/dev/null || true
    info "Всё остановлено"
}

# ─── Полный деплой ───────────────────────────────────────────────────────────
deploy_all() {
    check_deps_all
    ensure_shared_network
    start_search
    start_bot
    echo ""
    check_health
    info "=========================================="
    info "  Деплой завершён!"
    info "  Search API:  http://$(hostname -I | awk '{print $1}')/api/v1/health"
    info "  Bot API:     http://localhost:8100/api/v1/health"
    info "  Тест:        ./deploy.sh test-search"
    info "  Логи:        ./deploy.sh logs"
    info "  Статус:      ./deploy.sh status"
    info "=========================================="
}

# ─── Main ────────────────────────────────────────────────────────────────────
CMD="${1:-all}"
case "$CMD" in
    all)           check_deps_all && ensure_shared_network && start_search && start_bot && check_health ;;
    search)        check_deps && ensure_shared_network && start_search ;;
    bot)           check_deps_all && ensure_shared_network && start_bot ;;
    restart-bot)   restart_bot ;;
    rebuild-bot)   rebuild_bot ;;
    health)        check_health ;;
    test-search)   test_search ;;
    logs)          show_logs ;;
    logs-search)   show_logs_search ;;
    logs-bot)      show_logs_bot ;;
    status)        show_status ;;
    stop)          stop_all ;;
    *)
        echo "Использование: $0 [all|search|bot|restart-bot|rebuild-bot|health|test-search|logs|logs-search|logs-bot|status|stop]"
        exit 1
        ;;
esac
