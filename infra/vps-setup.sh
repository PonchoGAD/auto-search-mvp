#!/usr/bin/env bash
# =============================================================================
# vps-setup.sh — ПЕРВОНАЧАЛЬНАЯ УСТАНОВКА на чистый VPS
#
# Запуск: bash vps-setup.sh
#
# Что делает:
#   1. Устанавливает Docker + Docker Compose
#   2. Клонирует оба репозитория
#   3. Создаёт shared Docker network
#   4. Запускает Search Core
#   5. Запускает Telegram Bot
# =============================================================================

set -euo pipefail

VPS_IP="65.21.159.255"
DEPLOY_DIR="/opt"
SEARCH_REPO_DIR="$DEPLOY_DIR/auto-search-mvp"
BOT_REPO_DIR="$DEPLOY_DIR/auto-search-tg-bot"
SHARED_NETWORK="auto-search-shared"

RED='\033[0;31m'; GREEN='\033[0;32m'; YELLOW='\033[1;33m'; NC='\033[0m'
info()  { echo -e "${GREEN}[SETUP]${NC} $*"; }
warn()  { echo -e "${YELLOW}[WARN]${NC} $*"; }
error() { echo -e "${RED}[ERROR]${NC} $*"; exit 1; }

# ─── 1. Docker ──────────────────────────────────────────────────────────────
install_docker() {
    if command -v docker >/dev/null 2>&1; then
        info "Docker уже установлен: $(docker --version)"
        return
    fi

    info "Устанавливаю Docker..."
    apt-get update -qq
    apt-get install -y -qq ca-certificates curl gnupg lsb-release

    install -m 0755 -d /etc/apt/keyrings
    curl -fsSL https://download.docker.com/linux/ubuntu/gpg \
        | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
    chmod a+r /etc/apt/keyrings/docker.gpg

    echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] \
https://download.docker.com/linux/ubuntu $(lsb_release -cs) stable" \
        > /etc/apt/sources.list.d/docker.list

    apt-get update -qq
    apt-get install -y -qq docker-ce docker-ce-cli containerd.io docker-compose-plugin

    systemctl enable docker
    systemctl start docker
    info "Docker установлен: $(docker --version)"
}

# ─── 2. Репозитории ─────────────────────────────────────────────────────────
setup_repos() {
    info "Настраиваю репозитории..."

    # Если git не установлен
    command -v git >/dev/null 2>&1 || apt-get install -y -qq git

    # Search Core
    if [[ -d "$SEARCH_REPO_DIR" ]]; then
        info "Search Core уже есть — делаю git pull"
        cd "$SEARCH_REPO_DIR" && git pull --ff-only 2>/dev/null || true
    else
        warn "ВНИМАНИЕ: Папка $SEARCH_REPO_DIR не найдена"
        warn "Загрузи репозиторий вручную:"
        warn "  scp -r ./auto-search-mvp root@$VPS_IP:/opt/"
        warn "Или клонируй: git clone <repo-url> $SEARCH_REPO_DIR"
    fi

    # Bot
    if [[ -d "$BOT_REPO_DIR/auto-search-tg-bot" ]]; then
        info "TG Bot уже есть — делаю git pull"
        cd "$BOT_REPO_DIR/auto-search-tg-bot" && git pull --ff-only 2>/dev/null || true
    else
        warn "ВНИМАНИЕ: Папка $BOT_REPO_DIR не найдена"
        warn "Загрузи репозиторий вручную:"
        warn "  scp -r ./auto-search-tg-bot root@$VPS_IP:/opt/"
    fi
}

# ─── 3. Shared Network ──────────────────────────────────────────────────────
setup_network() {
    if docker network inspect "$SHARED_NETWORK" >/dev/null 2>&1; then
        info "Network '$SHARED_NETWORK' уже существует"
    else
        info "Создаю Docker network: $SHARED_NETWORK"
        docker network create "$SHARED_NETWORK"
    fi
}

# ─── 4. Search Core ─────────────────────────────────────────────────────────
start_search_core() {
    local compose_file="$SEARCH_REPO_DIR/infra/docker/docker-compose.prod.yml"
    local env_file="$SEARCH_REPO_DIR/infra/docker/.env.prod"

    [[ -f "$compose_file" ]] || error "Не найден: $compose_file"
    [[ -f "$env_file" ]]     || error "Не найден: $env_file"

    info "Запускаю Search Core..."
    cd "$SEARCH_REPO_DIR"

    docker compose -f "$compose_file" --env-file "$env_file" \
        up -d --build --remove-orphans

    info "Ожидаю health check API (загрузка моделей ~2 мин)..."
    local attempt=0
    while [[ $attempt -lt 60 ]]; do
        if curl -sf "http://localhost:8000/api/v1/health" >/dev/null 2>&1; then
            info "Search Core API — готов!"
            break
        fi
        attempt=$((attempt+1))
        printf "."
        sleep 3
    done
    [[ $attempt -lt 60 ]] || warn "API не ответил за 3 мин — проверь: docker logs auto-search-api"
}

# ─── 5. Telegram Bot ────────────────────────────────────────────────────────
start_tg_bot() {
    local compose_file="$BOT_REPO_DIR/auto-search-tg-bot/infra/docker-compose.prod.yml"

    [[ -f "$compose_file" ]] || error "Не найден: $compose_file"

    info "Запускаю Telegram Bot..."
    cd "$BOT_REPO_DIR/auto-search-tg-bot/infra"

    docker compose -f docker-compose.prod.yml \
        up -d --build --remove-orphans

    info "Ожидаю миграций и старта bot-api (30 сек)..."
    sleep 30

    if docker inspect auto-search-bot-api \
        --format "{{.State.Status}}" 2>/dev/null | grep -q "running"; then
        info "Telegram Bot — запущен!"
    else
        warn "bot-api не поднялся. Проверь: docker logs auto-search-bot-api"
    fi
}

# ─── 6. Итоговая проверка ───────────────────────────────────────────────────
final_check() {
    echo ""
    info "═══════════════════════════════════════════"
    info "       СТАТУС СИСТЕМЫ"
    info "═══════════════════════════════════════════"
    docker ps --filter "name=auto-search" \
        --format "table {{.Names}}\t{{.Status}}" 2>&1 || true

    echo ""
    info "Health endpoints:"
    echo -n "  Search Core: " && curl -sf "http://localhost:8000/api/v1/health" \
        && echo " ✓ OK" || echo " ✗ FAIL"
    echo -n "  Bot API:     " && curl -sf "http://localhost:8100/api/v1/health" \
        && echo " ✓ OK" || echo " ✗ FAIL"
    echo -n "  Nginx:       " && curl -sf "http://localhost:80/api/v1/health" \
        && echo " ✓ OK" || echo " ✗ FAIL"

    echo ""
    info "═══════════════════════════════════════════"
    info "  VPS IP:      $VPS_IP"
    info "  Search API:  http://$VPS_IP/api/v1/health"
    info "  Search API:  http://$VPS_IP/api/v1/ready"
    info "═══════════════════════════════════════════"
}

# ─── Main ───────────────────────────────────────────────────────────────────
main() {
    info "=== Начинаю установку на VPS $VPS_IP ==="
    install_docker
    setup_repos
    setup_network
    start_search_core
    start_tg_bot
    final_check
    info "=== Установка завершена ==="
}

main "$@"
