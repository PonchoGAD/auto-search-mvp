# =============================================================================
# upload-to-vps.ps1 — Загрузка обоих проектов на новый VPS
# Запуск: .\infra\upload-to-vps.ps1
# Требует: OpenSSH (встроен в Windows 10/11)
# =============================================================================

$VPS_IP   = "65.21.159.255"
$VPS_USER = "root"
$VPS_KEY  = ""   # путь к SSH ключу, например: "C:\Users\gafit\.ssh\id_rsa"
                 # если пусто — будет спрошен пароль

$SEARCH_DIR = "C:\Users\gafit\auto-search-mvp"
$BOT_DIR    = "C:\Users\gafit\auto-search-tg-bot\auto-search-tg-bot"

# SSH аргументы
$SSH_ARGS = @("-o", "StrictHostKeyChecking=no", "-o", "ConnectTimeout=10")
if ($VPS_KEY -ne "") { $SSH_ARGS += @("-i", $VPS_KEY) }
$SCP_ARGS = $SSH_ARGS

function Run-SSH {
    param([string]$cmd)
    & ssh @SSH_ARGS "${VPS_USER}@${VPS_IP}" $cmd
}

Write-Host "[UPLOAD] Начинаю загрузку на VPS $VPS_IP" -ForegroundColor Green

# ─── Создать директории на VPS ──────────────────────────────────────────────
Write-Host "[1/5] Создаю директории на VPS..." -ForegroundColor Cyan
Run-SSH "mkdir -p /opt/auto-search-mvp /opt/auto-search-tg-bot/auto-search-tg-bot"

# ─── Загрузить Search Core (без node_modules, __pycache__, .venv) ────────────
Write-Host "[2/5] Загружаю auto-search-mvp..." -ForegroundColor Cyan
& rsync -avz --progress `
    --exclude="node_modules" `
    --exclude="__pycache__" `
    --exclude=".venv" `
    --exclude="*.pyc" `
    --exclude=".next" `
    --exclude=".git" `
    --exclude="qdrant_data" `
    --exclude="postgres_data" `
    -e "ssh $($SSH_ARGS -join ' ')" `
    "$SEARCH_DIR/" `
    "${VPS_USER}@${VPS_IP}:/opt/auto-search-mvp/"

if ($LASTEXITCODE -ne 0) {
    Write-Host "[FALLBACK] rsync не найден, использую scp..." -ForegroundColor Yellow
    & scp @SCP_ARGS -r "$SEARCH_DIR" "${VPS_USER}@${VPS_IP}:/opt/"
}

# ─── Загрузить TG Bot ────────────────────────────────────────────────────────
Write-Host "[3/5] Загружаю auto-search-tg-bot..." -ForegroundColor Cyan
& rsync -avz --progress `
    --exclude="__pycache__" `
    --exclude=".venv" `
    --exclude="*.pyc" `
    --exclude=".git" `
    -e "ssh $($SSH_ARGS -join ' ')" `
    "$BOT_DIR/" `
    "${VPS_USER}@${VPS_IP}:/opt/auto-search-tg-bot/auto-search-tg-bot/"

if ($LASTEXITCODE -ne 0) {
    & scp @SCP_ARGS -r "$BOT_DIR" "${VPS_USER}@${VPS_IP}:/opt/auto-search-tg-bot/"
}

# ─── Права на скрипты ───────────────────────────────────────────────────────
Write-Host "[4/5] Устанавливаю права..." -ForegroundColor Cyan
Run-SSH "chmod +x /opt/auto-search-mvp/infra/deploy.sh /opt/auto-search-mvp/infra/vps-setup.sh"

# ─── Запуск установки на VPS ────────────────────────────────────────────────
Write-Host "[5/5] Запускаю установку на VPS..." -ForegroundColor Cyan
Write-Host ""
Write-Host "Для запуска системы подключись по SSH и выполни:" -ForegroundColor Yellow
Write-Host "  ssh ${VPS_USER}@${VPS_IP}" -ForegroundColor White
Write-Host "  bash /opt/auto-search-mvp/infra/vps-setup.sh" -ForegroundColor White
Write-Host ""
Write-Host "[UPLOAD] Загрузка завершена!" -ForegroundColor Green
Write-Host "VPS: $VPS_IP" -ForegroundColor Green
