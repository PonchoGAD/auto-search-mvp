from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from contextlib import asynccontextmanager
import os
import time
import uuid

from db.session import engine, Base

from api.v1.health import router as health_router
from api.v1.search import router as search_router
from api.v1.search_history import router as search_history_router
from api.v1.analytics import router as analytics_router
from api.v1.admin import router as admin_router
from api.v1.metrics import router as metrics_router
from api.v1.listings import router as listings_router

from core.settings import settings


@asynccontextmanager
async def lifespan(app: FastAPI):
    print("[STARTUP] pre-loading reranker model...", flush=True)
    try:
        from services.search_service import get_reranker
        get_reranker()
        print("[STARTUP] reranker ready", flush=True)
    except Exception as e:
        print(f"[STARTUP][WARN] reranker pre-load failed: {e}", flush=True)
    yield


app = FastAPI(
    title="Auto Semantic Search MVP",
    version="0.1.0",
    debug=settings.DEBUG,
    lifespan=lifespan,
)

# Разрешаем CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

API_KEY = os.getenv("API_KEY")


@app.middleware("http")
async def request_id_and_logging_middleware(request: Request, call_next):
    """Генерирует Request ID, считает время выполнения и логирует метаданные."""
    request_id = request.headers.get("X-Request-ID") or str(uuid.uuid4())
    request.state.request_id = request_id
    
    start_time = time.perf_counter()
    response = await call_next(request)
    duration_ms = int((time.perf_counter() - start_time) * 1000)
    
    # Безопасное логирование
    print(
        f"[HTTP] method={request.method} path={request.url.path} "
        f"status={response.status_code} duration={duration_ms}ms "
        f"request_id={request_id}",
        flush=True
    )
    
    response.headers["X-Request-ID"] = request_id
    return response


@app.middleware("http")
async def check_api_key(request: Request, call_next):
    # Разрешаем health и readiness без ключа (включая любые их подпути)
    path = request.url.path
    if path.startswith("/api/v1/health") or \
       path.startswith("/api/v1/ready") or \
       path == "/ready" or \
       path == "/health":
        return await call_next(request)

    # Защищаем остальное API
    if path.startswith("/api/"):
        if API_KEY and request.headers.get("X-API-Key") != API_KEY:
            return JSONResponse(
                status_code=403,
                content={"detail": "Forbidden"}
            )

    return await call_next(request)


app.include_router(health_router, prefix="/api/v1")
app.include_router(search_router, prefix="/api/v1")
app.include_router(search_history_router, prefix="/api/v1")
app.include_router(analytics_router, prefix="/api/v1")
app.include_router(listings_router, prefix="/api/v1")

if os.getenv("ENABLE_DEMO", "false").lower() == "true":
    from api.v1.demo import router as demo_router
    app.include_router(demo_router, prefix="/api/v1")

app.include_router(metrics_router, prefix="/api/v1")
app.include_router(admin_router)

# ⚠️ допустимо для MVP
if settings.DEBUG:
    Base.metadata.create_all(bind=engine)