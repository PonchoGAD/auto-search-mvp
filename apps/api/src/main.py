from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import JSONResponse
import os

from db.session import engine
from db.models import Base

from api.v1.health import router as health_router
from api.v1.search import router as search_router
from api.v1.search_history import router as search_history_router
from api.v1.analytics import router as analytics_router
from api.v1.admin import router as admin_router
from api.v1.metrics import router as metrics_router

from core.settings import settings


app = FastAPI(
    title="Auto Semantic Search MVP",
    version="0.1.0",
    debug=settings.DEBUG,
)

API_KEY = os.getenv("API_KEY")


# @app.middleware("http")
# async def check_api_key(request: Request, call_next):

    # Разрешаем health и readiness без ключа
    if request.url.path.startswith("/api/v1/health") or \
       request.url.path.startswith("/api/v1/ready"):
        return await call_next(request)

    # Защищаем остальное API
    if request.url.path.startswith("/api/"):
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

if os.getenv("ENABLE_DEMO", "false").lower() == "true":
    from api.v1.demo import router as demo_router
    app.include_router(demo_router, prefix="/api/v1")

app.include_router(metrics_router, prefix="/api/v1")
app.include_router(admin_router)

# ⚠️ допустимо для MVP
Base.metadata.create_all(bind=engine)