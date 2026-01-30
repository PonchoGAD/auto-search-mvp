# apps/api/src/api/v1/health.py

from fastapi import APIRouter
from sqlalchemy import text
import redis
import requests

from db.session import SessionLocal
from config import settings

router = APIRouter(tags=["Health"])


@router.get("/health", summary="Liveness probe")
def health():
    """
    Проверка, что процесс API жив.
    НИКАКИХ внешних зависимостей.
    """
    return {
        "status": "ok",
        "service": "auto-search-api",
        "env": settings.env,
    }


@router.get("/ready", summary="Readiness probe")
def readiness():
    """
    Проверка, что сервис готов к работе:
    - PostgreSQL
    - Redis
    - Qdrant
    """
    checks = {}

    # -------------------------
    # PostgreSQL
    # -------------------------
    try:
        session = SessionLocal()
        session.execute(text("SELECT 1"))
        session.close()
        checks["postgres"] = "ok"
    except Exception as e:
        checks["postgres"] = f"error: {e}"

    # -------------------------
    # Redis
    # -------------------------
    try:
        r = redis.from_url(settings.redis_url, socket_timeout=2)
        r.ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = f"error: {e}"

    # -------------------------
    # Qdrant
    # -------------------------
    try:
        resp = requests.get(f"{settings.qdrant_url}/collections", timeout=2)
        if resp.status_code == 200:
            checks["qdrant"] = "ok"
        else:
            checks["qdrant"] = f"bad_status: {resp.status_code}"
    except Exception as e:
        checks["qdrant"] = f"error: {e}"

    # -------------------------
    # Итог
    # -------------------------
    is_ready = all(v == "ok" for v in checks.values())

    return {
        "status": "ready" if is_ready else "not_ready",
        "checks": checks,
    }
