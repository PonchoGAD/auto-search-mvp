# apps/api/src/api/v1/health.py

from fastapi import APIRouter
from sqlalchemy import text
import redis
import requests
import time

from db.session import SessionLocal
from db.models import SearchHistory
from config import settings

router = APIRouter(tags=["Health"])


@router.get("/health", summary="Liveness probe")
def health():
    """
    Liveness probe.

    Проверяет только то, что процесс API жив
    и FastAPI может отдавать ответы.

    ❗ НИКАКИХ внешних зависимостей.
    """
    return {
        "status": "ok",
        "service": "auto-search-api",
        "env": settings.env,
    }


@router.get("/ready", summary="Readiness probe")
def readiness():
    """
    Readiness probe.

    Проверяет, что сервис ГОТОВ к работе:
    - PostgreSQL (DB)
    - Redis
    - Qdrant
    - Analytics (чтение SearchHistory)

    Используется для:
    - docker healthcheck
    - demo / production monitoring
    """
    checks = {}
    timings = {}

    # -------------------------
    # PostgreSQL / DB
    # -------------------------
    start = time.time()
    try:
        session = SessionLocal()
        session.execute(text("SELECT 1"))
        session.close()
        checks["postgres"] = "ok"
        checks["db"] = "ok"
    except Exception as e:
        checks["postgres"] = "error"
        checks["db"] = "error"
        checks["postgres_error"] = str(e)
    finally:
        timings["postgres_ms"] = int((time.time() - start) * 1000)

    # -------------------------
    # Redis
    # -------------------------
    start = time.time()
    try:
        r = redis.from_url(settings.redis_url, socket_timeout=2)
        r.ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = "error"
        checks["redis_error"] = str(e)
    finally:
        timings["redis_ms"] = int((time.time() - start) * 1000)

    # -------------------------
    # Qdrant
    # -------------------------
    start = time.time()
    try:
        resp = requests.get(
            f"{settings.qdrant_url}/collections",
            timeout=2,
        )
        if resp.status_code == 200:
            checks["qdrant"] = "ok"
        else:
            checks["qdrant"] = "error"
            checks["qdrant_status"] = resp.status_code
    except Exception as e:
        checks["qdrant"] = "error"
        checks["qdrant_error"] = str(e)
    finally:
        timings["qdrant_ms"] = int((time.time() - start) * 1000)

    # -------------------------
    # Analytics (light check)
    # -------------------------
    start = time.time()
    try:
        session = SessionLocal()
        session.query(SearchHistory.id).limit(1).all()
        session.close()
        checks["analytics"] = "ok"
    except Exception as e:
        checks["analytics"] = "error"
        checks["analytics_error"] = str(e)
    finally:
        timings["analytics_ms"] = int((time.time() - start) * 1000)

    # -------------------------
    # Итог
    # -------------------------
    is_ready = all(
        value == "ok"
        for key, value in checks.items()
        if not key.endswith("_error") and not key.endswith("_status")
    )

    return {
        "status": "ready" if is_ready else "not_ready",
        "checks": checks,
        "timings": timings,
    }
