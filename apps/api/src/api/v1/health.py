# apps/api/src/api/v1/health.py

from fastapi import APIRouter
from sqlalchemy import text
import redis
import time
import json
from fastapi import Response, status

from db.session import SessionLocal
from db.models import SearchHistory
from core.settings import settings
from integrations.vector_db.qdrant import qdrant_client

try:
    from shared.embeddings.provider import embed_text
except Exception:
    embed_text = None

router = APIRouter(tags=["Health"])


def run_readiness_checks() -> tuple[bool, dict[str, str], dict[str, int]]:
    """Выполняет базовые проверки готовности сервиса (Postgres, Qdrant, Embeddings)."""
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
    # Qdrant
    # -------------------------
    start = time.time()
    try:
        qdrant_client.get_collections(timeout=2)
        checks["qdrant"] = "ok"
    except Exception as e:
        checks["qdrant"] = "error"
        checks["qdrant_error"] = str(e)
    finally:
        timings["qdrant_ms"] = int((time.time() - start) * 1000)

    # -------------------------
    # Embedding Provider (embeddings)
    # -------------------------
    start = time.time()
    try:
        if embed_text:
            vec = embed_text("healthcheck")
            if vec and len(vec) > 0:
                checks["embeddings"] = "ok"
            else:
                checks["embeddings"] = "degraded"
        else:
            checks["embeddings"] = "error"
            checks["embeddings_error"] = "Embedding provider import failed"
    except Exception as e:
        checks["embeddings"] = "error"
        checks["embeddings_error"] = str(e)
    finally:
        timings["embeddings_ms"] = int((time.time() - start) * 1000)

    is_ready = all(
        value == "ok"
        for key, value in checks.items()
        if not key.endswith("_error") and not key.endswith("_status")
    )

    return is_ready, checks, timings


@router.get("/health", summary="Liveness probe")
@router.get("/health/live", summary="Liveness probe alias")
def health():
    """
    Liveness probe.

    Проверяет только то, что процесс API жив.
    """
    return {
        "status": "ok",
        "service": "auto-search-api",
        "env": getattr(settings, "ENV", None) or "dev",
    }


@router.get("/ready", summary="Readiness probe")
@router.get("/health/ready", summary="Readiness probe")
def readiness():
    """
    Readiness probe.

    Проверяет готовность критических компонентов: DB, Qdrant, Embeddings.
    """
    is_ready, checks, timings = run_readiness_checks()
    response_status = status.HTTP_200_OK if is_ready else status.HTTP_503_SERVICE_UNAVAILABLE

    return Response(
        content=json.dumps({
            "service": "auto-search-api",
            "status": "ready" if is_ready else "not_ready",
            "checks": checks,
            "timings": timings,
        }),
        media_type="application/json",
        status_code=response_status
    )


@router.get("/health/full", summary="Full health check")
def health_full():
    """
    Full detailed check.

    Дополнительно проверяет Redis и подсистему аналитики.
    """
    checks = {}
    timings = {}

    is_ready, base_checks, base_timings = run_readiness_checks()
    checks.update(base_checks)
    timings.update(base_timings)

    # -------------------------
    # Redis
    # -------------------------
    start = time.time()
    try:
        r = redis.from_url(settings.REDIS_URL, socket_timeout=2)
        r.ping()
        checks["redis"] = "ok"
    except Exception as e:
        checks["redis"] = "error"
        checks["redis_error"] = str(e)
    finally:
        timings["redis_ms"] = int((time.time() - start) * 1000)

    # -------------------------
    # Analytics
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

    overall_healthy = all(
        value == "ok" or value == "degraded"
        for key, value in checks.items()
        if not key.endswith("_error")
    )

    response_status = status.HTTP_200_OK if overall_healthy else status.HTTP_503_SERVICE_UNAVAILABLE

    return Response(
        content=json.dumps({
            "status": "healthy" if overall_healthy else "degraded",
            "postgres": checks.get("postgres", "error"),
            "qdrant": checks.get("qdrant", "error"),
            "embeddings": checks.get("embeddings", "error"),
            "redis": checks.get("redis", "error"),
            "analytics": checks.get("analytics", "error"),
            "checks": checks,
            "timings": timings,
        }),
        media_type="application/json",
        status_code=response_status
    )