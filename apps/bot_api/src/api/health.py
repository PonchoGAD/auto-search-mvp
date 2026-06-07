from __future__ import annotations

from datetime import datetime, timezone
import asyncio
import importlib

import httpx
from fastapi import APIRouter
from fastapi.responses import JSONResponse
from sqlalchemy import text

from src.clients.search_api import SearchApiClient
from src.config import settings
from src.db.session import SessionLocal
from src.schemas.common import HealthResponse


router = APIRouter(prefix="/health", tags=["health"])


def _checked_at() -> str:
    return datetime.now(timezone.utc).isoformat()


def _app_info() -> dict:
    return {
        "service": "bot-api",
        "version": "1.0.0",
        "app_name": settings.APP_NAME,
        "env": settings.APP_ENV,
        "debug": settings.DEBUG,
    }


def _postgres_check() -> dict:
    try:
        with SessionLocal() as db:
            db.execute(text("SELECT 1"))
        return {"status": "ok"}
    except Exception as exc:
        return {
            "status": "error",
            "error": str(exc),
        }


async def _search_core_check() -> dict:
    client = SearchApiClient()

    try:
        return {
            "status": "ok",
            "response": await client.health(),
        }
    except Exception as exc:
        return {
            "status": "error",
            "error": str(exc),
        }


def _overall_status(checks: dict[str, dict]) -> str:
    if all(item.get("status") == "ok" for item in checks.values()):
        return "ready"

    if any(item.get("status") == "ok" for item in checks.values()):
        return "degraded"

    return "error"


@router.get("", response_model=HealthResponse)
def health() -> HealthResponse:
    return HealthResponse(
        status="ok",
        service="bot-api",
        version="1.0.0",
    )


@router.get("/ready")
def readiness() -> dict:
    checked_at = _checked_at()
    checks = {
        "postgres": _postgres_check(),
    }

    return {
        "status": _overall_status(checks),
        **_app_info(),
        "checked_at": checked_at,
        "checks": checks,
    }


@router.get("/live")
def liveness() -> dict:
    return {
        "status": "alive",
        **_app_info(),
        "checked_at": _checked_at(),
    }


@router.get("/search-core")
async def search_core_health() -> dict:
    checked_at = _checked_at()
    check = await _search_core_check()

    return {
        "status": check["status"],
        **_app_info(),
        "dependency": "search-core",
        "checked_at": checked_at,
        "search_core": check,
    }


async def _check_redis() -> dict[str, str]:
    if not settings.REDIS_URL:
        return {"status": "disconnected", "error": "redis url is not configured"}

    try:
        redis_module = importlib.import_module("redis.asyncio")
        redis_client = redis_module.from_url(settings.REDIS_URL, socket_timeout=settings.REDIS_TIMEOUT_SEC)
        await redis_client.ping()
        await redis_client.close()
        return {"status": "connected"}
    except ImportError:
        return {"status": "disconnected", "error": "redis async client not installed"}
    except Exception as exc:
        return {"status": "disconnected", "error": str(exc)}


@router.get("/full")
async def full_health() -> JSONResponse:
    checked_at = _checked_at()
    results = await asyncio.gather(_check_redis(), _search_core_check())
    redis_result, search_core_result = results

    postgres_check = _postgres_check()
    status_code = 200
    detail_status = {
        "postgres": postgres_check["status"],
        "redis": redis_result["status"],
        "search_core": search_core_result["status"],
    }

    if postgres_check["status"] != "ok" or redis_result["status"] != "connected" or search_core_result["status"] != "ok":
        status_code = 503

    content = {
        "status": "ok" if status_code == 200 else "error",
        "postgres": postgres_check["status"],
        "redis": redis_result["status"],
        "search_core": search_core_result["status"],
        "checked_at": checked_at,
        "details": {},
    }

    if postgres_check["status"] != "ok":
        content["details"]["postgres"] = postgres_check.get("error", "postgres health check failed")

    if redis_result["status"] != "connected":
        content["details"]["redis"] = redis_result.get("error", "redis health check failed")

    if search_core_result["status"] != "ok":
        content["details"]["search_core"] = search_core_result.get("error", "search-core health check failed")

    return JSONResponse(status_code=status_code, content=content)
    