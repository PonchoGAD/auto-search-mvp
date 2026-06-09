from __future__ import annotations

import logging
import time
from collections import defaultdict, deque
from typing import Callable, Optional
from urllib.parse import urlsplit, urlunsplit
from uuid import uuid4

from fastapi import FastAPI, Request, Response, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from src.api.auth import router as auth_router
from src.api.favorites import router as favorites_router
from src.api.health import router as health_router
from src.api.internal import router as internal_router
from src.api.payments import router as payments_router
from src.api.saved_searches import router as saved_searches_router
from src.api.search_proxy import router as search_proxy_router
from src.api.subscriptions import router as subscriptions_router
from src.api.users import router as users_router
from src.config import settings
from src.logging import get_logger, setup_logging


_rate_limit_state: dict[str, deque[float]] = defaultdict(deque)
_redis_client: Optional[object] = None


def _client_key(request: Request) -> str:
    forwarded_for = request.headers.get("x-forwarded-for")
    if forwarded_for:
        return forwarded_for.split(",")[0].strip()

    if request.client:
        return request.client.host

    return "unknown"


def _is_internal_path(path: str) -> bool:
    return "/internal/" in path or path.endswith("/internal")


def _mask_database_url(url: str) -> str:
    if not url:
        return ""

    try:
        parts = urlsplit(url)
        netloc = parts.netloc

        if "@" in netloc:
            auth, host = netloc.rsplit("@", 1)

            if ":" in auth:
                username = auth.split(":", 1)[0]
                netloc = f"{username}:***@{host}"
            else:
                netloc = f"***@{host}"

        return urlunsplit(
            (
                parts.scheme,
                netloc,
                parts.path,
                parts.query,
                parts.fragment,
            )
        )
    except Exception:
        return "***"


def _request_id(request: Request) -> str:
    incoming = request.headers.get("x-request-id") or request.headers.get("X-Request-ID")
    clean = str(incoming or "").strip()

    if clean:
        return clean[:128]

    return uuid4().hex


def _is_health_path(path: str) -> bool:
    return path.endswith("/health") or path.endswith("/health/ready") or path.endswith("/health/live")


async def _rate_limited_redis(key: str, limit: int, window: int = 60) -> bool:
    global _redis_client
    if _redis_client is None:
        return False

    try:
        import redis.asyncio as aioredis
        r: aioredis.Redis = _redis_client  # type: ignore[assignment]
        bucket = int(time.time() // window)
        redis_key = f"rl:{key}:{bucket}"
        pipe = r.pipeline()
        pipe.incr(redis_key)
        pipe.expire(redis_key, window * 2)
        results = await pipe.execute()
        count = int(results[0])
        return count > limit
    except Exception:
        return False


def _rate_limited_memory(request: Request, limit: int) -> bool:
    now = time.monotonic()
    window = 60.0
    key = f"{_client_key(request)}:{request.url.path}"
    bucket = _rate_limit_state[key]

    while bucket and now - bucket[0] > window:
        bucket.popleft()

    if len(bucket) >= limit:
        return True

    bucket.append(now)
    return False


async def _rate_limited(request: Request) -> bool:
    if not settings.RATE_LIMIT_ENABLED:
        return False

    if _is_health_path(request.url.path):
        return False

    max_requests = max(1, int(settings.RATE_LIMIT_REQUESTS_PER_MINUTE))
    burst = max(1, int(settings.RATE_LIMIT_BURST))
    limit = max_requests + burst

    if _redis_client is not None:
        key = f"{_client_key(request)}:{request.url.path}"
        return await _rate_limited_redis(key, limit)

    return _rate_limited_memory(request, limit)


def create_application() -> FastAPI:
    setup_logging(settings.LOG_LEVEL if hasattr(settings, "LOG_LEVEL") else logging.INFO)
    logger = get_logger(__name__)

    app = FastAPI(
        title=settings.APP_NAME,
        version="1.0.0",
        debug=settings.DEBUG,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=settings.cors_origins,
        allow_credentials=True,
        allow_methods=["GET", "POST", "PATCH", "DELETE", "OPTIONS"],
        allow_headers=[
            "Authorization",
            "Content-Type",
            "X-Internal-Api-Key",
            "X-Webhook-Secret",
            "X-Request-ID",
        ],
    )

    @app.middleware("http")
    async def security_headers_middleware(
        request: Request,
        call_next: Callable,
    ) -> Response:
        request_id = _request_id(request)
        request.state.request_id = request_id

        if await _rate_limited(request):
            logger.warning(
                "request_rate_limited",
                extra={
                    "request_id": request_id,
                    "method": request.method,
                    "path": request.url.path,
                    "client": _client_key(request),
                    "status": status.HTTP_429_TOO_MANY_REQUESTS,
                    "internal": _is_internal_path(request.url.path),
                },
            )
            return JSONResponse(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                content={"detail": "Too many requests"},
                headers={"X-Request-ID": request_id},
            )

        started_at = time.monotonic()

        try:
            response = await call_next(request)
        except Exception:
            duration_ms = int((time.monotonic() - started_at) * 1000)
            logger.exception(
                "unhandled_request_error",
                extra={
                    "request_id": request_id,
                    "method": request.method,
                    "path": request.url.path,
                    "client": _client_key(request),
                    "duration_ms": duration_ms,
                    "internal": _is_internal_path(request.url.path),
                },
            )
            raise

        duration_ms = int((time.monotonic() - started_at) * 1000)

        response.headers["X-Request-ID"] = request_id
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["Referrer-Policy"] = "no-referrer"
        response.headers["X-Response-Time-Ms"] = str(duration_ms)

        if settings.is_prod:
            response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"

        log_extra = {
            "request_id": request_id,
            "method": request.method,
            "path": request.url.path,
            "status": response.status_code,
            "duration_ms": duration_ms,
            "client": _client_key(request),
            "internal": _is_internal_path(request.url.path),
        }

        if response.status_code >= 500:
            logger.error("request_completed_5xx", extra=log_extra)
        elif response.status_code >= 400:
            logger.warning("request_completed_4xx", extra=log_extra)
        else:
            logger.info("request_completed", extra=log_extra)

        return response

    api_prefix = settings.BOT_API_PREFIX

    app.include_router(health_router, prefix=api_prefix)
    app.include_router(auth_router, prefix=api_prefix)
    app.include_router(users_router, prefix=api_prefix)
    app.include_router(favorites_router, prefix=api_prefix)
    app.include_router(saved_searches_router, prefix=api_prefix)
    app.include_router(subscriptions_router, prefix=api_prefix)
    app.include_router(payments_router, prefix=api_prefix)
    app.include_router(search_proxy_router, prefix=api_prefix)
    app.include_router(internal_router, prefix=api_prefix)

    @app.on_event("startup")
    async def on_startup() -> None:
        global _redis_client

        security_issues = settings.validate_security()

        if security_issues:
            logger.warning("security_configuration_warnings issues=%s", security_issues)

        logger.info(
            "bot_api_started app=%s env=%s debug=%s",
            settings.APP_NAME,
            settings.APP_ENV,
            settings.DEBUG,
        )

        logger.info(
            "bot_api_database_config database_url=%s",
            _mask_database_url(settings.DATABASE_URL),
        )

        logger.info(
            "bot_api_search_core_config search_url=%s health_url=%s",
            getattr(settings, "search_url", None),
            getattr(settings, "search_health_url", None),
        )

        logger.info(
            "bot_api_migrations_note message=%s",
            "Database migrations must be applied with: alembic upgrade head. Base.metadata.create_all is not used.",
        )

        redis_url = getattr(settings, "REDIS_URL", None)
        if redis_url:
            try:
                import redis.asyncio as aioredis
                client = aioredis.Redis.from_url(
                    redis_url,
                    encoding="utf-8",
                    decode_responses=True,
                )
                await client.ping()
                _redis_client = client
                logger.info("rate_limit_redis_connected url=%s", redis_url.split("@")[-1])
            except Exception as exc:
                _redis_client = None
                logger.warning("rate_limit_redis_unavailable error=%s fallback=in_memory", repr(exc))
        else:
            logger.info("rate_limit_redis_not_configured fallback=in_memory")

    @app.on_event("shutdown")
    async def on_shutdown() -> None:
        global _redis_client
        if _redis_client is not None:
            try:
                await _redis_client.aclose()
            except Exception:
                pass
            _redis_client = None
        logger.info("bot_api_shutdown_complete")

    return app


app = create_application()