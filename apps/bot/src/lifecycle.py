from __future__ import annotations

import httpx

from src.config import settings
from src.logging import get_logger
from src.utils.internal_api import bot_api_headers


logger = get_logger(__name__)


def _bot_api_url(path: str) -> str:
    return (
        f"{settings.BOT_API_BASE_URL.rstrip('/')}"
        f"{settings.BOT_API_PREFIX.rstrip('/')}"
        f"/{path.lstrip('/')}"
    )


async def _check_endpoint(
    client: httpx.AsyncClient,
    name: str,
    url: str,
    protected: bool = False,
) -> dict:
    try:
        response = await client.get(
            url,
            headers=bot_api_headers() if protected else None,
        )
        response.raise_for_status()
        data = response.json()

        logger.info(
            "startup_health_check_ok dependency=%s url=%s status_code=%s data=%s",
            name,
            url,
            response.status_code,
            data,
        )

        return {
            "name": name,
            "status": "ok",
            "status_code": response.status_code,
            "data": data,
        }

    except Exception as exc:
        logger.warning(
            "startup_health_check_failed dependency=%s url=%s error=%s",
            name,
            url,
            repr(exc),
        )

        return {
            "name": name,
            "status": "error",
            "error": repr(exc),
        }


async def on_startup() -> None:
    logger.info("bot_startup_started")

    checks: list[dict] = []

    async with httpx.AsyncClient(timeout=settings.BOT_API_TIMEOUT_SEC) as client:
        checks.append(
            await _check_endpoint(
                client=client,
                name="bot_api",
                url=_bot_api_url("/health"),
            )
        )

        checks.append(
            await _check_endpoint(
                client=client,
                name="bot_api_postgres_ready",
                url=_bot_api_url("/health/ready"),
            )
        )

        checks.append(
            await _check_endpoint(
                client=client,
                name="search_core_via_bot_api",
                url=_bot_api_url("/health/search-core"),
            )
        )

    failed = [item for item in checks if item.get("status") != "ok"]

    if failed:
        logger.warning(
            "bot_startup_completed_with_warnings failed_checks=%s",
            failed,
        )
    else:
        logger.info("bot_startup_completed_all_dependencies_ok")


async def on_shutdown() -> None:
    logger.info("bot_shutdown_started")
    logger.info("bot_shutdown_complete")