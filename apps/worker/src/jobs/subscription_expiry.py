from __future__ import annotations

from datetime import datetime, timezone

from src.clients.bot_api import BotApiClient
from src.logging import get_logger


logger = get_logger(__name__)


class SubscriptionExpiryJob:
    def __init__(self) -> None:
        self.bot_api = BotApiClient()

    async def run(self) -> dict:
        started_at = datetime.now(timezone.utc)

        try:
            result = await self.bot_api.expire_overdue_subscriptions()
            finished_at = datetime.now(timezone.utc)
            duration_ms = int((finished_at - started_at).total_seconds() * 1000)

            message = str(result.get("message") or "")
            expired_count = None

            if "Expired subscriptions:" in message:
                try:
                    expired_count = int(message.rsplit(":", 1)[1].strip())
                except Exception:
                    expired_count = None

            summary = {
                "status": "ok",
                "started_at": started_at.isoformat(),
                "finished_at": finished_at.isoformat(),
                "duration_ms": duration_ms,
                "expired_count": expired_count,
                "result": result,
            }

            logger.info(
                "subscription_expiry_job_done summary=%s",
                summary,
            )

            return summary

        except Exception as exc:
            finished_at = datetime.now(timezone.utc)
            duration_ms = int((finished_at - started_at).total_seconds() * 1000)

            summary = {
                "status": "error",
                "started_at": started_at.isoformat(),
                "finished_at": finished_at.isoformat(),
                "duration_ms": duration_ms,
                "expired_count": None,
                "error": str(exc),
            }

            logger.exception(
                "subscription_expiry_job_failed error=%s",
                repr(exc),
            )

            return summary