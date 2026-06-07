from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from uuid import uuid4

from src.config import settings
from src.jobs.saved_search_alerts import SavedSearchAlertsJob
from src.jobs.subscription_expiry import SubscriptionExpiryJob
from src.logging import get_logger


logger = get_logger(__name__)


class WorkerScheduler:
    def __init__(self) -> None:
        self.saved_search_alerts_job = SavedSearchAlertsJob()
        self.subscription_expiry_job = SubscriptionExpiryJob()
        self._running = False
        self._run_lock = asyncio.Lock()
        self._last_run_started_at: datetime | None = None
        self._last_run_finished_at: datetime | None = None
        self._last_summary: dict | None = None
        self._last_run_id: str | None = None

    async def run_once(self) -> dict:
        run_id = uuid4().hex

        if self._run_lock.locked():
            summary = {
                "run_id": run_id,
                "status": "skipped_if_already_running",
                "started_at": None,
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "duration_ms": 0,
                "saved_search_alerts": None,
                "subscription_expiry": None,
            }
            self._last_summary = summary

            logger.warning(
                "scheduler_run_once_skipped_already_running",
                extra={
                    "run_id": run_id,
                    "status": summary["status"],
                    "duration_ms": 0,
                },
            )
            return summary

        async with self._run_lock:
            self._last_run_id = run_id
            self._last_run_started_at = datetime.now(timezone.utc)

            summary = {
                "run_id": run_id,
                "status": "running",
                "started_at": self._last_run_started_at.isoformat(),
                "finished_at": None,
                "duration_ms": None,
                "saved_search_alerts": None,
                "subscription_expiry": None,
            }

            logger.info(
                "scheduler_run_once_started",
                extra={
                    "run_id": run_id,
                    "status": "running",
                },
            )

            try:
                summary["saved_search_alerts"] = await self.saved_search_alerts_job.run()
            except Exception as exc:
                logger.exception(
                    "saved_search_alerts_job_failed error=%s",
                    repr(exc),
                    extra={
                        "run_id": run_id,
                        "job_name": "saved_search_alerts",
                        "status": "failed",
                    },
                )
                summary["saved_search_alerts"] = {
                    "status": "failed",
                    "error": str(exc),
                }

            try:
                summary["subscription_expiry"] = await self.subscription_expiry_job.run()
            except Exception as exc:
                logger.exception(
                    "subscription_expiry_job_failed error=%s",
                    repr(exc),
                    extra={
                        "run_id": run_id,
                        "job_name": "subscription_expiry",
                        "status": "failed",
                    },
                )
                summary["subscription_expiry"] = {
                    "status": "failed",
                    "error": str(exc),
                }

            self._last_run_finished_at = datetime.now(timezone.utc)
            duration_ms = int(
                (self._last_run_finished_at - self._last_run_started_at).total_seconds() * 1000
            )

            summary["status"] = "finished"
            summary["finished_at"] = self._last_run_finished_at.isoformat()
            summary["duration_ms"] = duration_ms

            self._last_summary = summary

            logger.info(
                "scheduler_run_once_summary summary=%s",
                summary,
                extra={
                    "run_id": run_id,
                    "status": summary["status"],
                    "duration_ms": duration_ms,
                },
            )
            return summary

    async def run_forever(self) -> None:
        self._running = True

        logger.info(
            "worker_scheduler_started poll_interval_sec=%s run_on_startup=%s",
            settings.SCHEDULER_POLL_INTERVAL_SEC,
            settings.SCHEDULER_RUN_ON_STARTUP,
        )

        if settings.SCHEDULER_RUN_ON_STARTUP:
            try:
                await self.run_once()
            except Exception as exc:
                logger.exception("scheduler_startup_run_failed error=%s", repr(exc))

        while self._running:
            try:
                await asyncio.sleep(settings.SCHEDULER_POLL_INTERVAL_SEC)

                if not self._running:
                    break

                await self.run_once()

            except asyncio.CancelledError:
                logger.info("scheduler_task_cancelled")
                self.stop()
                raise

            except Exception as exc:
                logger.exception("scheduler_loop_failed error=%s", repr(exc))

    def stop(self) -> None:
        self._running = False
        logger.info("worker_scheduler_stopped")

    def get_status(self) -> dict:
        return {
            "running": self._running,
            "locked": self._run_lock.locked(),
            "last_run_id": self._last_run_id,
            "last_run_started_at": self._last_run_started_at.isoformat()
            if self._last_run_started_at
            else None,
            "last_run_finished_at": self._last_run_finished_at.isoformat()
            if self._last_run_finished_at
            else None,
            "last_summary": self._last_summary,
        }

    @property
    def last_summary(self) -> dict | None:
        return self._last_summary

    @property
    def last_run_started_at(self) -> datetime | None:
        return self._last_run_started_at

    @property
    def last_run_finished_at(self) -> datetime | None:
        return self._last_run_finished_at