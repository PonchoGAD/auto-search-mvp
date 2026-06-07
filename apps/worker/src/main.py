from __future__ import annotations

import asyncio
import logging
import signal
from contextlib import suppress

from src.config import settings
from src.logging import get_logger, setup_logging
from src.scheduler import WorkerScheduler


logger = get_logger(__name__)


def _safe_value(value: object) -> str:
    return str(value or "").strip()


def _startup_diagnostics() -> None:
    logger.info(
        "worker_startup_config bot_api_url=%s search_api_url=%s scheduler_interval_sec=%s run_on_startup=%s",
        _safe_value(getattr(settings, "bot_api_url", "")),
        _safe_value(getattr(settings, "search_api_url", "")),
        getattr(settings, "SCHEDULER_POLL_INTERVAL_SEC", None),
        getattr(settings, "SCHEDULER_RUN_ON_STARTUP", None),
    )

    logger.info(
        "worker_alerts_config max_saved_searches_per_run=%s match_limit_per_search=%s max_new_items_per_search=%s max_items_per_message=%s",
        getattr(settings, "ALERTS_MAX_SAVED_SEARCHES_PER_RUN", None),
        getattr(settings, "ALERTS_MATCH_LIMIT_PER_SEARCH", None),
        getattr(settings, "ALERTS_MAX_NEW_ITEMS_PER_SEARCH", None),
        getattr(settings, "ALERTS_MAX_ITEMS_PER_MESSAGE", None),
    )


async def main() -> None:
    setup_logging(logging.DEBUG if settings.DEBUG else logging.INFO)
    _startup_diagnostics()

    scheduler = WorkerScheduler()
    stop_event = asyncio.Event()

    def _request_stop() -> None:
        logger.info("worker_stop_requested")
        scheduler.stop()
        stop_event.set()

    loop = asyncio.get_running_loop()

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _request_stop)
        except NotImplementedError:
            logger.warning("signal_handler_not_supported signal=%s", sig)

    worker_task = asyncio.create_task(scheduler.run_forever())

    try:
        logger.info("worker_starting")
        await stop_event.wait()
    except KeyboardInterrupt:
        logger.info("worker_interrupted")
        _request_stop()
    except asyncio.CancelledError:
        logger.info("worker_cancelled")
        _request_stop()
        raise
    finally:
        scheduler.stop()

        logger.info(
            "worker_scheduler_status_on_shutdown status=%s",
            scheduler.get_status(),
        )

        if not worker_task.done():
            worker_task.cancel()

        with suppress(asyncio.CancelledError):
            await worker_task

        logger.info("worker_stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass