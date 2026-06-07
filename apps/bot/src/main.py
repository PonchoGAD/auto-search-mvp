from __future__ import annotations

import asyncio
import logging
from contextlib import suppress

from src.bot_app import build_bot_app
from src.config import settings
from src.lifecycle import on_shutdown, on_startup
from src.logging import get_logger, setup_logging


logger = get_logger(__name__)


async def main() -> None:
    setup_logging(logging.DEBUG if settings.DEBUG else logging.INFO)

    bot, dp = build_bot_app()

    try:
        await on_startup()

        logger.info("polling_started")
        await dp.start_polling(bot)

    except asyncio.CancelledError:
        logger.info("polling_cancelled")
        raise

    except KeyboardInterrupt:
        logger.info("polling_interrupted")

    except Exception as exc:
        logger.exception("polling_failed error=%s", repr(exc))
        raise

    finally:
        logger.info("polling_stopping")

        with suppress(Exception):
            await on_shutdown()

        with suppress(Exception):
            await bot.session.close()

        logger.info("polling_stopped")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass