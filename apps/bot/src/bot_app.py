from __future__ import annotations

import logging

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode

from src.config import settings
from src.handlers.admin import router as admin_router
from src.handlers.callbacks import router as callbacks_router
from src.handlers.favorites import router as favorites_router
from src.handlers.help import router as help_router
from src.handlers.profile import router as profile_router
from src.handlers.saved_searches import router as saved_searches_router
from src.handlers.search import router as search_router
from src.handlers.start import router as start_router
from src.handlers.subscriptions import router as subscriptions_router
from src.logging import get_logger, setup_logging
from src.middlewares.access_guard import AccessGuardMiddleware
from src.middlewares.logging_middleware import LoggingMiddleware
from src.middlewares.throttling import ThrottlingMiddleware
from src.middlewares.user_context import UserContextMiddleware


logger = get_logger(__name__)


def create_bot() -> Bot:
    return Bot(
        token=settings.BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )


def create_dispatcher() -> Dispatcher:
    dp = Dispatcher()

    dp.message.middleware(LoggingMiddleware())
    dp.callback_query.middleware(LoggingMiddleware())

    dp.message.middleware(ThrottlingMiddleware())
    dp.callback_query.middleware(ThrottlingMiddleware())

    dp.message.middleware(UserContextMiddleware())
    dp.callback_query.middleware(UserContextMiddleware())

    dp.message.middleware(AccessGuardMiddleware())
    dp.callback_query.middleware(AccessGuardMiddleware())

    dp.include_router(start_router)
    dp.include_router(help_router)
    dp.include_router(admin_router)   # must be before search_router: state handlers (add_channel, manual_sub) win over search
    dp.include_router(profile_router)
    dp.include_router(subscriptions_router)
    dp.include_router(saved_searches_router)
    dp.include_router(favorites_router)
    dp.include_router(search_router)
    dp.include_router(callbacks_router)

    return dp


def build_bot_app() -> tuple[Bot, Dispatcher]:
    setup_logging(logging.DEBUG if settings.DEBUG else logging.INFO)

    bot = create_bot()
    dp = create_dispatcher()

    logger.info("bot application assembled")
    return bot, dp