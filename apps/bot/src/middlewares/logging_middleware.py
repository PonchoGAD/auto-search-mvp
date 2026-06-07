from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import CallbackQuery, Message, TelegramObject

from src.logging import get_logger


logger = get_logger(__name__)


class LoggingMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        if isinstance(event, Message):
            user = event.from_user
            logger.info(
                "message received user_id=%s username=%s text=%s",
                user.id if user else None,
                user.username if user else None,
                event.text or event.caption,
            )

        elif isinstance(event, CallbackQuery):
            user = event.from_user
            logger.info(
                "callback received user_id=%s username=%s data=%s",
                user.id if user else None,
                user.username if user else None,
                event.data,
            )

        return await handler(event, data)