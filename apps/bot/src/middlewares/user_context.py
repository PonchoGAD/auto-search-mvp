from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict

import httpx
from aiogram import BaseMiddleware
from aiogram.types import CallbackQuery, Message, TelegramObject

from src.config import settings
from src.logging import get_logger


logger = get_logger(__name__)


class UserContextMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        tg_user = None

        if isinstance(event, Message):
            tg_user = event.from_user
        elif isinstance(event, CallbackQuery):
            tg_user = event.from_user

        if not tg_user:
            return await handler(event, data)

        payload = {
            "telegram_user_id": tg_user.id,
            "telegram_chat_id": event.chat.id if isinstance(event, Message) and event.chat else None,
            "username": tg_user.username,
            "first_name": tg_user.first_name,
            "last_name": tg_user.last_name,
            "language_code": tg_user.language_code,
        }

        try:
            async with httpx.AsyncClient(timeout=settings.BOT_API_TIMEOUT_SEC) as client:
                await client.post(
                    settings.users_upsert_url,
                    json=payload,
                )
        except Exception as exc:
            logger.warning("failed to upsert telegram user: %s", exc)

        data["telegram_user"] = tg_user
        data["telegram_user_id"] = tg_user.id
        data["telegram_username"] = tg_user.username
        data["is_admin"] = tg_user.id in settings.admin_telegram_ids

        return await handler(event, data)