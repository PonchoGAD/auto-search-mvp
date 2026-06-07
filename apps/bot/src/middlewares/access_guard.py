from __future__ import annotations

from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import CallbackQuery, Message, TelegramObject

from src.config import settings


class AccessGuardMiddleware(BaseMiddleware):
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        user_id = self._extract_user_id(event)
        is_admin = bool(data.get("is_admin", False)) or user_id in settings.admin_telegram_ids

        callback_data = None
        if isinstance(event, CallbackQuery):
            callback_data = event.data or ""

        if callback_data and callback_data.startswith("admin:") and not is_admin:
            await event.answer("Нет доступа", show_alert=True)
            return None

        if isinstance(event, Message):
            text = (event.text or "").strip().lower()

            if text.startswith("/admin") and not is_admin:
                await event.answer("Нет доступа")
                return None

            if self._looks_like_abuse(text):
                await event.answer("Запрос отклонен. Напишите запрос обычным текстом.")
                return None

        data["is_admin"] = is_admin
        return await handler(event, data)

    def _extract_user_id(self, event: TelegramObject) -> int:
        if isinstance(event, Message) and event.from_user:
            return int(event.from_user.id)

        if isinstance(event, CallbackQuery) and event.from_user:
            return int(event.from_user.id)

        return 0

    def _looks_like_abuse(self, text: str) -> bool:
        if not text:
            return False

        if len(text) > int(getattr(settings, "MAX_MESSAGE_TEXT_LEN", 1000)):
            return True

        blocked_fragments = (
            "<script",
            "javascript:",
            "drop table",
            "delete from",
            "insert into",
            "update users",
            "../",
            "..\\",
            "/etc/passwd",
        )

        lowered = text.lower()
        return any(fragment in lowered for fragment in blocked_fragments)