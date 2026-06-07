from __future__ import annotations

import time
from collections import deque
from typing import Any, Awaitable, Callable, Dict

from aiogram import BaseMiddleware
from aiogram.types import CallbackQuery, Message, TelegramObject

from src.config import settings


class ThrottlingMiddleware(BaseMiddleware):
    def __init__(self) -> None:
        self._message_hits: dict[tuple[int, str], float] = {}
        self._callback_hits: dict[tuple[int, str], float] = {}
        self._user_windows: dict[int, deque[float]] = {}

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        if isinstance(event, Message):
            user_id = event.from_user.id if event.from_user else 0
            text = (event.text or "").strip().lower()

            if self._is_user_rate_limited(user_id):
                await event.answer("Слишком много запросов. Попробуйте чуть позже.")
                return None

            key = (user_id, text or "__empty__")
            now = time.monotonic()
            last_hit = self._message_hits.get(key, 0.0)

            if now - last_hit < settings.THROTTLE_SEARCH_SEC:
                await event.answer("Слишком быстро, попробуйте через секунду.")
                return None

            self._message_hits[key] = now
            self._cleanup_old_hits(self._message_hits, now)

        elif isinstance(event, CallbackQuery):
            user_id = event.from_user.id if event.from_user else 0
            payload = (event.data or "").strip().lower()

            if self._is_user_rate_limited(user_id):
                await event.answer("Слишком много действий. Попробуйте позже.", show_alert=False)
                return None

            key = (user_id, payload or "__empty__")
            now = time.monotonic()
            last_hit = self._callback_hits.get(key, 0.0)

            if now - last_hit < settings.THROTTLE_CALLBACK_SEC:
                await event.answer("Слишком быстро", show_alert=False)
                return None

            self._callback_hits[key] = now
            self._cleanup_old_hits(self._callback_hits, now)

        return await handler(event, data)

    def _is_user_rate_limited(self, user_id: int) -> bool:
        if user_id <= 0:
            return False

        now = time.monotonic()
        window_sec = int(getattr(settings, "THROTTLE_WINDOW_SEC", 60))
        max_events = int(getattr(settings, "THROTTLE_MAX_EVENTS_PER_WINDOW", 60))

        bucket = self._user_windows.setdefault(user_id, deque())

        while bucket and now - bucket[0] > window_sec:
            bucket.popleft()

        if len(bucket) >= max_events:
            return True

        bucket.append(now)
        return False

    def _cleanup_old_hits(
        self,
        storage: dict[tuple[int, str], float],
        now: float,
    ) -> None:
        if len(storage) < 10_000:
            return

        ttl = int(getattr(settings, "THROTTLE_STORAGE_TTL_SEC", 300))
        expired_keys = [key for key, value in storage.items() if now - value > ttl]

        for key in expired_keys:
            storage.pop(key, None)