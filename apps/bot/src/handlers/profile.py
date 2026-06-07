from __future__ import annotations

import httpx
from aiogram import Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from src.config import settings
from src.formatters.profile_card import format_profile_card
from src.keyboards.main import main_menu_keyboard
from src.logging import get_logger
from src.utils.internal_api import bot_api_headers


logger = get_logger(__name__)
router = Router()




def _api_error_message(exc: Exception, fallback: str) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        try:
            detail = exc.response.json().get("detail")
            if isinstance(detail, dict):
                return str(detail.get("message") or fallback)
            if isinstance(detail, str):
                return detail
        except Exception:
            pass

        if exc.response.status_code == 401:
            return "Ошибка доступа к bot_api. INTERNAL_API_KEY не совпадает или не передается."

        if exc.response.status_code == 403:
            return "Доступ запрещен. Проверь INTERNAL_API_KEY."

        if exc.response.status_code == 404:
            return "Профиль не найден. Нажмите /start и попробуйте снова."

    if isinstance(exc, httpx.ConnectError):
        return "Не удалось подключиться к bot_api."

    if isinstance(exc, httpx.TimeoutException):
        return "bot_api долго не отвечает. Попробуйте позже."

    return fallback


async def _get_profile(telegram_user_id: int) -> dict:
    async with httpx.AsyncClient(timeout=settings.BOT_API_TIMEOUT_SEC) as client:
        response = await client.get(
            f"{settings.bot_api_url}/users/me",
            params={"telegram_user_id": telegram_user_id},
            headers=bot_api_headers(),
        )
        response.raise_for_status()
        return response.json()


@router.message(Command("profile"))
async def cmd_profile(message: Message) -> None:
    if not message.from_user:
        await message.answer("Не удалось определить пользователя Telegram.")
        return

    try:
        profile = await _get_profile(message.from_user.id)
    except Exception as exc:
        logger.exception("profile_load_failed error=%s", repr(exc))
        await message.answer(_api_error_message(exc, "Ошибка загрузки профиля."))
        return

    await message.answer(
        format_profile_card(profile),
        reply_markup=main_menu_keyboard(
            is_admin=message.from_user.id in settings.admin_telegram_ids
        ),
    )


@router.callback_query(lambda c: c.data == "profile:open")
async def open_profile(callback: CallbackQuery) -> None:
    try:
        profile = await _get_profile(callback.from_user.id)
    except Exception as exc:
        logger.exception("profile_callback_failed error=%s", repr(exc))
        await callback.answer(
            _api_error_message(exc, "Ошибка загрузки профиля."),
            show_alert=True,
        )
        return

    await callback.message.answer(
        format_profile_card(profile),
        reply_markup=main_menu_keyboard(
            is_admin=callback.from_user.id in settings.admin_telegram_ids
        ),
    )
    await callback.answer()