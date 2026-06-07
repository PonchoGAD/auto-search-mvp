from __future__ import annotations

import httpx
from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.types import Message

from src.config import settings
from src.keyboards.main import main_menu_keyboard
from src.logging import get_logger
from src.utils.internal_api import bot_api_headers


logger = get_logger(__name__)
router = Router()


def _upsert_user_url() -> str:
    return f"{settings.BOT_API_BASE_URL.rstrip('/')}{settings.BOT_API_PREFIX}/users/telegram/upsert"


async def _upsert_telegram_user(message: Message) -> bool:
    if not message.from_user:
        return False

    payload = {
        "telegram_user_id": message.from_user.id,
        "telegram_chat_id": message.chat.id,
        "username": message.from_user.username,
        "first_name": message.from_user.first_name,
        "last_name": message.from_user.last_name,
        "language_code": message.from_user.language_code,
    }

    try:
        async with httpx.AsyncClient(timeout=settings.BOT_API_TIMEOUT_SEC) as client:
            response = await client.post(
                _upsert_user_url(),
                json=payload,
                headers=bot_api_headers(),
            )
            response.raise_for_status()
            return True
    except Exception as exc:
        logger.exception(
            "start_user_upsert_failed telegram_user_id=%s error=%s",
            message.from_user.id,
            repr(exc),
        )
        return False


@router.message(CommandStart())
async def cmd_start(message: Message) -> None:
    user_ready = await _upsert_telegram_user(message)

    text = (
        "<b>Добро пожаловать в Auto Search Bot</b>\n\n"
        "Я помогу найти автомобиль по обычному текстовому запросу.\n\n"
        "Пример:\n"
        "<code>BMW до 3 млн пробег до 50 тыс</code>\n\n"
        "После поиска можно добавить авто в избранное или сохранить поиск для уведомлений."
    )

    if not user_ready:
        text += (
            "\n\n"
            "<b>Внимание:</b> сейчас не удалось синхронизировать профиль с bot_api. "
            "Поиск может работать, но избранное, сохраненные поиски и подписка могут быть недоступны. "
            "Проверь bot_api, DATABASE_URL и INTERNAL_API_KEY."
        )

    await message.answer(
        text,
        reply_markup=main_menu_keyboard(
            is_admin=bool(
                message.from_user
                and message.from_user.id in settings.admin_telegram_ids
            )
        ),
    )