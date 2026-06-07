from __future__ import annotations

from aiogram import Router
from aiogram.filters import Command
from aiogram.types import Message

from src.keyboards.main import main_menu_keyboard


router = Router()


@router.message(Command("help"))
async def cmd_help(message: Message) -> None:
    text = (
        "Команды:\n"
        "/start — запуск\n"
        "/help — помощь\n"
        "/profile — профиль\n"
        "/favorites — избранное\n"
        "/saved — сохраненные поиски\n"
        "/subscription — подписка\n\n"
        "Для поиска просто отправьте текст запроса."
    )
    await message.answer(text, reply_markup=main_menu_keyboard())