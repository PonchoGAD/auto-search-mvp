from __future__ import annotations

from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from src.utils.callback_data import ProfileCallback, SubscriptionCallback


def main_menu_keyboard(is_admin: bool = False) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    builder.button(text="Найти авто", callback_data="menu:search")
    builder.button(text="Избранное", callback_data="menu:favorites")
    builder.button(text="Сохраненные поиски", callback_data="menu:saved")
    builder.button(text="Профиль", callback_data=ProfileCallback(action="open").pack())
    builder.button(text="Подписка", callback_data=SubscriptionCallback(action="open").pack())

    if is_admin:
        builder.button(text="Админ", callback_data="menu:admin")
        builder.adjust(2, 2, 1, 1)
    else:
        builder.adjust(2, 2, 1)

    return builder.as_markup()