from __future__ import annotations

from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from src.utils.callback_data import FavoriteCallback


def favorites_keyboard(
    listing_id: str,
    callback_id: str | None = None,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    item_id = callback_id or listing_id

    builder.button(
        text="Удалить из избранного",
        callback_data=FavoriteCallback(action="remove", item_id=item_id).pack(),
    )
    builder.button(text="В меню", callback_data="menu:main")

    builder.adjust(1, 1)
    return builder.as_markup()


def favorites_list_keyboard(
    page: int,
    has_prev: bool,
    has_next: bool,
    selected_listing_id: str | None = None,
    selected_callback_id: str | None = None,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    item_id = selected_callback_id or selected_listing_id

    if item_id:
        builder.button(
            text="Удалить выбранное",
            callback_data=FavoriteCallback(action="remove", item_id=item_id).pack(),
        )

    if has_prev:
        builder.button(text="Назад", callback_data=f"fav:list:{page - 1}")

    if has_next:
        builder.button(text="Дальше", callback_data=f"fav:list:{page + 1}")

    builder.button(text="В меню", callback_data="menu:main")

    builder.adjust(1, 2, 1)
    return builder.as_markup()