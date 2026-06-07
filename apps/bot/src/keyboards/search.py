from __future__ import annotations

from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from src.utils.callback_data import FavoriteCallback, SearchCallback


def search_results_keyboard(
    page: int,
    has_prev: bool,
    has_next: bool,
    selected_listing_id: str | None = None,
    selected_callback_id: str | None = None,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    favorite_item_id = selected_callback_id or selected_listing_id

    if favorite_item_id:
        builder.button(
            text="В избранное",
            callback_data=FavoriteCallback(action="add", item_id=favorite_item_id).pack(),
        )

    builder.button(
        text="Сохранить поиск",
        callback_data=SearchCallback(action="save", page=page).pack(),
    )

    if has_prev:
        builder.button(
            text="Назад",
            callback_data=SearchCallback(action="page", page=page - 1).pack(),
        )

    if has_next:
        builder.button(
            text="Дальше",
            callback_data=SearchCallback(action="page", page=page + 1).pack(),
        )

    builder.button(
        text="Новый поиск",
        callback_data=SearchCallback(action="new", page=1).pack(),
    )
    builder.button(text="В меню", callback_data="menu:main")

    builder.adjust(2, 2, 2)
    return builder.as_markup()


def search_item_keyboard(
    listing_id: str,
    callback_id: str | None = None,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    item_id = callback_id or listing_id

    builder.button(
        text="В избранное",
        callback_data=FavoriteCallback(action="add", item_id=item_id).pack(),
    )
    builder.button(
        text="Удалить из избранного",
        callback_data=FavoriteCallback(action="remove", item_id=item_id).pack(),
    )
    builder.button(
        text="К результатам",
        callback_data=SearchCallback(action="back", page=1).pack(),
    )
    builder.button(text="В меню", callback_data="menu:main")

    builder.adjust(1, 1, 2)
    return builder.as_markup()