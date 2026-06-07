from __future__ import annotations

from aiogram import Router
from aiogram.types import CallbackQuery

from src.config import settings
from src.keyboards.main import main_menu_keyboard


router = Router()


def _is_admin(user_id: int | None) -> bool:
    return bool(user_id and user_id in settings.admin_telegram_ids)


@router.callback_query(lambda c: c.data == "menu:main")
async def menu_main(callback: CallbackQuery) -> None:
    await callback.message.answer(
        "<b>Главное меню</b>\n\n"
        "Выберите действие или напишите поисковый запрос обычным текстом.",
        reply_markup=main_menu_keyboard(
            is_admin=_is_admin(callback.from_user.id if callback.from_user else None)
        ),
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "menu:search")
async def menu_search(callback: CallbackQuery) -> None:
    await callback.message.answer(
        "<b>Поиск авто</b>\n\n"
        "Отправьте запрос обычным текстом.\n\n"
        "Примеры:\n"
        "<code>BMW дизель до 3 млн пробег до 100 тыс</code>\n"
        "<code>Toyota Camry 2019 бензин</code>\n"
        "<code>Mercedes до 5 млн</code>"
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "menu:favorites")
async def menu_favorites(callback: CallbackQuery) -> None:
    await callback.message.answer(
        "Откройте избранное командой /favorites."
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "menu:saved")
async def menu_saved(callback: CallbackQuery) -> None:
    await callback.message.answer(
        "Откройте сохраненные поиски командой /saved."
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "menu:subscription")
async def menu_subscription(callback: CallbackQuery) -> None:
    await callback.message.answer(
        "Откройте подписку командой /subscription."
    )
    await callback.answer()


@router.callback_query(lambda c: c.data == "menu:admin")
async def menu_admin(callback: CallbackQuery) -> None:
    if not _is_admin(callback.from_user.id if callback.from_user else None):
        await callback.answer("Нет доступа", show_alert=True)
        return

    await callback.message.answer("Откройте админ-панель командой /admin.")
    await callback.answer()


@router.callback_query(lambda c: c.data == "noop")
async def noop_callback(callback: CallbackQuery) -> None:
    await callback.answer()