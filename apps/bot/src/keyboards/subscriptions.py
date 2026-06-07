from __future__ import annotations

from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from src.utils.callback_data import SubscriptionCallback


def subscription_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    builder.button(
        text="Купить Premium",
        callback_data=SubscriptionCallback(action="buy", plan="premium").pack(),
    )
    builder.button(
        text="Купить Pro",
        callback_data=SubscriptionCallback(action="buy", plan="pro").pack(),
    )
    builder.button(
        text="Обновить статус",
        callback_data=SubscriptionCallback(action="refresh").pack(),
    )
    builder.button(text="В меню", callback_data="menu:main")

    builder.adjust(2, 1, 1)
    return builder.as_markup()


def payment_keyboard(
    payment_url: str | None,
    payment_id: int,
) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    if payment_url:
        builder.button(
            text="Открыть оплату",
            url=payment_url,
        )

    if payment_id:
        builder.button(
            text="Проверить оплату",
            callback_data=SubscriptionCallback(
                action="check",
                plan=str(payment_id),
            ).pack(),
        )

    builder.button(
        text="К подписке",
        callback_data=SubscriptionCallback(action="open").pack(),
    )
    builder.button(text="В меню", callback_data="menu:main")

    builder.adjust(1, 1, 1, 1)
    return builder.as_markup()