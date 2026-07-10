from __future__ import annotations

from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder


def admin_keyboard() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()

    builder.button(text="📡 Каналы источники", callback_data="admin:channels_list")

    builder.button(text="Статус системы", callback_data="admin:system_status")
    builder.button(text="Пользователи", callback_data="admin:user_stats")

    builder.button(text="Поиски", callback_data="admin:search_stats")
    builder.button(text="Последние поиски", callback_data="admin:latest_searches")

    builder.button(text="Избранное", callback_data="admin:favorites_stats")
    builder.button(text="Сохраненные поиски", callback_data="admin:saved_searches_stats")

    builder.button(text="Последние сохраненные", callback_data="admin:latest_saved_searches")
    builder.button(text="Доход", callback_data="admin:revenue_stats")

    builder.button(text="Платежи", callback_data="admin:payment_logs")
    builder.button(text="Подписки", callback_data="admin:subscription_stats")

    builder.button(text="Ручная подписка", callback_data="admin:manual_subscription_start")
    builder.button(text="Уведомления", callback_data="admin:notification_logs")

    builder.button(text="Pending уведомления", callback_data="admin:pending_notifications")
    builder.button(text="Запустить alerts", callback_data="admin:run_alerts")

    builder.button(text="Expire подписки", callback_data="admin:expire_subscriptions")
    builder.button(text="Ошибки системы", callback_data="admin:error_logs")

    builder.button(text="В меню", callback_data="menu:main")

    builder.adjust(1, 2, 2, 2, 2, 2, 2, 2, 2, 1)
    return builder.as_markup()