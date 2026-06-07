from __future__ import annotations

import json
from typing import Any

import httpx
from aiogram import Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, Message

from src.config import settings
from src.keyboards.admin import admin_keyboard
from src.logging import get_logger
from src.utils.internal_api import bot_api_headers


logger = get_logger(__name__)
router = Router()


_manual_subscription_state: dict[int, dict[str, Any]] = {}


def _is_admin(user_id: int) -> bool:
    return user_id in settings.admin_telegram_ids


def _internal_headers() -> dict[str, str]:
    return bot_api_headers()


def _format_json(payload: Any) -> str:
    try:
        return json.dumps(payload, ensure_ascii=False, indent=2, default=str)
    except Exception:
        return str(payload)


def _escape_html(value: str) -> str:
    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
    )


def _truncate(text: str, limit: int = 3200) -> str:
    if len(text) <= limit:
        return text

    return text[:limit].rstrip() + "\n...truncated..."


def _render_stats_block(title: str, payload: Any) -> str:
    safe_payload = _escape_html(_truncate(_format_json(payload)))
    return f"<b>{_escape_html(title)}</b>\n<pre>{safe_payload}</pre>"


def _api_error_message(exc: Exception) -> str:
    if isinstance(exc, httpx.HTTPStatusError):
        try:
            detail = exc.response.json().get("detail")
            if isinstance(detail, dict):
                return str(detail.get("message") or "HTTP error")
            if isinstance(detail, str):
                return detail
        except Exception:
            pass

        if exc.response.status_code == 401:
            return "Ошибка internal API key"

        if exc.response.status_code == 403:
            return "Недостаточно прав"

        if exc.response.status_code == 404:
            return "Endpoint не найден"

        if exc.response.status_code >= 500:
            return "bot_api internal server error"

    if isinstance(exc, httpx.ConnectError):
        return "Нет соединения с bot_api"

    if isinstance(exc, httpx.TimeoutException):
        return "bot_api timeout"

    return f"Ошибка: {repr(exc)}"


async def _admin_get(
    client: httpx.AsyncClient,
    path: str,
    params: dict | None = None,
) -> Any:
    response = await client.get(
        f"{settings.bot_api_url}{path}",
        headers=_internal_headers(),
        params=params,
    )
    response.raise_for_status()
    return response.json()


async def _admin_post(
    client: httpx.AsyncClient,
    path: str,
    json_payload: dict | None = None,
) -> Any:
    response = await client.post(
        f"{settings.bot_api_url}{path}",
        headers=_internal_headers(),
        json=json_payload or {},
    )
    response.raise_for_status()
    return response.json()


def _manual_subscription_prompt(step: str) -> str:
    if step == "telegram_user_id":
        return "Введите telegram_user_id пользователя."

    if step == "plan":
        return "Введите тариф: free, premium или pro."

    if step == "duration_days":
        return "Введите срок подписки в днях. Для free можно отправить 0."

    if step == "reason":
        return "Введите причину выдачи подписки."

    return "Введите значение."


@router.message(Command("admin"))
async def cmd_admin(message: Message) -> None:
    if not message.from_user or not _is_admin(message.from_user.id):
        await message.answer("Нет доступа.")
        return

    await message.answer(
        "Админ панель",
        reply_markup=admin_keyboard(),
    )


@router.message(Command("cancel_admin"))
async def cancel_admin_flow(message: Message) -> None:
    if not message.from_user or not _is_admin(message.from_user.id):
        await message.answer("Нет доступа.")
        return

    _manual_subscription_state.pop(message.from_user.id, None)
    await message.answer("Админ-действие отменено.", reply_markup=admin_keyboard())


@router.message(lambda m: m.from_user and m.from_user.id in _manual_subscription_state)
async def manual_subscription_flow(message: Message) -> None:
    if not message.from_user or not _is_admin(message.from_user.id):
        await message.answer("Нет доступа.")
        return

    admin_id = message.from_user.id
    state = _manual_subscription_state.get(admin_id) or {}
    step = state.get("step")
    value = (message.text or "").strip()

    if not value:
        await message.answer("Пустое значение. " + _manual_subscription_prompt(step))
        return

    if step == "telegram_user_id":
        try:
            state["telegram_user_id"] = int(value)
        except ValueError:
            await message.answer("telegram_user_id должен быть числом.")
            return

        state["step"] = "plan"
        _manual_subscription_state[admin_id] = state
        await message.answer(_manual_subscription_prompt("plan"))
        return

    if step == "plan":
        plan = value.lower()
        if plan not in {"free", "premium", "pro"}:
            await message.answer("Тариф должен быть: free, premium или pro.")
            return

        state["plan"] = plan
        state["step"] = "duration_days"
        _manual_subscription_state[admin_id] = state
        await message.answer(_manual_subscription_prompt("duration_days"))
        return

    if step == "duration_days":
        try:
            duration_days = int(value)
        except ValueError:
            await message.answer("duration_days должен быть числом.")
            return

        if duration_days < 0:
            await message.answer("duration_days не может быть отрицательным.")
            return

        state["duration_days"] = duration_days
        state["step"] = "reason"
        _manual_subscription_state[admin_id] = state
        await message.answer(_manual_subscription_prompt("reason"))
        return

    if step == "reason":
        state["reason"] = value
        payload = {
            "telegram_user_id": state["telegram_user_id"],
            "plan": state["plan"],
            "duration_days": state["duration_days"],
            "reason": state["reason"],
            "admin_telegram_id": admin_id,
        }

        try:
            async with httpx.AsyncClient(timeout=settings.BOT_API_TIMEOUT_SEC) as client:
                result = await _admin_post(
                    client,
                    "/internal/admin/manual-activate-subscription",
                    json_payload=payload,
                )
        except Exception as exc:
            logger.exception("manual_subscription_failed error=%s", repr(exc))
            await message.answer(_api_error_message(exc), reply_markup=admin_keyboard())
            return
        finally:
            _manual_subscription_state.pop(admin_id, None)

        await message.answer(
            _render_stats_block("Manual subscription activated", result),
            reply_markup=admin_keyboard(),
        )
        return

    _manual_subscription_state.pop(admin_id, None)
    await message.answer("Состояние сброшено.", reply_markup=admin_keyboard())


@router.callback_query(lambda c: c.data and c.data.startswith("admin:"))
async def admin_callbacks(callback: CallbackQuery) -> None:
    if not callback.from_user or not _is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return

    action = (callback.data or "").split(":")[1]

    if action == "manual_subscription_start":
        _manual_subscription_state[callback.from_user.id] = {
            "step": "telegram_user_id",
        }

        if callback.message:
            await callback.message.answer(
                _manual_subscription_prompt("telegram_user_id")
                + "\n\nДля отмены отправьте /cancel_admin"
            )

        await callback.answer()
        return

    try:
        async with httpx.AsyncClient(timeout=settings.BOT_API_TIMEOUT_SEC) as client:
            if action in {"health", "system_status"}:
                payload = await _admin_get(client, "/internal/admin/system-status")
                text = _render_stats_block("System Status", payload)

            elif action == "pending_notifications":
                payload = await _admin_get(
                    client,
                    "/internal/notifications/pending",
                    params={"limit": 100},
                )
                text = _render_stats_block(
                    f"Pending notifications: {len(payload)}",
                    payload,
                )

            elif action == "expire_subscriptions":
                payload = await _admin_post(
                    client,
                    "/internal/subscriptions/expire-overdue",
                )
                text = _render_stats_block("Expire subscriptions", payload)

            elif action == "user_stats":
                payload = await _admin_get(
                    client,
                    "/internal/admin/user-stats",
                )
                text = _render_stats_block("User Stats", payload)

            elif action == "search_stats":
                payload = await _admin_get(
                    client,
                    "/internal/admin/search-stats",
                )
                text = _render_stats_block("Search Stats", payload)

            elif action == "favorites_stats":
                payload = await _admin_get(
                    client,
                    "/internal/admin/favorites-stats",
                )
                text = _render_stats_block("Favorites Stats", payload)

            elif action == "saved_searches_stats":
                payload = await _admin_get(
                    client,
                    "/internal/admin/saved-searches-stats",
                )
                text = _render_stats_block("Saved Searches Stats", payload)

            elif action == "latest_searches":
                payload = await _admin_get(
                    client,
                    "/internal/admin/latest-searches",
                    params={"limit": 50},
                )
                text = _render_stats_block("Latest Searches", payload)

            elif action == "latest_saved_searches":
                payload = await _admin_get(
                    client,
                    "/internal/admin/latest-saved-searches",
                    params={"limit": 50},
                )
                text = _render_stats_block("Latest Saved Searches", payload)

            elif action == "revenue_stats":
                payload = await _admin_get(
                    client,
                    "/internal/admin/revenue-stats",
                )
                text = _render_stats_block("Revenue Stats", payload)

            elif action == "subscription_stats":
                payload = await _admin_get(
                    client,
                    "/internal/admin/subscription-stats",
                )
                text = _render_stats_block("Subscription Stats", payload)

            elif action == "payment_logs":
                payload = await _admin_get(
                    client,
                    "/internal/admin/payment-logs",
                    params={"limit": 50},
                )
                text = _render_stats_block("Payment Logs", payload)

            elif action == "notification_logs":
                payload = await _admin_get(
                    client,
                    "/internal/admin/notification-logs",
                    params={"limit": 50},
                )
                text = _render_stats_block("Notification Logs", payload)

            elif action == "run_alerts":
                payload = await _admin_post(
                    client,
                    "/internal/admin/run-alerts",
                )
                text = _render_stats_block("Manual Alerts Run", payload)

            elif action == "error_logs":
                payload = await _admin_get(
                    client,
                    "/internal/admin/error-logs",
                    params={"limit": 50},
                )
                text = _render_stats_block("System Errors", payload)

            else:
                await callback.answer("Неизвестное действие", show_alert=True)
                return

        if callback.message:
            await callback.message.answer(
                text,
                reply_markup=admin_keyboard(),
            )

    except Exception as exc:
        logger.exception("admin action failed: %s", exc)
        await callback.answer(
            _api_error_message(exc),
            show_alert=True,
        )
        return

    await callback.answer()