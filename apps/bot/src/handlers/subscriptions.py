from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import httpx
from aiogram import Router
from aiogram.filters import Command
from aiogram.types import CallbackQuery, LabeledPrice, Message, PreCheckoutQuery

from src.config import settings
from src.formatters.subscription_card import format_subscription_card
from src.keyboards.subscriptions import payment_keyboard, subscription_keyboard
from src.logging import get_logger
from src.utils.internal_api import bot_api_headers


logger = get_logger(__name__)
router = Router()


PLAN_TITLES = {
    "premium": "Premium",
    "pro": "Pro",
}


def _plan_price(plan: str) -> Decimal:
    if plan == "pro":
        return Decimal(str(getattr(settings, "PAYMENT_PLAN_PRO_PRICE", "1990.00")))

    return Decimal(str(getattr(settings, "PAYMENT_PLAN_PREMIUM_PRICE", "990.00")))


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

        if exc.response.status_code == 400:
            return "Некорректные данные платежа."

        if exc.response.status_code == 401:
            return "Ошибка доступа к bot_api. INTERNAL_API_KEY не совпадает или не передается."

        if exc.response.status_code == 403:
            return "Доступ запрещен. Проверь INTERNAL_API_KEY."

        if exc.response.status_code == 404:
            return "Пользователь, подписка или платеж не найдены. Нажмите /start и попробуйте снова."

        if exc.response.status_code == 409:
            return "Платеж уже существует. Проверьте статус оплаты."

        if exc.response.status_code == 502:
            return "bot_api временно недоступен."

    if isinstance(exc, httpx.ConnectError):
        return "Не удалось подключиться к bot_api."

    if isinstance(exc, httpx.TimeoutException):
        return "bot_api долго не отвечает. Попробуйте позже."

    return fallback


async def _get_subscription(telegram_user_id: int) -> dict:
    async with httpx.AsyncClient(timeout=settings.BOT_API_TIMEOUT_SEC) as client:
        response = await client.get(
            settings.subscriptions_me_url,
            params={"telegram_user_id": telegram_user_id},
            headers=bot_api_headers(),
        )
        response.raise_for_status()
        return response.json()


async def _get_payment_status(payment_id: int) -> dict:
    async with httpx.AsyncClient(timeout=settings.BOT_API_TIMEOUT_SEC) as client:
        response = await client.get(
            f"{settings.bot_api_url}/payments/{payment_id}/status",
            headers=bot_api_headers(),
        )
        response.raise_for_status()
        return response.json()


async def _create_payment(
    telegram_user_id: int,
    plan: str,
) -> dict:
    amount = _plan_price(plan)
    provider = getattr(settings, "PAYMENT_PROVIDER", "stub") or "stub"

    payload = {
        "telegram_user_id": telegram_user_id,
        "amount": str(amount),
        "currency": getattr(settings, "DEFAULT_CURRENCY", "RUB"),
        "provider": provider,
        "description": f"{PLAN_TITLES.get(plan, plan)} subscription",
        "plan": plan,
        "idempotency_key": f"tg:{telegram_user_id}:{plan}:{uuid4().hex[:16]}",
        "payload": {
            "source": "telegram_bot",
            "plan": plan,
        },
    }

    return_url = getattr(settings, "PAYMENT_RETURN_URL", None)
    success_url = getattr(settings, "PAYMENT_SUCCESS_URL", None)
    fail_url = getattr(settings, "PAYMENT_FAIL_URL", None)

    if return_url:
        payload["return_url"] = return_url

    if success_url:
        payload["success_url"] = success_url

    if fail_url:
        payload["fail_url"] = fail_url

    async with httpx.AsyncClient(timeout=settings.BOT_API_TIMEOUT_SEC) as client:
        response = await client.post(
            f"{settings.bot_api_url}/payments/create",
            json=payload,
            headers=bot_api_headers(),
        )
        response.raise_for_status()
        return response.json()


def _provider_hint(provider: str, payment_url: str | None) -> str:
    provider = (provider or "stub").lower()

    if provider == "stub":
        return (
            "Режим оплаты: <code>stub</code>\n"
            "Это тестовый режим. Если включён PAYMENT_STUB_SUCCESS_ENABLED, подписка активируется автоматически."
        )

    if provider == "yookassa":
        if payment_url:
            return "Режим оплаты: <code>ЮKassa</code>\nПерейдите по ссылке и оплатите картой РФ, СБП или ЮMoney."
        return "Режим оплаты: <code>ЮKassa</code>\nСсылка не сформирована. Проверьте YOOKASSA настройки в .env."

    if provider == "stars":
        return "Режим оплаты: <code>Telegram Stars ⭐</code>\nОплата звёздами Telegram — без карты, мгновенно."

    if provider == "telegram":
        return "Режим оплаты: <code>Telegram Payments</code>\nПроверьте TELEGRAM_PROVIDER_TOKEN и платёжную интеграцию."

    if provider == "stripe":
        return "Режим оплаты: <code>Stripe</code>\nStripe зарезервирован."

    return f"Режим оплаты: <code>{provider}</code>"


def _format_payment_created_text(
    plan: str,
    payment_data: dict,
) -> str:
    amount = _plan_price(plan)
    payment_url = (
        payment_data.get("payment_url")
        or payment_data.get("invoice_url")
        or payment_data.get("url")
    )
    payment_id = payment_data.get("id") or payment_data.get("payment_id")
    external_payment_id = payment_data.get("external_payment_id")
    provider = payment_data.get("provider") or "provider"
    status = payment_data.get("status") or "pending"

    text = (
        "<b>Платеж создан</b>\n\n"
        f"Тариф: <code>{PLAN_TITLES.get(plan, plan)}</code>\n"
        f"Сумма: <code>{amount} RUB</code>\n"
        f"Provider: <code>{provider}</code>\n"
        f"Статус: <code>{status}</code>\n"
    )

    if payment_id:
        text += f"ID платежа: <code>{payment_id}</code>\n"

    if external_payment_id:
        text += f"External ID: <code>{external_payment_id}</code>\n"

    text += "\n" + _provider_hint(str(provider), payment_url)

    if payment_url:
        text += f"\n\nСсылка на оплату:\n{payment_url}"

    return text


def _format_payment_status_text(payment_data: dict) -> str:
    status = payment_data.get("status") or "unknown"
    provider = payment_data.get("provider") or "provider"
    payment_id = payment_data.get("id")
    external_payment_id = payment_data.get("external_payment_id")
    paid_at = payment_data.get("paid_at")

    text = (
        "<b>Статус платежа</b>\n\n"
        f"ID: <code>{payment_id}</code>\n"
        f"Provider: <code>{provider}</code>\n"
        f"Статус: <code>{status}</code>\n"
    )

    if external_payment_id:
        text += f"External ID: <code>{external_payment_id}</code>\n"

    if paid_at:
        text += f"Оплачен: <code>{paid_at}</code>\n"

    if status == "succeeded":
        text += "\nОплата подтверждена. Подписка должна быть активирована."
    elif status == "pending":
        text += "\nОплата пока не подтверждена. Если вы уже оплатили, подождите webhook или проверьте позже."
    elif status in {"failed", "canceled", "refunded"}:
        text += "\nПлатеж не активен. Можно создать новый платеж."

    return text


@router.message(Command("subscription"))
async def cmd_subscription(message: Message) -> None:
    if not message.from_user:
        await message.answer("Не удалось определить пользователя Telegram.")
        return

    try:
        data = await _get_subscription(message.from_user.id)
    except Exception as exc:
        logger.exception("subscription_load_failed error=%s", repr(exc))
        await message.answer(_api_error_message(exc, "Ошибка загрузки подписки."))
        return

    prices = {
        "premium": str(_plan_price("premium")),
        "pro": str(_plan_price("pro")),
    }

    await message.answer(
        format_subscription_card({**data, "prices": prices}),
        reply_markup=subscription_keyboard(),
    )


@router.callback_query(lambda c: c.data and c.data.startswith("sub:"))
async def subscription_callbacks(callback: CallbackQuery) -> None:
    parts = (callback.data or "").split(":")
    action = parts[1] if len(parts) > 1 else ""
    plan = parts[2] if len(parts) > 2 else None

    if not callback.from_user:
        await callback.answer("Не удалось определить пользователя", show_alert=True)
        return

    if action in {"open", "refresh"}:
        try:
            data = await _get_subscription(callback.from_user.id)
        except Exception as exc:
            logger.exception("subscription_refresh_failed error=%s", repr(exc))
            await callback.answer(_api_error_message(exc, "Ошибка"), show_alert=True)
            return

        prices = {
            "premium": str(_plan_price("premium")),
            "pro": str(_plan_price("pro")),
        }

        if callback.message:
            await callback.message.answer(
                format_subscription_card({**data, "prices": prices}),
                reply_markup=subscription_keyboard(),
            )

        await callback.answer()
        return

    if action == "buy" and plan:
        if plan not in {"premium", "pro"}:
            await callback.answer("Неизвестный тариф", show_alert=True)
            return

        try:
            payment_data = await _create_payment(
                telegram_user_id=callback.from_user.id,
                plan=plan,
            )
        except Exception as exc:
            logger.exception("payment_create_failed error=%s", repr(exc))
            await callback.answer(
                _api_error_message(exc, "Ошибка создания платежа"),
                show_alert=True,
            )
            return

        external_payment_id = str(payment_data.get("external_payment_id") or "").strip()
        provider = str(payment_data.get("provider") or "stub").lower()

        if provider == "stars" and callback.message:
            # Telegram Stars: currency=XTR, provider_token="" (native Telegram)
            stars_amount = int(Decimal(str(payment_data.get("amount") or 0)))
            if stars_amount <= 0:
                await callback.answer("Ошибка: некорректное количество звёзд.", show_alert=True)
                return
            try:
                await callback.message.bot.send_invoice(
                    chat_id=callback.from_user.id,
                    title=f"{PLAN_TITLES.get(plan, plan)} подписка",
                    description=f"Тариф {PLAN_TITLES.get(plan, plan)} на {getattr(settings, 'PAYMENT_PLAN_DURATION_DAYS', 30)} дней.",
                    payload=external_payment_id,
                    provider_token="",
                    currency="XTR",
                    prices=[LabeledPrice(label=PLAN_TITLES.get(plan, plan), amount=stars_amount)],
                    start_parameter=f"sub_{plan}_{callback.from_user.id}",
                    need_name=False,
                    need_phone_number=False,
                    need_email=False,
                    need_shipping_address=False,
                )
            except Exception as exc:
                logger.exception("stars_invoice_send_failed error=%s", repr(exc))
                await callback.answer(
                    _api_error_message(exc, "Не удалось отправить счёт Telegram Stars"),
                    show_alert=True,
                )
                return

            await callback.message.answer(
                f"⭐ Счёт на {stars_amount} Stars отправлен. Оплатите прямо в Telegram.",
                reply_markup=payment_keyboard(
                    payment_url=None,
                    payment_id=int(payment_data.get("id") or 0),
                ),
            )
            await callback.answer()
            return

        if provider == "telegram" and callback.message:
            try:
                await callback.message.bot.send_invoice(
                    chat_id=callback.from_user.id,
                    title=f"{PLAN_TITLES.get(plan, plan)} подписка",
                    description=f"Оплата тарифа {PLAN_TITLES.get(plan, plan)} на {getattr(settings, 'PAYMENT_PLAN_DURATION_DAYS', 30)} дней.",
                    payload=external_payment_id,
                    provider_token=str(getattr(settings, "PAYMENT_TELEGRAM_PROVIDER_TOKEN", "") or ""),
                    currency=str(payment_data.get("currency") or "RUB").upper(),
                    prices=[LabeledPrice(label=PLAN_TITLES.get(plan, plan), amount=int(Decimal(str(payment_data.get("amount") or 0)) * 100))],
                    start_parameter=f"sub_{plan}_{callback.from_user.id}",
                    need_name=False,
                    need_phone_number=False,
                    need_email=False,
                    need_shipping_address=False,
                )
            except Exception as exc:
                logger.exception("telegram_invoice_send_failed error=%s", repr(exc))
                await callback.answer(
                    _api_error_message(exc, "Не удалось отправить счёт по Telegram Payments"),
                    show_alert=True,
                )
                return

            if callback.message:
                await callback.message.answer(
                    "Счёт Telegram Payments отправлен. После оплаты вы получите уведомление.",
                    reply_markup=payment_keyboard(
                        payment_url=None,
                        payment_id=int(payment_data.get("id") or 0),
                    ),
                )

            await callback.answer()
            return

        payment_id = payment_data.get("id") or payment_data.get("payment_id")
        payment_url = payment_data.get("payment_url") or payment_data.get("invoice_url")

        if callback.message:
            await callback.message.answer(
                _format_payment_created_text(
                    plan=plan,
                    payment_data=payment_data,
                ),
                reply_markup=payment_keyboard(
                    payment_url=payment_url,
                    payment_id=int(payment_id) if payment_id else 0,
                ),
                disable_web_page_preview=True,
            )

        await callback.answer()
        return

    if action == "check" and plan:
        try:
            payment_id = int(plan)
        except Exception:
            await callback.answer("Некорректный ID платежа", show_alert=True)
            return

        try:
            payment_data = await _get_payment_status(payment_id)
        except Exception as exc:
            logger.exception("payment_status_check_failed error=%s", repr(exc))
            await callback.answer(
                _api_error_message(exc, "Ошибка проверки платежа"),
                show_alert=True,
            )
            return

        if callback.message:
            await callback.message.answer(
                _format_payment_status_text(payment_data),
                reply_markup=subscription_keyboard(),
            )

        await callback.answer()
        return

    await callback.answer()


@router.pre_checkout_query()
async def pre_checkout(query: PreCheckoutQuery) -> None:
    configured_provider = str(getattr(settings, "PAYMENT_PROVIDER", "stub") or "stub").lower()
    is_stars = str(query.currency or "").upper() == "XTR"

    if not is_stars and configured_provider not in {"telegram"}:
        await query.answer(ok=False, error_message="Платёжный провайдер не настроен.")
        return

    if not query.invoice_payload:
        await query.answer(ok=False, error_message="Не найден payload счёта.")
        return

    await query.answer(ok=True)


@router.message(lambda message: message.successful_payment is not None)
async def successful_payment(message: Message) -> None:
    if not message.from_user or not message.successful_payment:
        return

    currency = str(message.successful_payment.currency or "").upper()
    is_stars = currency == "XTR"
    actual_provider = "stars" if is_stars else "telegram"

    # Stars work everywhere; Telegram Payments require specific provider setting
    configured_provider = str(getattr(settings, "PAYMENT_PROVIDER", "stub") or "stub").lower()
    if not is_stars and configured_provider != "telegram":
        return

    payment_payload = str(message.successful_payment.invoice_payload or "").strip()
    if not payment_payload:
        logger.warning("successful_payment_missing_payload provider=%s", actual_provider)
        return

    transaction_id = str(
        message.successful_payment.provider_payment_charge_id
        or message.successful_payment.telegram_payment_charge_id
        or payment_payload
    ).strip()

    # Stars: amount is in Stars (integer), no division needed
    # Telegram Payments: amount is in minor units (kopeks for RUB), divide by 100
    raw_amount = int(message.successful_payment.total_amount or 0)
    if is_stars:
        amount = Decimal(raw_amount)
    else:
        amount = Decimal(raw_amount) / 100

    signature = str(
        getattr(settings, "PAYMENT_TELEGRAM_PROVIDER_TOKEN", "")
        or getattr(settings, "PAYMENT_WEBHOOK_SECRET", "")
        or ""
    )

    webhook_payload = {
        "provider": actual_provider,
        "external_payment_id": payment_payload,
        "status": "succeeded",
        "amount": str(amount),
        "currency": currency,
        "payload": {
            "invoice_payload": payment_payload,
            "telegram_user_id": message.from_user.id,
        },
        "metadata": {
            "chat_id": message.chat.id,
            "telegram_message_id": message.message_id,
        },
        "transaction_id": transaction_id,
        "event_id": transaction_id,
        "signature": signature,
    }

    try:
        async with httpx.AsyncClient(timeout=settings.BOT_API_TIMEOUT_SEC) as client:
            response = await client.post(
                f"{settings.bot_api_url}/payments/webhook",
                json=webhook_payload,
                headers=bot_api_headers(),
            )
            response.raise_for_status()
    except Exception as exc:
        logger.exception("successful_payment_webhook_failed provider=%s error=%s", actual_provider, repr(exc))
        await message.answer("Произошла ошибка при подтверждении оплаты. Попробуйте позже или обратитесь в поддержку.")
        return

    if is_stars:
        await message.answer("⭐ Оплата Stars подтверждена! Ваша подписка активирована.")
    else:
        await message.answer("Оплата подтверждена. Спасибо! Ваша подписка активирована.")
