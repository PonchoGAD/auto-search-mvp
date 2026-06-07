from __future__ import annotations

from typing import Any

from src.utils.text import escape_html, join_nonempty, safe_text


def _format_dt(value: Any) -> str | None:
    if not value:
        return None

    text = str(value).strip()

    if not text:
        return None

    if "T" in text:
        text = text.split("T", 1)[0]

    return text


def _limit_text(left: Any, used: Any = None, total: Any = None) -> str:
    if left is None:
        return "без лимита"

    if used is not None and total is not None:
        return f"{left} осталось, использовано {used} из {total}"

    return str(left)


def _plan_description(plan: str) -> str:
    if plan == "pro":
        return "Максимальный тариф для активного использования."

    if plan == "premium":
        return "Расширенный тариф для поиска, избранного и уведомлений."

    return "Бесплатный тариф с базовыми лимитами."


def _format_price(value: Any) -> str | None:
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    return f"{text} RUB"


def _pending_payment_warning(data: dict[str, Any]) -> str | None:
    payment = data.get("payment") or data.get("pending_payment")

    if not isinstance(payment, dict):
        return None

    status = str(payment.get("status") or "").strip()

    if status != "pending":
        return None

    payment_id = payment.get("id") or payment.get("payment_id")
    provider = payment.get("provider") or "provider"

    if payment_id:
        return f"Есть ожидающий платеж #{payment_id} через {provider}. После оплаты нажмите «Проверить оплату»."

    return f"Есть ожидающий платеж через {provider}."


def format_subscription_card(data: dict[str, Any]) -> str:
    plan = safe_text(data.get("plan"), "free")
    status = safe_text(data.get("status"), "active")
    is_premium = bool(data.get("is_premium", False))
    active = bool(data.get("active", False))

    starts_at = _format_dt(data.get("starts_at"))
    expires_at = _format_dt(data.get("expires_at"))

    searches_left = data.get("searches_left_today")
    saved_left = data.get("saved_searches_left")
    favorites_left = data.get("favorites_left")

    searches_used = data.get("searches_used_today")
    saved_count = data.get("saved_searches_count")
    favorites_count = data.get("favorites_count")

    free_daily_limit = data.get("free_daily_search_limit")
    free_saved_limit = data.get("free_saved_searches_limit")
    free_favorites_limit = data.get("free_favorites_limit")

    prices = data.get("prices") or {}
    premium_price = _format_price(prices.get("premium") if isinstance(prices, dict) else None)
    pro_price = _format_price(prices.get("pro") if isinstance(prices, dict) else None)

    pending_warning = _pending_payment_warning(data)

    lines: list[str] = [
        "<b>Подписка</b>",
        f"План: <code>{escape_html(plan)}</code>",
        f"Описание: {escape_html(_plan_description(plan))}",
        f"Статус: <code>{escape_html(status)}</code>",
        f"Premium/Pro: <code>{'Да' if is_premium else 'Нет'}</code>",
        f"Активна: <code>{'Да' if active else 'Нет'}</code>",
    ]

    if starts_at:
        lines.append(f"Начало: <code>{escape_html(starts_at)}</code>")

    if expires_at:
        lines.append(f"Окончание: <code>{escape_html(expires_at)}</code>")
    elif is_premium:
        lines.append("Окончание: <code>не ограничено</code>")

    if pending_warning:
        lines.append("")
        lines.append(f"<b>Ожидающий платеж:</b> {escape_html(pending_warning)}")

    lines.append("")
    lines.append("<b>Лимиты</b>")
    lines.append("Поиски сегодня: " + escape_html(_limit_text(searches_left, searches_used, free_daily_limit)))
    lines.append("Сохраненные поиски: " + escape_html(_limit_text(saved_left, saved_count, free_saved_limit)))
    lines.append("Избранное: " + escape_html(_limit_text(favorites_left, favorites_count, free_favorites_limit)))

    lines.append("")
    lines.append("<b>Тарифы</b>")

    if premium_price:
        lines.append(f"Premium: <code>{escape_html(premium_price)}</code>")
    else:
        lines.append("Premium: расширенные лимиты для регулярного поиска.")

    if pro_price:
        lines.append(f"Pro: <code>{escape_html(pro_price)}</code>")
    else:
        lines.append("Pro: максимальные лимиты для активного использования.")

    if not is_premium:
        lines.append("")
        lines.append("<b>Что дают Premium и Pro</b>")
        lines.append("Больше поисков в день.")
        lines.append("Больше избранного.")
        lines.append("Больше сохраненных поисков.")
        lines.append("Уведомления по новым объявлениям.")

    return join_nonempty(lines, sep="\n")