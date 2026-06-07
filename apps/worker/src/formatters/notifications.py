from __future__ import annotations

from typing import Any


TELEGRAM_MESSAGE_LIMIT = 3900


def _escape_html(value: str | None) -> str:
    if value is None:
        return ""

    return (
        str(value)
        .replace("&", "&amp;")
        .replace("<", "&lt;")
        .replace(">", "&gt;")
        .replace('"', "&quot;")
    )


def _safe_text(value: Any, fallback: str = "—") -> str:
    text = str(value or "").strip()
    return text if text else fallback


def _truncate(value: str, max_len: int = 160) -> str:
    text = str(value or "").strip()

    if len(text) <= max_len:
        return text

    return text[: max_len - 1].rstrip() + "…"


def _format_price(price: Any, currency: str | None = "RUB") -> str:
    if price is None:
        return "Цена не указана"

    try:
        value = f"{int(price):,}".replace(",", " ")
    except Exception:
        return "Цена не указана"

    if currency == "RUB":
        return f"{value} ₽"

    if currency:
        return f"{value} {currency}"

    return value


def _format_mileage(mileage: Any) -> str | None:
    if mileage is None:
        return None

    try:
        return f"{int(mileage):,}".replace(",", " ") + " км"
    except Exception:
        return None


def _format_header(item: dict[str, Any]) -> str:
    title = _safe_text(item.get("title"), "")
    brand = _safe_text(item.get("brand"), "")
    model = _safe_text(item.get("model"), "")

    header = title or " ".join(x for x in [brand, model] if x).strip() or "Объявление"
    return _truncate(header, max_len=120)


def _format_location(item: dict[str, Any]) -> str | None:
    city = _safe_text(item.get("city"), "")
    region = _safe_text(item.get("region"), "")

    if city and region and city.lower() != region.lower():
        return f"{city}, {region}"

    return city or region or None


def _format_extra(item: dict[str, Any]) -> str:
    price = _format_price(item.get("price"), item.get("currency"))
    year = item.get("year")
    mileage = _format_mileage(item.get("mileage"))
    location = _format_location(item)
    fuel = item.get("fuel")

    extra: list[str] = []

    if year:
        extra.append(str(year))

    if fuel:
        extra.append(str(fuel))

    if mileage:
        extra.append(mileage)

    if location:
        extra.append(location)

    extra.append(price)

    return " • ".join(extra)


def _trim_message(text: str) -> str:
    if len(text) <= TELEGRAM_MESSAGE_LIMIT:
        return text

    suffix = "\n\nСообщение сокращено, откройте бот для просмотра всех результатов."
    limit = TELEGRAM_MESSAGE_LIMIT - len(suffix)

    return text[:limit].rstrip() + suffix


def format_saved_search_alert(
    saved_search_name: str,
    items: list[dict[str, Any]],
) -> str:
    lines: list[str] = [
        "<b>Новые объявления</b>",
        f"Поиск: <code>{_escape_html(_truncate(saved_search_name, 120))}</code>",
        f"Найдено новых: <b>{len(items)}</b>",
        "",
    ]

    if not items:
        lines.append("Новых объявлений нет.")
        return "\n".join(lines)

    for index, item in enumerate(items[:5], start=1):
        header = _format_header(item)
        extra = _format_extra(item)
        source_url = item.get("source_url") or item.get("url")

        lines.append(f"<b>{index}. {_escape_html(header)}</b>")

        if extra:
            lines.append(_escape_html(extra))

        if source_url:
            lines.append(f"<a href=\"{_escape_html(str(source_url))}\">Открыть объявление</a>")

        lines.append("")

    if len(items) > 5:
        lines.append(f"Еще найдено: {len(items) - 5}")

    return _trim_message("\n".join(lines).strip())


def format_subscription_expiry_notice(expires_at: str | None) -> str:
    lines = [
        "<b>Подписка скоро закончится</b>",
        "Продлите доступ, чтобы не потерять уведомления и расширенные лимиты.",
    ]

    if expires_at:
        lines.append(f"Действует до: {_escape_html(expires_at)}")

    return "\n".join(lines)