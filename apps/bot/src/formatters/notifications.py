from __future__ import annotations

from typing import Any

from src.utils.text import escape_html, format_price, safe_text


def format_saved_search_alert(
    saved_search_name: str,
    items: list[dict[str, Any]],
) -> str:
    lines: list[str] = [
        f"<b>Новое по поиску:</b> {escape_html(saved_search_name)}",
    ]

    if not items:
        lines.append("Новых объявлений нет.")
        return "\n".join(lines)

    for item in items[:5]:
        title = safe_text(item.get("title"), "")
        brand = safe_text(item.get("brand"), "")
        model = safe_text(item.get("model"), "")
        header = title or " ".join(x for x in [brand, model] if x).strip() or "Объявление"

        price = format_price(item.get("price"), item.get("currency"))
        year = item.get("year")
        mileage = item.get("mileage")
        source_url = item.get("source_url")

        line = f"• {escape_html(header)}"
        extra: list[str] = []

        if year:
            extra.append(str(year))
        if mileage is not None:
            extra.append(f"{mileage:,}".replace(",", " ") + " км")
        extra.append(price)

        if extra:
            line += " — " + escape_html(" • ".join(extra))

        lines.append(line)

        if source_url:
            lines.append(f"  <a href=\"{escape_html(source_url)}\">Открыть</a>")

    return "\n".join(lines)


def format_subscription_expiry_notice(expires_at: str | None) -> str:
    lines = [
        "<b>Подписка скоро закончится</b>",
        "Продлите доступ, чтобы не потерять уведомления и расширенные лимиты.",
    ]

    if expires_at:
        lines.append(f"Действует до: {escape_html(expires_at)}")

    return "\n".join(lines)