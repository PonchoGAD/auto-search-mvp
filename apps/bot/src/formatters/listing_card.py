from __future__ import annotations

from typing import Any

from src.config import settings
from src.utils.text import (
    escape_html,
    format_fuel,
    format_mileage,
    format_price,
    format_year,
    join_nonempty,
    safe_text,
    truncate_text,
)


def _format_created_at(value: Any) -> str | None:
    if not value:
        return None

    text = str(value).strip()
    if not text:
        return None

    if "T" in text:
        text = text.split("T", 1)[0]

    return text


def _format_score(value: Any) -> str | None:
    if value is None:
        return None

    try:
        score = float(value)
    except Exception:
        return None

    if score <= 1:
        return f"{score:.3f}"

    return f"{score:.1f}"


def _format_location(item: dict[str, Any]) -> str:
    city = safe_text(item.get("city"), "")
    region = safe_text(item.get("region"), "")

    if city and region and city.lower() != region.lower():
        return f"{city}, {region}"

    return city or region or "Регион не указан"


def _normalize_listing_item(item: Any) -> dict[str, Any]:
    if hasattr(item, "model_dump"):
        item = item.model_dump()

    if isinstance(item, dict):
        return item

    try:
        return dict(item)
    except Exception:
        return {}


def get_listing_media_url(item: Any) -> str | None:
    listing = _normalize_listing_item(item)
    photos = listing.get("photos") or []

    if isinstance(photos, list) and photos:
        return str(photos[0]).strip() or None

    image_url = str(listing.get("image_url") or "").strip()
    return image_url or None


def format_listing_card(item: dict[str, Any], index: int | None = None) -> str:
    item = _normalize_listing_item(item)
    brand = safe_text(item.get("brand"), "")
    model = safe_text(item.get("model"), "")
    title = safe_text(item.get("title"), "").strip()

    header_title = title or " ".join(x for x in [brand, model] if x).strip() or "Объявление"

    year = format_year(item.get("year"))
    mileage = format_mileage(item.get("mileage"))
    price = format_price(item.get("price"), item.get("currency"))
    fuel = format_fuel(item.get("fuel"))
    location = _format_location(item)
    color = safe_text(item.get("color"), "")
    condition = safe_text(item.get("condition"), "")
    paint_condition = safe_text(item.get("paint_condition"), "")
    source_name = safe_text(item.get("source_name"), "Источник не указан")
    source_url = item.get("source_url")
    created_at = _format_created_at(item.get("created_at"))
    why_match = truncate_text(item.get("why_match"), max_len=160)
    score = _format_score(item.get("score"))

    lines: list[str] = []

    title_line = f"{index}. {header_title}" if index is not None else header_title
    lines.append(f"<b>{escape_html(title_line)}</b>")

    meta_line = " • ".join(
        x
        for x in [
            year if year != "Год не указан" else None,
            fuel if fuel != "Топливо не указано" else None,
            location if location != "Регион не указан" else None,
        ]
        if x
    )

    if meta_line:
        lines.append(escape_html(meta_line))

    if item.get("price") is not None:
        lines.append(f"Цена: <b>{escape_html(price)}</b>")
    if item.get("mileage") is not None:
        lines.append(f"Пробег: {escape_html(mileage)}")

    if color:
        lines.append(f"Цвет: {escape_html(color)}")

    if condition:
        lines.append(f"Состояние: {escape_html(condition)}")

    if paint_condition:
        lines.append(f"Окрас: {escape_html(paint_condition)}")

    lines.append(f"Источник: {escape_html(source_name)}")

    if created_at:
        lines.append(f"Дата: {escape_html(created_at)}")

    if bool(getattr(settings, "DEBUG", False)) and score:
        lines.append(f"Релевантность: {escape_html(score)}")

    if why_match:
        lines.append(f"Почему найдено: {escape_html(why_match)}")

    if source_url:
        lines.append(f"<a href=\"{escape_html(source_url)}\">Открыть объявление</a>")

    return join_nonempty(lines, sep="\n")


def format_listings_page(
    items: list[dict[str, Any]],
    page: int,
    total_pages: int,
    total_items: int | None = None,
) -> str:
    safe_page = max(1, int(page or 1))
    safe_total_pages = max(1, int(total_pages or 1))

    lines: list[str] = [
        "<b>Результаты поиска</b>",
        f"Страница {safe_page} из {safe_total_pages}",
    ]

    if total_items is not None:
        lines.append(f"Всего: {total_items}")

    if not items:
        lines.append("")
        lines.append("Ничего не найдено. Попробуйте изменить запрос.")
        lines.append("")
        lines.append("Примеры:")
        lines.append("<code>BMW до 3 млн дизель</code>")
        lines.append("<code>Toyota Camry 2018 бензин</code>")
        return "\n".join(lines)

    for idx, item in enumerate(items, start=1):
        lines.append("")
        lines.append(format_listing_card(item=item, index=idx))

    return "\n".join(lines)