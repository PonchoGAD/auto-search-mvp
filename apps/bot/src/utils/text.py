from __future__ import annotations

import html
from typing import Iterable


def escape_html(value: str | None) -> str:
    if value is None:
        return ""
    return html.escape(str(value), quote=False)


def compact_whitespace(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(str(value).split()).strip()


def safe_text(value: str | None, fallback: str = "—") -> str:
    text = compact_whitespace(value)
    return text or fallback


def truncate_text(value: str | None, max_len: int = 300, suffix: str = "...") -> str:
    text = compact_whitespace(value)
    if len(text) <= max_len:
        return text
    trimmed = text[: max_len - len(suffix)].rstrip()
    return f"{trimmed}{suffix}"


def join_nonempty(parts: Iterable[str | None], sep: str = "\n") -> str:
    values = [compact_whitespace(part) for part in parts]
    values = [value for value in values if value]
    return sep.join(values)


def format_price(price: int | None, currency: str | None = "RUB") -> str:
    if price is None:
        return "Цена не указана"

    value = f"{price:,}".replace(",", " ")

    if currency == "RUB":
        return f"{value} ₽"

    if currency:
        return f"{value} {currency}"

    return value


def format_mileage(mileage: int | None) -> str:
    if mileage is None:
        return "Пробег не указан"
    return f"{mileage:,}".replace(",", " ") + " км"


def format_year(year: int | None) -> str:
    if year is None:
        return "Год не указан"
    return str(year)


def format_fuel(fuel: str | None) -> str:
    mapping = {
        "petrol": "Бензин",
        "diesel": "Дизель",
        "hybrid": "Гибрид",
        "electric": "Электро",
        "gas": "Газ",
        "gas_petrol": "Газ/бензин",
    }
    if not fuel:
        return "Топливо не указано"
    return mapping.get(fuel, fuel)


def format_bool(value: bool) -> str:
    return "Да" if value else "Нет"