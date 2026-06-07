from __future__ import annotations

from dataclasses import dataclass
from math import ceil
from typing import Generic, Sequence, TypeVar


T = TypeVar("T")


@dataclass(slots=True)
class PageSlice(Generic[T]):
    items: list[T]
    page: int
    per_page: int
    total_items: int
    total_pages: int
    has_prev: bool
    has_next: bool
    prev_page: int | None
    next_page: int | None


def normalize_page(page: int | None) -> int:
    if page is None:
        return 1

    try:
        page = int(page)
    except Exception:
        return 1

    if page < 1:
        return 1

    return page


def paginate_items(
    items: Sequence[T],
    page: int = 1,
    per_page: int = 10,
) -> PageSlice[T]:
    safe_page = normalize_page(page)

    try:
        safe_per_page = int(per_page or 10)
    except Exception:
        safe_per_page = 10

    safe_per_page = max(1, safe_per_page)

    total_items = len(items)
    total_pages = max(1, ceil(total_items / safe_per_page)) if total_items > 0 else 1

    if safe_page > total_pages:
        safe_page = total_pages

    start = (safe_page - 1) * safe_per_page
    end = start + safe_per_page

    page_items = list(items[start:end])

    has_prev = safe_page > 1
    has_next = safe_page < total_pages

    return PageSlice(
        items=page_items,
        page=safe_page,
        per_page=safe_per_page,
        total_items=total_items,
        total_pages=total_pages,
        has_prev=has_prev,
        has_next=has_next,
        prev_page=safe_page - 1 if has_prev else None,
        next_page=safe_page + 1 if has_next else None,
    )