from __future__ import annotations

from typing import Any

from src.schemas.search import SearchResultItem


class ListingResult(SearchResultItem):
    def to_telegram_dict(self) -> dict[str, Any]:
        return self.model_dump()

    def best_photo_url(self) -> str | None:
        if self.photos:
            return self.photos[0]

        if self.image_url:
            return self.image_url

        return None
