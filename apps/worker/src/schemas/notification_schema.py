from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, ConfigDict

from src.schemas.listing import ListingResult


class SavedSearchAlertPayload(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        extra="ignore",
    )

    saved_search_id: int
    saved_search_name: str
    items: list[ListingResult]
    metadata: Optional[dict[str, Any]] = None
