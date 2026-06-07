from __future__ import annotations

import hashlib
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field, field_validator


def _build_listing_id(
    source_url: Optional[str],
    brand: Optional[str],
    model: Optional[str],
    year: Optional[int],
    price: Optional[int],
    mileage: Optional[int],
) -> str:
    raw = "|".join(
        [
            str(source_url or "").strip().lower(),
            str(brand or "").strip().lower(),
            str(model or "").strip().lower(),
            str(year or ""),
            str(price or ""),
            str(mileage or ""),
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:24]


class ListingResult(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        extra="ignore",
    )

    listing_id: Optional[str] = None
    title: Optional[str] = None
    brand: Optional[str] = None
    model: Optional[str] = None
    year: Optional[int] = None
    mileage: Optional[int] = None
    price: Optional[int] = None
    currency: Optional[str] = Field(default="RUB")
    fuel: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None
    color: Optional[str] = None
    condition: Optional[str] = None
    paint_condition: Optional[str] = None
    score: Optional[float] = Field(default=0.0)
    why_match: Optional[str] = None
    source_url: Optional[str] = None
    source_name: Optional[str] = None
    image_url: Optional[str] = None
    photos: Optional[list[str]] = None
    created_at: Optional[str] = None
    created_at_ts: Optional[int] = None
    score_breakdown: Optional[dict[str, float]] = None
    raw_payload: Optional[dict[str, Any]] = None

    @field_validator("listing_id", mode="before")
    @classmethod
    def normalize_listing_id(cls, value: Any) -> Optional[str]:
        if value is None:
            return None
        value = str(value).strip()
        return value or None

    @field_validator("photos", mode="before")
    @classmethod
    def normalize_photos(cls, value: Any) -> Optional[list[str]]:
        if value is None:
            return None

        if isinstance(value, list):
            photos = [str(x).strip() for x in value if str(x).strip()]
            return photos or None

        if isinstance(value, str) and value.strip():
            return [value.strip()]

        return None

    def ensure_listing_id(self) -> "ListingResult":
        if not self.listing_id:
            self.listing_id = _build_listing_id(
                source_url=self.source_url,
                brand=self.brand,
                model=self.model,
                year=self.year,
                price=self.price,
                mileage=self.mileage,
            )

        return self
