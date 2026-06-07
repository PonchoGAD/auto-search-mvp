from __future__ import annotations

import hashlib
from datetime import datetime
from typing import Any, Optional

from pydantic import Field, field_validator

from src.schemas.common import BaseSchema, PaginationMeta


def build_listing_id(
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
    digest = hashlib.sha1(raw.encode("utf-8")).hexdigest()
    return digest[:24]


class SearchRequest(BaseSchema):
    query: str = Field(min_length=1, max_length=1000)
    page: int = Field(default=1, ge=1)
    limit: int = Field(default=10, ge=1, le=50)
    include_answer: bool = False


class SearchResultItem(BaseSchema):
    listing_id: Optional[str] = None

    title: Optional[str] = None
    brand: Optional[str] = None
    model: Optional[str] = None
    year: Optional[int] = None
    mileage: Optional[int] = None
    price: Optional[int] = None
    currency: Optional[str] = "RUB"

    fuel: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None
    color: Optional[str] = None
    condition: Optional[str] = None
    paint_condition: Optional[str] = None

    score: Optional[float] = 0.0
    why_match: Optional[str] = None

    source_url: Optional[str] = None
    source_name: Optional[str] = None
    image_url: Optional[str] = None
    photos: Optional[list[str]] = None

    created_at: Optional[datetime | str] = None
    created_at_ts: Optional[int] = None

    score_breakdown: Optional[dict[str, float]] = None
    raw_payload: Optional[dict[str, Any]] = None

    @field_validator("listing_id", mode="before")
    @classmethod
    def normalize_listing_id(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = str(value).strip()
        return value or None

    @field_validator("city", "region", mode="before")
    @classmethod
    def normalize_location_text(cls, value: Optional[str]) -> Optional[str]:
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

    def ensure_listing_id(self) -> "SearchResultItem":
        if not self.listing_id:
            self.listing_id = build_listing_id(
                source_url=self.source_url,
                brand=self.brand,
                model=self.model,
                year=self.year,
                price=self.price,
                mileage=self.mileage,
            )
        return self


class SearchDebugInfo(BaseSchema):
    latency_ms: int = 0
    vector_hits: int = 0
    final_results: int = 0
    query_language: str = "ru"
    empty_result: bool = False


class SearchResponse(BaseSchema):
    structured_query: dict[str, Any] = Field(default_factory=dict, alias="structuredQuery")
    results: list[SearchResultItem] = Field(default_factory=list)
    answer: Optional[Any] = None
    debug: SearchDebugInfo = Field(default_factory=SearchDebugInfo)
    pagination: PaginationMeta = Field(default_factory=PaginationMeta)


class ListingDetailsResponse(BaseSchema):
    listing_id: str

    title: Optional[str] = None
    brand: Optional[str] = None
    model: Optional[str] = None
    year: Optional[int] = None
    mileage: Optional[int] = None
    price: Optional[int] = None
    currency: Optional[str] = "RUB"

    fuel: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None
    color: Optional[str] = None
    condition: Optional[str] = None
    paint_condition: Optional[str] = None

    source_url: Optional[str] = None
    source_name: Optional[str] = None
    image_url: Optional[str] = None
    photos: Optional[list[str]] = None

    why_match: Optional[str] = None
    score: Optional[float] = None
    score_breakdown: Optional[dict[str, float]] = None

    created_at: Optional[datetime | str] = None
    created_at_ts: Optional[int] = None
    raw_payload: Optional[dict[str, Any]] = None

    @field_validator("city", "region", mode="before")
    @classmethod
    def normalize_location_text(cls, value: Optional[str]) -> Optional[str]:
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