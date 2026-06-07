from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import Field, field_validator

from src.schemas.common import BaseSchema
from src.schemas.search import SearchResultItem


class FavoriteCreateRequest(BaseSchema):
    listing_id: Optional[str] = Field(default=None, max_length=128)

    source_url: Optional[str] = None
    source_name: Optional[str] = Field(default=None, max_length=64)

    title: Optional[str] = Field(default=None, max_length=512)
    brand: Optional[str] = Field(default=None, max_length=64)
    model: Optional[str] = Field(default=None, max_length=128)
    year: Optional[int] = None
    mileage: Optional[int] = None
    price: Optional[int] = None
    currency: Optional[str] = Field(default="RUB", max_length=8)
    fuel: Optional[str] = Field(default=None, max_length=32)
    region: Optional[str] = Field(default=None, max_length=128)
    paint_condition: Optional[str] = Field(default=None, max_length=64)
    image_url: Optional[str] = None
    photos: Optional[list[str]] = None

    payload: Optional[dict] = None
    source_type: str = Field(default="search", max_length=16)

    @field_validator("listing_id", mode="before")
    @classmethod
    def normalize_listing_id(cls, value: Optional[str]) -> Optional[str]:
        if value is None:
            return None
        value = str(value).strip()
        return value or None

    @field_validator("photos", mode="before")
    @classmethod
    def normalize_photos(cls, value):
        if value is None:
            return None

        if isinstance(value, list):
            photos = [str(x).strip() for x in value if str(x).strip()]
            return photos or None

        if isinstance(value, str) and value.strip():
            return [value.strip()]

        return None


class FavoriteResponse(BaseSchema):
    id: int
    user_id: int

    listing_id: str
    source_url: Optional[str] = None
    source_name: Optional[str] = None

    title: Optional[str] = None
    brand: Optional[str] = None
    model: Optional[str] = None
    year: Optional[int] = None
    mileage: Optional[int] = None
    price: Optional[int] = None
    currency: Optional[str] = None
    fuel: Optional[str] = None
    region: Optional[str] = None
    paint_condition: Optional[str] = None
    image_url: Optional[str] = None
    photos: Optional[list[str]] = None

    payload: Optional[dict] = None
    source_type: str

    created_at: datetime
    updated_at: datetime


class FavoriteListResponse(BaseSchema):
    items: list[FavoriteResponse]


class FavoriteFromSearchRequest(BaseSchema):
    item: SearchResultItem