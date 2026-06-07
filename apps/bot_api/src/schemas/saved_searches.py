from __future__ import annotations

from datetime import datetime
from typing import Any, Optional

from pydantic import Field

from src.schemas.common import BaseSchema


class SavedSearchCreateRequest(BaseSchema):
    name: str = Field(min_length=1, max_length=255)
    raw_query: str = Field(min_length=1, max_length=1000)
    query_payload: dict[str, Any] = Field(default_factory=dict)
    is_alert_enabled: bool = True


class SavedSearchUpdateRequest(BaseSchema):
    name: Optional[str] = Field(default=None, min_length=1, max_length=255)
    raw_query: Optional[str] = Field(default=None, min_length=1, max_length=1000)
    query_payload: Optional[dict[str, Any]] = None
    status: Optional[str] = Field(default=None, max_length=16)
    is_alert_enabled: Optional[bool] = None
    last_seen_listing_id: Optional[str] = Field(default=None, max_length=128)


class SavedSearchResponse(BaseSchema):
    id: int
    user_id: int

    name: str
    raw_query: str
    query_payload: dict[str, Any]

    status: str
    is_alert_enabled: bool

    last_seen_listing_id: Optional[str] = None
    last_checked_at: Optional[datetime] = None

    created_at: datetime
    updated_at: datetime


class SavedSearchListResponse(BaseSchema):
    items: list[SavedSearchResponse]