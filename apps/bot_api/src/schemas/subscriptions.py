from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import Field

from src.schemas.common import BaseSchema


class SubscriptionResponse(BaseSchema):
    id: int
    user_id: int

    plan: str
    status: str

    starts_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    canceled_at: Optional[datetime] = None

    meta: Optional[dict] = None

    created_at: datetime
    updated_at: datetime


class SubscriptionStatusResponse(BaseSchema):
    is_premium: bool = False
    active: bool = False

    plan: str = "free"
    status: str = "active"

    starts_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None

    searches_left_today: Optional[int] = None
    saved_searches_left: Optional[int] = None
    favorites_left: Optional[int] = None


class SubscriptionCreateRequest(BaseSchema):
    user_id: int
    plan: str = Field(min_length=1, max_length=16)
    status: str = Field(default="active", min_length=1, max_length=16)

    starts_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    meta: Optional[dict] = None


class SubscriptionUpdateRequest(BaseSchema):
    plan: Optional[str] = Field(default=None, min_length=1, max_length=16)
    status: Optional[str] = Field(default=None, min_length=1, max_length=16)

    starts_at: Optional[datetime] = None
    expires_at: Optional[datetime] = None
    canceled_at: Optional[datetime] = None
    meta: Optional[dict] = None