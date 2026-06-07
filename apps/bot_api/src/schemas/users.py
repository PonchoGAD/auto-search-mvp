from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import Field

from src.schemas.common import BaseSchema


class TelegramUserUpsertRequest(BaseSchema):
    telegram_user_id: int
    telegram_chat_id: Optional[int] = None

    username: Optional[str] = Field(default=None, max_length=64)
    first_name: Optional[str] = Field(default=None, max_length=128)
    last_name: Optional[str] = Field(default=None, max_length=128)
    language_code: Optional[str] = Field(default=None, max_length=16)


class UserResponse(BaseSchema):
    id: int

    telegram_user_id: int
    telegram_chat_id: Optional[int] = None

    username: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    language_code: Optional[str] = None

    role: str
    status: str
    is_premium: bool

    last_seen_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime


class UserProfileResponse(BaseSchema):
    id: int
    telegram_user_id: int
    username: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    language_code: Optional[str] = None

    role: str
    status: str
    is_premium: bool

    favorites_count: int = 0
    saved_searches_count: int = 0

    active_subscription_plan: Optional[str] = None
    active_subscription_status: Optional[str] = None
    subscription_expires_at: Optional[datetime] = None

    last_seen_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime