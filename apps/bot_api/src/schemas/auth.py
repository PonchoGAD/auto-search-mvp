from __future__ import annotations

from datetime import datetime
from typing import Optional

from pydantic import Field

from src.schemas.common import BaseSchema


class TelegramAuthRequest(BaseSchema):
    telegram_user_id: int
    telegram_chat_id: Optional[int] = None

    username: Optional[str] = Field(default=None, max_length=64)
    first_name: Optional[str] = Field(default=None, max_length=128)
    last_name: Optional[str] = Field(default=None, max_length=128)
    language_code: Optional[str] = Field(default=None, max_length=16)


class TokenResponse(BaseSchema):
    access_token: str
    token_type: str = "bearer"
    expires_in: int


class AuthUserResponse(BaseSchema):
    id: int
    telegram_user_id: int
    username: Optional[str] = None
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    language_code: Optional[str] = None

    role: str
    status: str
    is_premium: bool

    created_at: datetime
    updated_at: datetime


class AuthResponse(BaseSchema):
    access_token: str
    token_type: str = "bearer"
    expires_in: int
    user: AuthUserResponse