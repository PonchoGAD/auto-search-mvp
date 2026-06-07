from __future__ import annotations

from datetime import datetime, timedelta, timezone

import jwt
from fastapi import APIRouter, Depends
from sqlalchemy.orm import Session

from src.config import settings
from src.db.session import get_db
from src.repositories.subscriptions import SubscriptionsRepository
from src.repositories.users import UsersRepository
from src.schemas.auth import AuthResponse, AuthUserResponse, TelegramAuthRequest


router = APIRouter(prefix="/auth", tags=["auth"])


def _build_access_token(user_id: int, telegram_user_id: int) -> tuple[str, int]:
    expires_in = settings.JWT_EXPIRE_MINUTES * 60
    expire_at = datetime.now(timezone.utc) + timedelta(minutes=settings.JWT_EXPIRE_MINUTES)

    payload = {
        "sub": str(user_id),
        "telegram_user_id": telegram_user_id,
        "exp": expire_at,
    }

    token = jwt.encode(
        payload,
        settings.JWT_SECRET,
        algorithm=settings.JWT_ALGORITHM,
    )
    return token, expires_in


@router.post("/telegram", response_model=AuthResponse)
def telegram_auth(
    payload: TelegramAuthRequest,
    db: Session = Depends(get_db),
) -> AuthResponse:
    users_repo = UsersRepository(db)
    subs_repo = SubscriptionsRepository(db)

    user = users_repo.upsert_telegram_user(payload)
    subs_repo.ensure_free_subscription(user.id)

    token, expires_in = _build_access_token(
        user_id=user.id,
        telegram_user_id=user.telegram_user_id,
    )

    return AuthResponse(
        access_token=token,
        token_type="bearer",
        expires_in=expires_in,
        user=AuthUserResponse(
            id=user.id,
            telegram_user_id=user.telegram_user_id,
            username=user.username,
            first_name=user.first_name,
            last_name=user.last_name,
            language_code=user.language_code,
            role=user.role,
            status=user.status,
            is_premium=user.is_premium,
            created_at=user.created_at,
            updated_at=user.updated_at,
        ),
    )