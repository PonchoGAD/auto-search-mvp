from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from src.db.session import get_db
from src.dependencies.auth import verify_internal_api_key
from src.repositories.users import UsersRepository
from src.schemas.users import (
    TelegramUserUpsertRequest,
    UserProfileResponse,
    UserResponse,
)


router = APIRouter(
    prefix="/users",
    tags=["users"],
    dependencies=[Depends(verify_internal_api_key)],
)


@router.post("/telegram/upsert", response_model=UserResponse)
def upsert_telegram_user(
    payload: TelegramUserUpsertRequest,
    db: Session = Depends(get_db),
) -> UserResponse:
    repo = UsersRepository(db)
    user = repo.upsert_telegram_user(payload)
    return UserResponse.model_validate(user)


@router.get("/me", response_model=UserProfileResponse)
def get_me(
    telegram_user_id: int = Query(...),
    db: Session = Depends(get_db),
) -> UserProfileResponse:
    repo = UsersRepository(db)
    user = repo.get_by_telegram_user_id(telegram_user_id)

    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )

    favorites_count = repo.count_favorites(user.id)
    saved_searches_count = repo.count_saved_searches(user.id)
    subscription = repo.get_active_subscription(user.id)

    return UserProfileResponse(
        id=user.id,
        telegram_user_id=user.telegram_user_id,
        username=user.username,
        first_name=user.first_name,
        last_name=user.last_name,
        language_code=user.language_code,
        role=user.role,
        status=user.status,
        is_premium=user.is_premium,
        favorites_count=favorites_count,
        saved_searches_count=saved_searches_count,
        active_subscription_plan=subscription.plan if subscription else None,
        active_subscription_status=subscription.status if subscription else None,
        subscription_expires_at=subscription.expires_at if subscription else None,
        last_seen_at=user.last_seen_at,
        created_at=user.created_at,
        updated_at=user.updated_at,
    )