from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from src.db.session import get_db
from src.dependencies.auth import verify_internal_api_key
from src.repositories.subscriptions import SubscriptionsRepository
from src.repositories.usage_limits import UsageLimitsRepository
from src.repositories.users import UsersRepository
from src.schemas.subscriptions import (
    SubscriptionCreateRequest,
    SubscriptionResponse,
    SubscriptionStatusResponse,
    SubscriptionUpdateRequest,
)


router = APIRouter(
    prefix="/subscriptions",
    tags=["subscriptions"],
    dependencies=[Depends(verify_internal_api_key)],
)


def _require_user_id(db: Session, telegram_user_id: int) -> int:
    user_repo = UsersRepository(db)
    user = user_repo.get_by_telegram_user_id(telegram_user_id)
    if not user:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="User not found",
        )
    return user.id


@router.get("/me", response_model=SubscriptionStatusResponse)
def get_my_subscription(
    telegram_user_id: int = Query(...),
    db: Session = Depends(get_db),
) -> SubscriptionStatusResponse:
    user_id = _require_user_id(db, telegram_user_id)

    subs_repo = SubscriptionsRepository(db)
    limits_repo = UsageLimitsRepository(db)

    subscription = subs_repo.get_active_by_user(user_id)
    limits = limits_repo.get_limits_snapshot(user_id)

    if subscription:
        return SubscriptionStatusResponse(
            is_premium=limits["is_premium"],
            active=True,
            plan=subscription.plan,
            status=subscription.status,
            starts_at=subscription.starts_at,
            expires_at=subscription.expires_at,
            searches_left_today=limits["searches_left_today"],
            saved_searches_left=limits["saved_searches_left"],
            favorites_left=limits["favorites_left"],
        )

    return SubscriptionStatusResponse(
        is_premium=False,
        active=False,
        plan="free",
        status="active",
        starts_at=None,
        expires_at=None,
        searches_left_today=limits["searches_left_today"],
        saved_searches_left=limits["saved_searches_left"],
        favorites_left=limits["favorites_left"],
    )


@router.get("", response_model=list[SubscriptionResponse])
def list_subscriptions(
    telegram_user_id: int = Query(...),
    db: Session = Depends(get_db),
) -> list[SubscriptionResponse]:
    user_id = _require_user_id(db, telegram_user_id)
    repo = SubscriptionsRepository(db)
    items = repo.list_by_user(user_id)
    return [SubscriptionResponse.model_validate(x) for x in items]


@router.post("", response_model=SubscriptionResponse)
def create_subscription(
    payload: SubscriptionCreateRequest,
    db: Session = Depends(get_db),
) -> SubscriptionResponse:
    repo = SubscriptionsRepository(db)
    item = repo.create(payload)
    return SubscriptionResponse.model_validate(item)


@router.patch("/{subscription_id}", response_model=SubscriptionResponse)
def update_subscription(
    subscription_id: int,
    payload: SubscriptionUpdateRequest,
    db: Session = Depends(get_db),
) -> SubscriptionResponse:
    repo = SubscriptionsRepository(db)
    entity = repo.get_by_id(subscription_id)

    if not entity:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Subscription not found",
        )

    entity = repo.update(entity=entity, payload=payload)
    return SubscriptionResponse.model_validate(entity)


@router.post("/{subscription_id}/cancel", response_model=SubscriptionResponse)
def cancel_subscription(
    subscription_id: int,
    db: Session = Depends(get_db),
) -> SubscriptionResponse:
    repo = SubscriptionsRepository(db)
    entity = repo.get_by_id(subscription_id)

    if not entity:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Subscription not found",
        )

    entity = repo.cancel(entity)
    return SubscriptionResponse.model_validate(entity)