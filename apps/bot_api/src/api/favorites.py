from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from src.db.session import get_db
from src.dependencies.auth import verify_internal_api_key
from src.repositories.favorites import FavoritesRepository
from src.repositories.users import UsersRepository
from src.schemas.common import MessageResponse
from src.schemas.favorites import (
    FavoriteCreateRequest,
    FavoriteListResponse,
    FavoriteResponse,
)
from src.schemas.search import SearchResultItem


router = APIRouter(
    prefix="/favorites",
    tags=["favorites"],
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


@router.get("", response_model=FavoriteListResponse)
def list_favorites(
    telegram_user_id: int = Query(...),
    db: Session = Depends(get_db),
) -> FavoriteListResponse:
    user_id = _require_user_id(db, telegram_user_id)
    repo = FavoritesRepository(db)
    items = repo.list_by_user(user_id)

    return FavoriteListResponse(
        items=[FavoriteResponse.model_validate(x) for x in items]
    )


@router.post("", response_model=FavoriteResponse)
def create_favorite(
    payload: FavoriteCreateRequest,
    telegram_user_id: int = Query(...),
    db: Session = Depends(get_db),
) -> FavoriteResponse:
    user_id = _require_user_id(db, telegram_user_id)
    repo = FavoritesRepository(db)

    try:
        item = repo.create(user_id=user_id, payload=payload)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc

    return FavoriteResponse.model_validate(item)


@router.post("/from-search", response_model=FavoriteResponse)
def create_favorite_from_search(
    item: SearchResultItem,
    telegram_user_id: int = Query(...),
    db: Session = Depends(get_db),
) -> FavoriteResponse:
    user_id = _require_user_id(db, telegram_user_id)
    repo = FavoritesRepository(db)

    try:
        favorite = repo.create_from_search_item(user_id=user_id, item=item)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc

    return FavoriteResponse.model_validate(favorite)


@router.delete("/{listing_id}", response_model=MessageResponse)
def delete_favorite(
    listing_id: str,
    telegram_user_id: int = Query(...),
    db: Session = Depends(get_db),
) -> MessageResponse:
    user_id = _require_user_id(db, telegram_user_id)
    repo = FavoritesRepository(db)

    deleted = repo.delete_by_user_and_listing(
        user_id=user_id,
        listing_id=listing_id,
    )

    if not deleted:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Favorite not found",
        )

    return MessageResponse(message="Favorite deleted")