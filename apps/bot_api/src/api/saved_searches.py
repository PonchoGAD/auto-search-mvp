from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.db.session import get_db
from src.dependencies.auth import verify_internal_api_key
from src.repositories.saved_searches import SavedSearchesRepository
from src.repositories.users import UsersRepository
from src.schemas.common import MessageResponse
from src.schemas.saved_searches import (
    SavedSearchCreateRequest,
    SavedSearchListResponse,
    SavedSearchResponse,
    SavedSearchUpdateRequest,
)


router = APIRouter(
    prefix="/saved-searches",
    tags=["saved-searches"],
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


@router.get("", response_model=SavedSearchListResponse)
def list_saved_searches(
    telegram_user_id: int = Query(...),
    db: Session = Depends(get_db),
) -> SavedSearchListResponse:
    user_id = _require_user_id(db, telegram_user_id)
    repo = SavedSearchesRepository(db)
    items = repo.list_by_user(user_id)

    return SavedSearchListResponse(
        items=[SavedSearchResponse.model_validate(x) for x in items]
    )


@router.post("", response_model=SavedSearchResponse)
def create_saved_search(
    payload: SavedSearchCreateRequest,
    telegram_user_id: int = Query(...),
    db: Session = Depends(get_db),
) -> SavedSearchResponse:
    user_id = _require_user_id(db, telegram_user_id)
    repo = SavedSearchesRepository(db)

    try:
        item = repo.create(user_id=user_id, payload=payload)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Saved search with this name already exists",
        ) from exc

    return SavedSearchResponse.model_validate(item)


@router.patch("/{saved_search_id}", response_model=SavedSearchResponse)
def update_saved_search(
    saved_search_id: int,
    payload: SavedSearchUpdateRequest,
    telegram_user_id: int = Query(...),
    db: Session = Depends(get_db),
) -> SavedSearchResponse:
    user_id = _require_user_id(db, telegram_user_id)
    repo = SavedSearchesRepository(db)

    entity = repo.get_by_user_and_id(user_id=user_id, saved_search_id=saved_search_id)

    if not entity:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Saved search not found",
        )

    try:
        entity = repo.update(entity=entity, payload=payload)
    except ValueError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc
    except IntegrityError as exc:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_409_CONFLICT,
            detail="Saved search with this name already exists",
        ) from exc

    return SavedSearchResponse.model_validate(entity)


@router.delete("/{saved_search_id}", response_model=MessageResponse)
def delete_saved_search(
    saved_search_id: int,
    telegram_user_id: int = Query(...),
    db: Session = Depends(get_db),
) -> MessageResponse:
    user_id = _require_user_id(db, telegram_user_id)
    repo = SavedSearchesRepository(db)

    entity = repo.get_by_user_and_id(user_id=user_id, saved_search_id=saved_search_id)

    if not entity:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Saved search not found",
        )

    repo.delete(entity)
    return MessageResponse(message="Saved search deleted")