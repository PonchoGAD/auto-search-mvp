from __future__ import annotations

from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel
from sqlalchemy.orm import Session

from src.db.models import TelegramChannel
from src.db.session import get_db
from src.dependencies.auth import require_internal_api_key


router = APIRouter(prefix="/internal/admin/tg-channels", tags=["admin-channels"])


class ChannelAddRequest(BaseModel):
    username: str
    display_name: str | None = None
    notes: str | None = None
    added_by_admin_id: int | None = None


def _normalize_username(raw: str) -> str:
    raw = raw.strip()
    if "t.me/" in raw:
        raw = raw.split("t.me/")[-1].strip("/").split("?")[0]
    raw = raw.lstrip("@").strip()
    return raw.lower()


def _channel_dict(ch: TelegramChannel) -> dict:
    return {
        "id": ch.id,
        "username": ch.username,
        "tme_link": f"https://t.me/{ch.username}",
        "display_name": ch.display_name,
        "notes": ch.notes,
        "is_active": ch.is_active,
        "added_by_admin_id": ch.added_by_admin_id,
        "created_at": ch.created_at.isoformat() if ch.created_at else None,
    }


@router.get("")
def list_channels(
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> list[dict]:
    channels = db.query(TelegramChannel).order_by(TelegramChannel.created_at.desc()).all()
    return [_channel_dict(c) for c in channels]


@router.post("", status_code=status.HTTP_201_CREATED)
def add_channel(
    body: ChannelAddRequest,
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> dict:
    username = _normalize_username(body.username)
    if not username:
        raise HTTPException(status_code=422, detail="username is empty after normalization")

    existing = db.query(TelegramChannel).filter(TelegramChannel.username == username).first()
    if existing:
        if not existing.is_active:
            existing.is_active = True
            existing.display_name = body.display_name or existing.display_name
            existing.notes = body.notes or existing.notes
            db.commit()
            db.refresh(existing)
            return {**_channel_dict(existing), "reactivated": True}
        raise HTTPException(status_code=409, detail=f"Channel @{username} already exists")

    ch = TelegramChannel(
        username=username,
        display_name=body.display_name,
        notes=body.notes,
        is_active=True,
        added_by_admin_id=body.added_by_admin_id,
    )
    db.add(ch)
    db.commit()
    db.refresh(ch)
    return _channel_dict(ch)


@router.delete("/{channel_id}", status_code=status.HTTP_200_OK)
def remove_channel(
    channel_id: int,
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> dict:
    ch = db.query(TelegramChannel).filter(TelegramChannel.id == channel_id).first()
    if not ch:
        raise HTTPException(status_code=404, detail="Channel not found")
    username = ch.username
    db.delete(ch)
    db.commit()
    return {"deleted": True, "username": username}


@router.patch("/{channel_id}/toggle", status_code=status.HTTP_200_OK)
def toggle_channel(
    channel_id: int,
    _: str = Depends(require_internal_api_key),
    db: Session = Depends(get_db),
) -> dict:
    ch = db.query(TelegramChannel).filter(TelegramChannel.id == channel_id).first()
    if not ch:
        raise HTTPException(status_code=404, detail="Channel not found")
    ch.is_active = not ch.is_active
    db.commit()
    db.refresh(ch)
    return _channel_dict(ch)
