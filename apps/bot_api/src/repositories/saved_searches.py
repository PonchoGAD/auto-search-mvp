from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.db.enums import SavedSearchStatus
from src.db.models import SavedSearch
from src.schemas.saved_searches import (
    SavedSearchCreateRequest,
    SavedSearchUpdateRequest,
)


class SavedSearchesRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def list_by_user(self, user_id: int) -> list[SavedSearch]:
        stmt = (
            select(SavedSearch)
            .where(SavedSearch.user_id == user_id)
            .order_by(SavedSearch.created_at.desc(), SavedSearch.id.desc())
        )
        return list(self.db.execute(stmt).scalars().all())

    def list_active_alerts(self, limit: int = 100) -> list[SavedSearch]:
        stmt = (
            select(SavedSearch)
            .where(
                SavedSearch.status == SavedSearchStatus.ACTIVE.value,
                SavedSearch.is_alert_enabled.is_(True),
            )
            .order_by(
                SavedSearch.last_checked_at.asc().nullsfirst(),
                SavedSearch.id.asc(),
            )
            .limit(limit)
        )
        return list(self.db.execute(stmt).scalars().all())

    def list_alert_ready(self, limit: int = 100) -> list[SavedSearch]:
        stmt = (
            select(SavedSearch)
            .where(
                SavedSearch.status == SavedSearchStatus.ACTIVE.value,
                SavedSearch.is_alert_enabled.is_(True),
            )
            .order_by(
                SavedSearch.last_checked_at.asc().nullsfirst(),
                SavedSearch.id.asc(),
            )
            .limit(limit)
        )
        return list(self.db.execute(stmt).scalars().all())

    def get_by_id(self, saved_search_id: int) -> Optional[SavedSearch]:
        stmt = select(SavedSearch).where(SavedSearch.id == saved_search_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def get_by_user_and_id(self, user_id: int, saved_search_id: int) -> Optional[SavedSearch]:
        stmt = select(SavedSearch).where(
            SavedSearch.user_id == user_id,
            SavedSearch.id == saved_search_id,
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def get_by_user_and_name(self, user_id: int, name: str) -> Optional[SavedSearch]:
        stmt = select(SavedSearch).where(
            SavedSearch.user_id == user_id,
            SavedSearch.name == name.strip(),
        )
        return self.db.execute(stmt).scalar_one_or_none()

    def create(self, user_id: int, payload: SavedSearchCreateRequest) -> SavedSearch:
        name = payload.name.strip()
        raw_query = payload.raw_query.strip()

        if not name:
            raise ValueError("saved search name is required")

        if not raw_query:
            raise ValueError("raw_query is required")

        query_payload = dict(payload.query_payload or {})

        if "query" not in query_payload:
            query_payload["query"] = raw_query

        if "page" not in query_payload:
            query_payload["page"] = 1

        entity = SavedSearch(
            user_id=user_id,
            name=name,
            raw_query=raw_query,
            query_payload=query_payload,
            is_alert_enabled=payload.is_alert_enabled,
            status=SavedSearchStatus.ACTIVE.value,
            last_seen_listing_id=None,
            last_checked_at=None,
        )

        try:
            self.db.add(entity)
            self.db.commit()
            self.db.refresh(entity)
            return entity
        except IntegrityError:
            self.db.rollback()
            existing = self.get_by_user_and_name(user_id, name)
            if existing:
                return existing
            raise

    def update(self, entity: SavedSearch, payload: SavedSearchUpdateRequest) -> SavedSearch:
        if payload.name is not None:
            clean_name = payload.name.strip()
            if not clean_name:
                raise ValueError("saved search name cannot be empty")
            entity.name = clean_name

        if payload.raw_query is not None:
            clean_query = payload.raw_query.strip()
            if not clean_query:
                raise ValueError("raw_query cannot be empty")
            entity.raw_query = clean_query

        if payload.query_payload is not None:
            entity.query_payload = dict(payload.query_payload or {})

        if payload.status is not None:
            if payload.status not in {
                SavedSearchStatus.ACTIVE.value,
                SavedSearchStatus.PAUSED.value,
                SavedSearchStatus.DISABLED.value,
            }:
                raise ValueError(f"invalid saved search status: {payload.status}")
            entity.status = payload.status

        if payload.is_alert_enabled is not None:
            entity.is_alert_enabled = payload.is_alert_enabled

        if payload.last_seen_listing_id is not None:
            entity.last_seen_listing_id = str(payload.last_seen_listing_id).strip() or None

        self.db.add(entity)
        self.db.commit()
        self.db.refresh(entity)

        return entity

    def mark_checked(
        self,
        entity: SavedSearch,
        last_seen_listing_id: Optional[str] = None,
    ) -> SavedSearch:
        entity.last_checked_at = datetime.now(timezone.utc)

        clean_last_seen = str(last_seen_listing_id or "").strip()
        if clean_last_seen:
            entity.last_seen_listing_id = clean_last_seen

        self.db.add(entity)
        self.db.commit()
        self.db.refresh(entity)

        return entity

    def pause(self, entity: SavedSearch) -> SavedSearch:
        entity.is_alert_enabled = False
        entity.status = SavedSearchStatus.PAUSED.value

        self.db.add(entity)
        self.db.commit()
        self.db.refresh(entity)

        return entity

    def resume(self, entity: SavedSearch) -> SavedSearch:
        entity.is_alert_enabled = True
        entity.status = SavedSearchStatus.ACTIVE.value

        self.db.add(entity)
        self.db.commit()
        self.db.refresh(entity)

        return entity

    def disable(self, entity: SavedSearch) -> SavedSearch:
        entity.is_alert_enabled = False
        entity.status = SavedSearchStatus.DISABLED.value

        self.db.add(entity)
        self.db.commit()
        self.db.refresh(entity)

        return entity

    def delete(self, entity: SavedSearch) -> bool:
        self.db.delete(entity)
        self.db.commit()
        return True

    def count_total(self) -> int:
        stmt = select(func.count(SavedSearch.id))
        return int(self.db.execute(stmt).scalar() or 0)

    def count_active(self) -> int:
        stmt = select(func.count(SavedSearch.id)).where(
            SavedSearch.status == SavedSearchStatus.ACTIVE.value,
        )
        return int(self.db.execute(stmt).scalar() or 0)

    def count_paused(self) -> int:
        stmt = select(func.count(SavedSearch.id)).where(
            SavedSearch.status == SavedSearchStatus.PAUSED.value,
        )
        return int(self.db.execute(stmt).scalar() or 0)

    def count_disabled(self) -> int:
        stmt = select(func.count(SavedSearch.id)).where(
            SavedSearch.status == SavedSearchStatus.DISABLED.value,
        )
        return int(self.db.execute(stmt).scalar() or 0)

    def count_alert_enabled(self) -> int:
        stmt = select(func.count(SavedSearch.id)).where(
            SavedSearch.is_alert_enabled.is_(True),
        )
        return int(self.db.execute(stmt).scalar() or 0)

    def count_alert_ready(self) -> int:
        stmt = select(func.count(SavedSearch.id)).where(
            SavedSearch.status == SavedSearchStatus.ACTIVE.value,
            SavedSearch.is_alert_enabled.is_(True),
        )
        return int(self.db.execute(stmt).scalar() or 0)

    def latest_saved_searches(self, limit: int = 50) -> list[dict]:
        stmt = (
            select(SavedSearch)
            .order_by(SavedSearch.created_at.desc(), SavedSearch.id.desc())
            .limit(limit)
        )

        rows = list(self.db.execute(stmt).scalars().all())
        return [self._to_dict(row) for row in rows]

    def saved_searches_stats(self) -> dict:
        return {
            "total": self.count_total(),
            "active": self.count_active(),
            "paused": self.count_paused(),
            "disabled": self.count_disabled(),
            "alert_enabled": self.count_alert_enabled(),
            "alert_ready": self.count_alert_ready(),
        }

    def _to_dict(self, row: SavedSearch) -> dict:
        return {
            "id": row.id,
            "user_id": row.user_id,
            "name": row.name,
            "raw_query": row.raw_query,
            "query_payload": row.query_payload,
            "last_seen_listing_id": row.last_seen_listing_id,
            "last_checked_at": row.last_checked_at,
            "status": row.status,
            "is_alert_enabled": row.is_alert_enabled,
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }