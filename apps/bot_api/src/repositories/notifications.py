from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Session

from src.db.enums import NotificationStatus
from src.db.models import NotificationLog


class NotificationsRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def create(
        self,
        user_id: int,
        type: str,
        payload: dict | None = None,
        dedup_key: str | None = None,
        status: str = NotificationStatus.PENDING.value,
    ) -> NotificationLog:
        entity = NotificationLog(
            user_id=user_id,
            type=type,
            status=status,
            dedup_key=dedup_key,
            payload=payload or {},
        )

        self.db.add(entity)
        self.db.commit()
        self.db.refresh(entity)
        return entity

    def safe_create(
        self,
        user_id: int,
        type: str,
        payload: dict | None = None,
        dedup_key: str | None = None,
        status: str = NotificationStatus.PENDING.value,
    ) -> tuple[NotificationLog, bool]:
        if dedup_key:
            existing = self.get_by_dedup_key(dedup_key)
            if existing:
                return existing, False

        entity = NotificationLog(
            user_id=user_id,
            type=type,
            status=status,
            dedup_key=dedup_key,
            payload=payload or {},
        )

        try:
            self.db.add(entity)
            self.db.commit()
            self.db.refresh(entity)
            return entity, True
        except IntegrityError:
            self.db.rollback()

            if dedup_key:
                existing = self.get_by_dedup_key(dedup_key)
                if existing:
                    return existing, False

            raise

    def list_pending(self, limit: int = 100) -> list[NotificationLog]:
        stmt = (
            select(NotificationLog)
            .where(NotificationLog.status == NotificationStatus.PENDING.value)
            .order_by(NotificationLog.created_at.asc(), NotificationLog.id.asc())
            .limit(limit)
        )

        return list(self.db.execute(stmt).scalars().all())

    def list_by_user(self, user_id: int, limit: int = 100) -> list[NotificationLog]:
        stmt = (
            select(NotificationLog)
            .where(NotificationLog.user_id == user_id)
            .order_by(NotificationLog.created_at.desc(), NotificationLog.id.desc())
            .limit(limit)
        )

        return list(self.db.execute(stmt).scalars().all())

    def get_by_id(self, notification_id: int) -> Optional[NotificationLog]:
        stmt = select(NotificationLog).where(NotificationLog.id == notification_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def get_by_dedup_key(self, dedup_key: str) -> Optional[NotificationLog]:
        stmt = select(NotificationLog).where(NotificationLog.dedup_key == dedup_key)
        return self.db.execute(stmt).scalar_one_or_none()

    def exists_by_dedup_key(self, dedup_key: str) -> bool:
        return self.get_by_dedup_key(dedup_key) is not None

    def mark_sent(self, entity: NotificationLog) -> NotificationLog:
        entity.status = NotificationStatus.SENT.value
        entity.sent_at = datetime.now(timezone.utc)
        entity.error_message = None

        self.db.add(entity)
        self.db.commit()
        self.db.refresh(entity)

        return entity

    def mark_sent_by_id(self, notification_id: int) -> Optional[NotificationLog]:
        entity = self.get_by_id(notification_id)

        if not entity:
            return None

        return self.mark_sent(entity)

    def mark_failed(
        self,
        entity: NotificationLog,
        error_message: str | None = None,
    ) -> NotificationLog:
        entity.status = NotificationStatus.FAILED.value
        entity.error_message = error_message

        self.db.add(entity)
        self.db.commit()
        self.db.refresh(entity)

        return entity

    def mark_failed_by_id(
        self,
        notification_id: int,
        error_message: str | None = None,
    ) -> Optional[NotificationLog]:
        entity = self.get_by_id(notification_id)

        if not entity:
            return None

        return self.mark_failed(entity, error_message=error_message)

    def mark_pending(self, entity: NotificationLog) -> NotificationLog:
        entity.status = NotificationStatus.PENDING.value
        entity.error_message = None

        self.db.add(entity)
        self.db.commit()
        self.db.refresh(entity)

        return entity

    def latest_logs(self, limit: int = 50) -> list[dict]:
        stmt = (
            select(NotificationLog)
            .order_by(NotificationLog.created_at.desc(), NotificationLog.id.desc())
            .limit(limit)
        )

        rows = list(self.db.execute(stmt).scalars().all())
        return [self._to_dict(row) for row in rows]

    def failed_logs(self, limit: int = 50) -> list[dict]:
        stmt = (
            select(NotificationLog)
            .where(NotificationLog.status == NotificationStatus.FAILED.value)
            .order_by(NotificationLog.created_at.desc(), NotificationLog.id.desc())
            .limit(limit)
        )

        rows = list(self.db.execute(stmt).scalars().all())
        return [self._to_dict(row) for row in rows]

    def count_total(self) -> int:
        stmt = select(func.count(NotificationLog.id))
        return int(self.db.execute(stmt).scalar() or 0)

    def count_pending(self) -> int:
        stmt = select(func.count(NotificationLog.id)).where(
            NotificationLog.status == NotificationStatus.PENDING.value,
        )
        return int(self.db.execute(stmt).scalar() or 0)

    def count_sent(self) -> int:
        stmt = select(func.count(NotificationLog.id)).where(
            NotificationLog.status == NotificationStatus.SENT.value,
        )
        return int(self.db.execute(stmt).scalar() or 0)

    def count_failed(self) -> int:
        stmt = select(func.count(NotificationLog.id)).where(
            NotificationLog.status == NotificationStatus.FAILED.value,
        )
        return int(self.db.execute(stmt).scalar() or 0)

    def notification_stats(self) -> dict:
        return {
            "total": self.count_total(),
            "pending": self.count_pending(),
            "sent": self.count_sent(),
            "failed": self.count_failed(),
        }

    def _to_dict(self, row: NotificationLog) -> dict:
        return {
            "id": row.id,
            "user_id": row.user_id,
            "type": row.type,
            "status": row.status,
            "dedup_key": row.dedup_key,
            "payload": row.payload,
            "error_message": row.error_message,
            "sent_at": row.sent_at,
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }