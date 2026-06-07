from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.db.enums import SubscriptionStatus, UserRole, UserStatus
from src.db.models import Favorite, SavedSearch, Subscription, User
from src.schemas.users import TelegramUserUpsertRequest


class UsersRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def get_by_id(self, user_id: int) -> Optional[User]:
        stmt = select(User).where(User.id == user_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def get_by_telegram_user_id(self, telegram_user_id: int) -> Optional[User]:
        stmt = select(User).where(User.telegram_user_id == telegram_user_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def upsert_telegram_user(self, payload: TelegramUserUpsertRequest) -> User:
        user = self.get_by_telegram_user_id(payload.telegram_user_id)
        now = datetime.now(timezone.utc)

        if user:
            user.telegram_chat_id = payload.telegram_chat_id
            user.username = payload.username
            user.first_name = payload.first_name
            user.last_name = payload.last_name
            user.language_code = payload.language_code
            user.last_seen_at = now

            self.db.add(user)
            self.db.commit()
            self.db.refresh(user)
            return user

        user = User(
            telegram_user_id=payload.telegram_user_id,
            telegram_chat_id=payload.telegram_chat_id,
            username=payload.username,
            first_name=payload.first_name,
            last_name=payload.last_name,
            language_code=payload.language_code,
            role=UserRole.USER.value,
            status=UserStatus.ACTIVE.value,
            is_premium=False,
            last_seen_at=now,
        )

        self.db.add(user)
        self.db.commit()
        self.db.refresh(user)

        return user

    def count_favorites(self, user_id: int) -> int:
        stmt = select(func.count(Favorite.id)).where(Favorite.user_id == user_id)
        return int(self.db.execute(stmt).scalar() or 0)

    def count_saved_searches(self, user_id: int) -> int:
        stmt = select(func.count(SavedSearch.id)).where(SavedSearch.user_id == user_id)
        return int(self.db.execute(stmt).scalar() or 0)

    def get_active_subscription(self, user_id: int) -> Optional[Subscription]:
        now = datetime.now(timezone.utc)

        stmt = (
            select(Subscription)
            .where(
                Subscription.user_id == user_id,
                Subscription.status == SubscriptionStatus.ACTIVE.value,
            )
            .order_by(Subscription.expires_at.desc().nullslast(), Subscription.id.desc())
        )

        items = list(self.db.execute(stmt).scalars().all())

        for item in items:
            if item.expires_at is None or item.expires_at >= now:
                return item

        return None

    def total_users(self) -> int:
        stmt = select(func.count(User.id))
        return int(self.db.execute(stmt).scalar() or 0)

    def active_users(self) -> int:
        stmt = select(func.count(User.id)).where(User.status == UserStatus.ACTIVE.value)
        return int(self.db.execute(stmt).scalar() or 0)

    def premium_users(self) -> int:
        stmt = select(func.count(User.id)).where(User.is_premium.is_(True))
        return int(self.db.execute(stmt).scalar() or 0)

    def admin_users(self) -> int:
        stmt = select(func.count(User.id)).where(User.role == UserRole.ADMIN.value)
        return int(self.db.execute(stmt).scalar() or 0)

    def new_users_last_24h(self) -> int:
        since = datetime.now(timezone.utc) - timedelta(hours=24)

        stmt = select(func.count(User.id)).where(User.created_at >= since)
        return int(self.db.execute(stmt).scalar() or 0)

    def latest_users(self, limit: int = 50) -> list[dict]:
        stmt = (
            select(User)
            .order_by(User.created_at.desc(), User.id.desc())
            .limit(limit)
        )

        rows = list(self.db.execute(stmt).scalars().all())
        return [self._to_dict(row) for row in rows]

    def _to_dict(self, row: User) -> dict:
        return {
            "id": row.id,
            "telegram_user_id": row.telegram_user_id,
            "telegram_chat_id": row.telegram_chat_id,
            "username": row.username,
            "first_name": row.first_name,
            "last_name": row.last_name,
            "language_code": row.language_code,
            "role": row.role,
            "status": row.status,
            "is_premium": row.is_premium,
            "last_seen_at": row.last_seen_at,
            "created_at": row.created_at,
            "updated_at": row.updated_at,
        }