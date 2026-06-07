from __future__ import annotations

from datetime import datetime, timedelta, timezone

from sqlalchemy import select
from sqlalchemy.orm import Session

from src.config import settings
from src.db.enums import SubscriptionPlan, SubscriptionStatus
from src.db.models import Favorite, SavedSearch, SearchHistory, Subscription, User


class UsageLimitsRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def get_daily_searches_used(self, user_id: int) -> int:
        now = datetime.now(timezone.utc)
        start_of_day = datetime(
            year=now.year,
            month=now.month,
            day=now.day,
            tzinfo=timezone.utc,
        )

        stmt = (
            select(SearchHistory)
            .where(
                SearchHistory.user_id == user_id,
                SearchHistory.created_at >= start_of_day,
            )
        )
        return len(list(self.db.execute(stmt).scalars().all()))

    def get_favorites_count(self, user_id: int) -> int:
        stmt = select(Favorite).where(Favorite.user_id == user_id)
        return len(list(self.db.execute(stmt).scalars().all()))

    def get_saved_searches_count(self, user_id: int) -> int:
        stmt = select(SavedSearch).where(SavedSearch.user_id == user_id)
        return len(list(self.db.execute(stmt).scalars().all()))

    def get_active_subscription(self, user_id: int) -> Subscription | None:
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

    def is_premium_user(self, user_id: int) -> bool:
        subscription = self.get_active_subscription(user_id)
        if not subscription:
            return False

        return subscription.plan in {
            SubscriptionPlan.PREMIUM.value,
            SubscriptionPlan.PRO.value,
        }

    def get_limits_snapshot(self, user_id: int) -> dict:
        searches_used_today = self.get_daily_searches_used(user_id)
        favorites_count = self.get_favorites_count(user_id)
        saved_searches_count = self.get_saved_searches_count(user_id)

        subscription = self.get_active_subscription(user_id)

        if subscription and subscription.plan in {
            SubscriptionPlan.PREMIUM.value,
            SubscriptionPlan.PRO.value,
        }:
            return {
                "is_premium": True,
                "plan": subscription.plan,
                "status": subscription.status,
                "searches_left_today": None,
                "saved_searches_left": None,
                "favorites_left": None,
                "searches_used_today": searches_used_today,
                "saved_searches_count": saved_searches_count,
                "favorites_count": favorites_count,
                "expires_at": subscription.expires_at,
            }

        return {
            "is_premium": False,
            "plan": SubscriptionPlan.FREE.value,
            "status": SubscriptionStatus.ACTIVE.value,
            "searches_left_today": max(settings.FREE_DAILY_SEARCH_LIMIT - searches_used_today, 0),
            "saved_searches_left": max(settings.FREE_SAVED_SEARCHES_LIMIT - saved_searches_count, 0),
            "favorites_left": max(settings.FREE_FAVORITES_LIMIT - favorites_count, 0),
            "searches_used_today": searches_used_today,
            "saved_searches_count": saved_searches_count,
            "favorites_count": favorites_count,
            "expires_at": None,
        }

    def can_create_saved_search(self, user_id: int) -> bool:
        snapshot = self.get_limits_snapshot(user_id)
        left = snapshot.get("saved_searches_left")
        return left is None or left > 0

    def can_add_favorite(self, user_id: int) -> bool:
        snapshot = self.get_limits_snapshot(user_id)
        left = snapshot.get("favorites_left")
        return left is None or left > 0

    def can_search(self, user_id: int) -> bool:
        snapshot = self.get_limits_snapshot(user_id)
        left = snapshot.get("searches_left_today")
        return left is None or left > 0