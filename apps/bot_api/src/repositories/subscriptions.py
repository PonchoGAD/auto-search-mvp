from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Optional

from sqlalchemy import func, select
from sqlalchemy.orm import Session

from src.db.enums import SubscriptionPlan, SubscriptionStatus
from src.db.models import Subscription, User
from src.schemas.subscriptions import (
    SubscriptionCreateRequest,
    SubscriptionUpdateRequest,
)


class SubscriptionsRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def get_by_id(self, subscription_id: int) -> Optional[Subscription]:
        stmt = select(Subscription).where(Subscription.id == subscription_id)
        return self.db.execute(stmt).scalar_one_or_none()

    def list_by_user(self, user_id: int) -> list[Subscription]:
        stmt = (
            select(Subscription)
            .where(Subscription.user_id == user_id)
            .order_by(Subscription.created_at.desc(), Subscription.id.desc())
        )
        return list(self.db.execute(stmt).scalars().all())

    def get_active_by_user(self, user_id: int) -> Optional[Subscription]:
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

    def create(self, payload: SubscriptionCreateRequest) -> Subscription:
        self._validate_plan(payload.plan)
        self._validate_status(payload.status)
        self._validate_dates(payload.starts_at, payload.expires_at)

        entity = Subscription(
            user_id=payload.user_id,
            plan=payload.plan,
            status=payload.status,
            starts_at=payload.starts_at,
            expires_at=payload.expires_at,
            meta=payload.meta,
        )

        self.db.add(entity)
        self.db.commit()
        self.db.refresh(entity)

        self._sync_user_premium_flag(payload.user_id)
        return entity

    def update(self, entity: Subscription, payload: SubscriptionUpdateRequest) -> Subscription:
        if payload.plan is not None:
            self._validate_plan(payload.plan)
            entity.plan = payload.plan

        if payload.status is not None:
            self._validate_status(payload.status)
            entity.status = payload.status

        if payload.starts_at is not None:
            entity.starts_at = payload.starts_at

        if payload.expires_at is not None:
            entity.expires_at = payload.expires_at

        self._validate_dates(entity.starts_at, entity.expires_at)

        if payload.canceled_at is not None:
            entity.canceled_at = payload.canceled_at

        if payload.meta is not None:
            entity.meta = payload.meta

        self.db.add(entity)
        self.db.commit()
        self.db.refresh(entity)

        self._sync_user_premium_flag(entity.user_id)
        return entity

    def cancel(self, entity: Subscription) -> Subscription:
        entity.status = SubscriptionStatus.CANCELED.value
        entity.canceled_at = datetime.now(timezone.utc)

        self.db.add(entity)
        self.db.commit()
        self.db.refresh(entity)

        self._sync_user_premium_flag(entity.user_id)
        return entity

    def expire_overdue(self) -> int:
        now = datetime.now(timezone.utc)

        stmt = (
            select(Subscription)
            .where(
                Subscription.status == SubscriptionStatus.ACTIVE.value,
                Subscription.expires_at.is_not(None),
                Subscription.expires_at < now,
            )
        )

        items = list(self.db.execute(stmt).scalars().all())
        affected_user_ids: set[int] = set()

        for item in items:
            item.status = SubscriptionStatus.EXPIRED.value
            self.db.add(item)
            affected_user_ids.add(item.user_id)

        if items:
            self.db.commit()

            for user_id in affected_user_ids:
                self._sync_user_premium_flag(user_id)

        return len(items)

    def ensure_free_subscription(self, user_id: int) -> Subscription:
        existing = self.get_active_by_user(user_id)
        if existing:
            return existing

        entity = Subscription(
            user_id=user_id,
            plan=SubscriptionPlan.FREE.value,
            status=SubscriptionStatus.ACTIVE.value,
            starts_at=datetime.now(timezone.utc),
            expires_at=None,
            meta={"auto_created": True},
        )

        self.db.add(entity)
        self.db.commit()
        self.db.refresh(entity)

        self._sync_user_premium_flag(user_id)
        return entity

    def replace_active_subscription(
        self,
        user_id: int,
        plan: str,
        starts_at: datetime,
        expires_at: datetime | None,
        meta: dict | None = None,
    ) -> Subscription:
        self._validate_plan(plan)
        self._validate_dates(starts_at, expires_at)

        now = datetime.now(timezone.utc)

        current_active = self.db.execute(
            select(Subscription).where(
                Subscription.user_id == user_id,
                Subscription.status == SubscriptionStatus.ACTIVE.value,
            )
        ).scalars().all()

        for item in current_active:
            item.status = SubscriptionStatus.CANCELED.value
            item.canceled_at = now
            self.db.add(item)

        entity = Subscription(
            user_id=user_id,
            plan=plan,
            status=SubscriptionStatus.ACTIVE.value,
            starts_at=starts_at,
            expires_at=expires_at,
            meta=meta or {},
        )

        self.db.add(entity)
        self.db.commit()
        self.db.refresh(entity)

        self._sync_user_premium_flag(user_id)
        return entity

    def activate_paid_subscription_from_payment(
        self,
        user_id: int,
        plan: str,
        payment_id: int,
        external_payment_id: str | None = None,
        provider: str | None = None,
        duration_days: int = 30,
        starts_at: datetime | None = None,
        meta: dict | None = None,
    ) -> Subscription:
        self._validate_plan(plan)

        if plan not in {SubscriptionPlan.PREMIUM.value, SubscriptionPlan.PRO.value}:
            raise ValueError(f"paid subscription plan required, got: {plan}")

        if duration_days <= 0:
            raise ValueError("duration_days must be positive")

        start = starts_at or datetime.now(timezone.utc).replace(microsecond=0)
        expires_at = start + timedelta(days=duration_days)

        subscription_meta = {
            "payment_id": payment_id,
            "external_payment_id": external_payment_id,
            "provider": provider,
            "duration_days": duration_days,
            "activated_at": datetime.now(timezone.utc).replace(microsecond=0).isoformat(),
            "activation_source": "payment",
        }

        if meta:
            subscription_meta.update(meta)

        return self.replace_active_subscription(
            user_id=user_id,
            plan=plan,
            starts_at=start,
            expires_at=expires_at,
            meta=subscription_meta,
        )

    def manual_activate(
        self,
        user_id: int,
        plan: str,
        duration_days: int = 30,
        admin_telegram_id: int | None = None,
        reason: str | None = None,
        meta: dict | None = None,
    ) -> Subscription:
        self._validate_plan(plan)

        if plan not in {SubscriptionPlan.PREMIUM.value, SubscriptionPlan.PRO.value, SubscriptionPlan.FREE.value}:
            raise ValueError(f"invalid manual activation plan: {plan}")

        if duration_days <= 0 and plan != SubscriptionPlan.FREE.value:
            raise ValueError("duration_days must be positive")

        now = datetime.now(timezone.utc).replace(microsecond=0)
        expires_at = None if plan == SubscriptionPlan.FREE.value else now + timedelta(days=duration_days)

        subscription_meta = {
            "activation_source": "manual_admin",
            "admin_telegram_id": admin_telegram_id,
            "reason": reason,
            "duration_days": duration_days if expires_at else None,
            "activated_at": now.isoformat(),
        }

        if meta:
            subscription_meta.update(meta)

        return self.replace_active_subscription(
            user_id=user_id,
            plan=plan,
            starts_at=now,
            expires_at=expires_at,
            meta=subscription_meta,
        )

    def active_total(self) -> int:
        stmt = select(func.count(Subscription.id)).where(
            Subscription.status == SubscriptionStatus.ACTIVE.value,
        )
        return int(self.db.execute(stmt).scalar() or 0)

    def active_by_plan(self, plan: str) -> int:
        stmt = select(func.count(Subscription.id)).where(
            Subscription.status == SubscriptionStatus.ACTIVE.value,
            Subscription.plan == plan,
        )
        return int(self.db.execute(stmt).scalar() or 0)

    def expired_total(self) -> int:
        stmt = select(func.count(Subscription.id)).where(
            Subscription.status == SubscriptionStatus.EXPIRED.value,
        )
        return int(self.db.execute(stmt).scalar() or 0)

    def canceled_total(self) -> int:
        stmt = select(func.count(Subscription.id)).where(
            Subscription.status == SubscriptionStatus.CANCELED.value,
        )
        return int(self.db.execute(stmt).scalar() or 0)

    def past_due_total(self) -> int:
        stmt = select(func.count(Subscription.id)).where(
            Subscription.status == SubscriptionStatus.PAST_DUE.value,
        )
        return int(self.db.execute(stmt).scalar() or 0)

    def _sync_user_premium_flag(self, user_id: int) -> None:
        user = self.db.execute(select(User).where(User.id == user_id)).scalar_one_or_none()
        if not user:
            return

        active = self.get_active_by_user(user_id)
        user.is_premium = bool(
            active and active.plan in {SubscriptionPlan.PREMIUM.value, SubscriptionPlan.PRO.value}
        )

        self.db.add(user)
        self.db.commit()

    def _validate_plan(self, plan: str) -> None:
        if plan not in {item.value for item in SubscriptionPlan}:
            raise ValueError(f"invalid subscription plan: {plan}")

    def _validate_status(self, status: str) -> None:
        if status not in {item.value for item in SubscriptionStatus}:
            raise ValueError(f"invalid subscription status: {status}")

    def _validate_dates(
        self,
        starts_at: datetime | None,
        expires_at: datetime | None,
    ) -> None:
        if starts_at and expires_at and expires_at < starts_at:
            raise ValueError("expires_at cannot be earlier than starts_at")