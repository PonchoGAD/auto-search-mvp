from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    CheckConstraint,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    JSON,
    Numeric,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from src.db.base import Base
from src.db.enums import (
    FavoriteSource,
    NotificationStatus,
    PaymentProvider,
    PaymentStatus,
    SavedSearchStatus,
    SubscriptionPlan,
    SubscriptionStatus,
    UserRole,
    UserStatus,
)


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )


class User(Base, TimestampMixin):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)

    telegram_user_id: Mapped[int] = mapped_column(
        BigInteger,
        unique=True,
        nullable=False,
        index=True,
    )
    telegram_chat_id: Mapped[Optional[int]] = mapped_column(
        BigInteger,
        nullable=True,
        index=True,
    )

    username: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    first_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    last_name: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    language_code: Mapped[Optional[str]] = mapped_column(String(16), nullable=True)

    role: Mapped[str] = mapped_column(
        String(16),
        default=UserRole.USER.value,
        nullable=False,
    )
    status: Mapped[str] = mapped_column(
        String(16),
        default=UserStatus.ACTIVE.value,
        nullable=False,
    )

    is_premium: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    last_seen_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    favorites: Mapped[list["Favorite"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    saved_searches: Mapped[list["SavedSearch"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    search_history: Mapped[list["SearchHistory"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    subscriptions: Mapped[list["Subscription"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    payments: Mapped[list["Payment"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )
    notifications: Mapped[list["NotificationLog"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
        passive_deletes=True,
    )

    __table_args__ = (
        Index("ix_users_role_status", "role", "status"),
        CheckConstraint("role IN ('user', 'admin')", name="ck_users_role_valid"),
        CheckConstraint("status IN ('active', 'blocked')", name="ck_users_status_valid"),
    )


class Favorite(Base, TimestampMixin):
    __tablename__ = "favorites"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    listing_id: Mapped[str] = mapped_column(String(128), nullable=False, index=True)
    source_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    source_name: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)

    title: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)
    brand: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, index=True)
    model: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    year: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    mileage: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    price: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    currency: Mapped[Optional[str]] = mapped_column(String(8), nullable=True)
    fuel: Mapped[Optional[str]] = mapped_column(String(32), nullable=True)
    region: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    city: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    color: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    condition: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    paint_condition: Mapped[Optional[str]] = mapped_column(String(64), nullable=True)
    image_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    photos: Mapped[Optional[list]] = mapped_column(JSON, nullable=True)
    created_at_ts: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)

    payload: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    source_type: Mapped[str] = mapped_column(
        String(16),
        default=FavoriteSource.SEARCH.value,
        nullable=False,
    )

    user: Mapped["User"] = relationship(back_populates="favorites")

    __table_args__ = (
        UniqueConstraint("user_id", "listing_id", name="uq_favorites_user_listing"),
        Index("ix_favorites_user_created_at", "user_id", "created_at"),
        Index("ix_favorites_user_brand_model", "user_id", "brand", "model"),
        CheckConstraint("source_type IN ('search', 'alert', 'manual')", name="ck_favorites_source_type_valid"),
        CheckConstraint("year IS NULL OR year BETWEEN 1950 AND 2100", name="ck_favorites_year_valid"),
        CheckConstraint("mileage IS NULL OR mileage >= 0", name="ck_favorites_mileage_non_negative"),
        CheckConstraint("price IS NULL OR price >= 0", name="ck_favorites_price_non_negative"),
    )


class SavedSearch(Base, TimestampMixin):
    __tablename__ = "saved_searches"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    name: Mapped[str] = mapped_column(String(255), nullable=False)
    raw_query: Mapped[str] = mapped_column(String(1000), nullable=False)

    query_payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)
    last_seen_listing_id: Mapped[Optional[str]] = mapped_column(String(128), nullable=True)
    last_checked_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    status: Mapped[str] = mapped_column(
        String(16),
        default=SavedSearchStatus.ACTIVE.value,
        nullable=False,
    )
    is_alert_enabled: Mapped[bool] = mapped_column(Boolean, default=True, nullable=False)

    user: Mapped["User"] = relationship(back_populates="saved_searches")

    __table_args__ = (
        Index("ix_saved_searches_user_status", "user_id", "status"),
        Index("ix_saved_searches_alerts_queue", "status", "is_alert_enabled", "last_checked_at"),
        UniqueConstraint("user_id", "name", name="uq_saved_searches_user_name"),
        CheckConstraint("status IN ('active', 'paused', 'disabled')", name="ck_saved_searches_status_valid"),
    )


class SearchHistory(Base):
    __tablename__ = "search_history"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    raw_query: Mapped[str] = mapped_column(String(1000), nullable=False)
    query_payload: Mapped[dict] = mapped_column(JSON, nullable=False, default=dict)

    results_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False)
    latency_ms: Mapped[Optional[int]] = mapped_column(Integer, nullable=True)
    empty_result: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )

    user: Mapped["User"] = relationship(back_populates="search_history")

    __table_args__ = (
        Index("ix_search_history_user_created_at", "user_id", "created_at"),
        Index("ix_search_history_created_at", "created_at"),
        CheckConstraint("results_count >= 0", name="ck_search_history_results_count_non_negative"),
        CheckConstraint("latency_ms IS NULL OR latency_ms >= 0", name="ck_search_history_latency_non_negative"),
    )


class Subscription(Base, TimestampMixin):
    __tablename__ = "subscriptions"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    plan: Mapped[str] = mapped_column(
        String(16),
        default=SubscriptionPlan.FREE.value,
        nullable=False,
    )
    status: Mapped[str] = mapped_column(
        String(16),
        default=SubscriptionStatus.ACTIVE.value,
        nullable=False,
    )

    starts_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    expires_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)
    canceled_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    meta: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)

    user: Mapped["User"] = relationship(back_populates="subscriptions")

    __table_args__ = (
        Index("ix_subscriptions_user_status", "user_id", "status"),
        Index("ix_subscriptions_status_expires_at", "status", "expires_at"),
        CheckConstraint("plan IN ('free', 'premium', 'pro')", name="ck_subscriptions_plan_valid"),
        CheckConstraint(
            "status IN ('active', 'expired', 'canceled', 'past_due')",
            name="ck_subscriptions_status_valid",
        ),
        CheckConstraint(
            "expires_at IS NULL OR starts_at IS NULL OR expires_at >= starts_at",
            name="ck_subscriptions_dates_valid",
        ),
    )


class Payment(Base, TimestampMixin):
    __tablename__ = "payments"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    provider: Mapped[str] = mapped_column(
        String(32),
        default=PaymentProvider.STUB.value,
        nullable=False,
    )
    status: Mapped[str] = mapped_column(
        String(16),
        default=PaymentStatus.PENDING.value,
        nullable=False,
    )

    amount: Mapped[Decimal] = mapped_column(Numeric(12, 2), nullable=False)
    currency: Mapped[str] = mapped_column(String(8), nullable=False, default="RUB")

    external_payment_id: Mapped[Optional[str]] = mapped_column(
        String(128),
        nullable=True,
        index=True,
    )
    idempotency_key: Mapped[Optional[str]] = mapped_column(String(128), nullable=True, index=True)

    payment_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    invoice_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    return_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    success_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    fail_url: Mapped[Optional[str]] = mapped_column(Text, nullable=True)

    description: Mapped[Optional[str]] = mapped_column(String(512), nullable=True)

    payload: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    paid_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    user: Mapped["User"] = relationship(back_populates="payments")

    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_payments_amount_positive"),
        CheckConstraint(
            "provider IN ('stub', 'yookassa', 'stars', 'telegram', 'stripe')",
            name="ck_payments_provider_valid",
        ),
        CheckConstraint(
            "status IN ('pending', 'succeeded', 'failed', 'canceled', 'refunded')",
            name="ck_payments_status_valid",
        ),
        UniqueConstraint("external_payment_id", name="uq_payments_external_payment_id"),
        UniqueConstraint("user_id", "provider", "idempotency_key", name="uq_payments_user_provider_idempotency"),
        Index("ix_payments_user_status", "user_id", "status"),
        Index("ix_payments_provider_status", "provider", "status"),
        Index("ix_payments_status_created_at", "status", "created_at"),
    )


class NotificationLog(Base, TimestampMixin):
    __tablename__ = "notifications"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    type: Mapped[str] = mapped_column(String(64), nullable=False)
    status: Mapped[str] = mapped_column(
        String(16),
        default=NotificationStatus.PENDING.value,
        nullable=False,
    )

    dedup_key: Mapped[Optional[str]] = mapped_column(String(255), nullable=True, index=True)
    payload: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True)
    error_message: Mapped[Optional[str]] = mapped_column(Text, nullable=True)
    sent_at: Mapped[Optional[datetime]] = mapped_column(DateTime(timezone=True), nullable=True)

    user: Mapped["User"] = relationship(back_populates="notifications")

    __table_args__ = (
        UniqueConstraint("dedup_key", name="uq_notifications_dedup_key"),
        Index("ix_notifications_user_type_status", "user_id", "type", "status"),
        Index("ix_notifications_status_created_at", "status", "created_at"),
        CheckConstraint("status IN ('pending', 'sent', 'failed')", name="ck_notifications_status_valid"),
    )