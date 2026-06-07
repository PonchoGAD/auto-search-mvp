from __future__ import annotations

import enum


class UserRole(str, enum.Enum):
    USER = "user"
    ADMIN = "admin"


class UserStatus(str, enum.Enum):
    ACTIVE = "active"
    BLOCKED = "blocked"


class SubscriptionPlan(str, enum.Enum):
    FREE = "free"
    PREMIUM = "premium"
    PRO = "pro"


class SubscriptionStatus(str, enum.Enum):
    ACTIVE = "active"
    EXPIRED = "expired"
    CANCELED = "canceled"
    PAST_DUE = "past_due"


class PaymentProvider(str, enum.Enum):
    STUB = "stub"
    YOOKASSA = "yookassa"
    STARS = "stars"
    TELEGRAM = "telegram"
    STRIPE = "stripe"


class PaymentStatus(str, enum.Enum):
    PENDING = "pending"
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    CANCELED = "canceled"
    REFUNDED = "refunded"


class SavedSearchStatus(str, enum.Enum):
    ACTIVE = "active"
    PAUSED = "paused"
    DISABLED = "disabled"


class NotificationType(str, enum.Enum):
    SAVED_SEARCH_ALERT = "saved_search_alert"
    SUBSCRIPTION_EXPIRY = "subscription_expiry"


class NotificationStatus(str, enum.Enum):
    PENDING = "pending"
    SENT = "sent"
    FAILED = "failed"


class FavoriteSource(str, enum.Enum):
    SEARCH = "search"
    ALERT = "alert"
    MANUAL = "manual"