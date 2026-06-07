from __future__ import annotations

from decimal import Decimal
from functools import lru_cache
from typing import List, Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    APP_NAME: str = "auto-search-bot-api"
    APP_ENV: str = Field(default="dev")
    DEBUG: bool = Field(default=False)
    LOG_LEVEL: str = Field(default="INFO")

    API_HOST: str = Field(default="0.0.0.0")
    API_PORT: int = Field(default=8100)

    BOT_API_PREFIX: str = Field(default="/api/v1")
    BOT_INTERNAL_PREFIX: str = Field(default="/internal")

    DATABASE_URL: str = Field(
        default="postgresql+psycopg2://auto:auto133@postgres:5432/auto_search"
    )

    SEARCH_API_BASE_URL: str = Field(default="http://auto-search-api:8000")
    SEARCH_API_PREFIX: str = Field(default="/api/v1")
    SEARCH_API_KEY: Optional[str] = Field(default=None)
    SEARCH_API_TIMEOUT_SEC: float = Field(default=20.0)
    SEARCH_API_RETRIES: int = Field(default=3)
    SEARCH_API_RETRY_DELAY_SEC: float = Field(default=1.0)

    TELEGRAM_BOT_TOKEN: Optional[str] = Field(default=None)
    TELEGRAM_BOT_USERNAME: Optional[str] = Field(default=None)

    INTERNAL_API_KEY: str = Field(default="change-me-in-env")
    JWT_SECRET: str = Field(default="change-me-in-env")
    JWT_ALGORITHM: str = Field(default="HS256")
    JWT_EXPIRE_MINUTES: int = Field(default=60 * 24 * 30)

    DEFAULT_CURRENCY: str = Field(default="RUB")
    DEFAULT_PAGE_SIZE: int = Field(default=10)
    MAX_PAGE_SIZE: int = Field(default=50)

    FREE_DAILY_SEARCH_LIMIT: int = Field(default=25)
    FREE_SAVED_SEARCHES_LIMIT: int = Field(default=3)
    FREE_FAVORITES_LIMIT: int = Field(default=50)

    ADMIN_TELEGRAM_IDS_RAW: str = Field(default="")
    CORS_ORIGINS_RAW: str = Field(default="*")

    PAYMENT_PROVIDER: str = Field(default="stub")

    PAYMENT_RETURN_URL: Optional[str] = Field(default=None)
    PAYMENT_SUCCESS_URL: Optional[str] = Field(default=None)
    PAYMENT_FAIL_URL: Optional[str] = Field(default=None)

    REDIS_URL: Optional[str] = Field(default=None)
    REDIS_TIMEOUT_SEC: float = Field(default=2.0)

    PAYMENT_WEBHOOK_SECRET: Optional[str] = Field(default=None)

    PAYMENT_YOOKASSA_SHOP_ID: Optional[str] = Field(default=None)
    PAYMENT_YOOKASSA_SECRET_KEY: Optional[str] = Field(default=None)
    PAYMENT_YOOKASSA_BASE_URL: str = Field(default="https://api.yookassa.ru/v3")

    PAYMENT_PLAN_PREMIUM_STARS: int = Field(default=200)
    PAYMENT_PLAN_PRO_STARS: int = Field(default=400)

    PAYMENT_TELEGRAM_PROVIDER_TOKEN: Optional[str] = Field(default=None)

    PAYMENT_STRIPE_SECRET_KEY: Optional[str] = Field(default=None)
    PAYMENT_STRIPE_WEBHOOK_SECRET: Optional[str] = Field(default=None)

    PAYMENT_STUB_SUCCESS_ENABLED: bool = Field(default=False)
    PAYMENT_PLAN_PREMIUM_PRICE: Decimal = Field(default=Decimal("990.00"))
    PAYMENT_PLAN_PRO_PRICE: Decimal = Field(default=Decimal("1990.00"))
    PAYMENT_PLAN_DURATION_DAYS: int = Field(default=30)

    PAYMENT_IDEMPOTENCY_TTL_HOURS: int = Field(default=24)

    ALERTS_BATCH_SIZE: int = Field(default=100)
    ALERTS_MATCH_LIMIT_PER_SEARCH: int = Field(default=30)

    RATE_LIMIT_ENABLED: bool = Field(default=True)
    RATE_LIMIT_REQUESTS_PER_MINUTE: int = Field(default=120)
    RATE_LIMIT_BURST: int = Field(default=30)

    SECURITY_STRICT_ENV: bool = Field(default=False)

    @field_validator("JWT_ALGORITHM")
    @classmethod
    def validate_jwt_algorithm(cls, value: str) -> str:
        allowed = {"HS256", "HS384", "HS512"}
        clean = str(value or "").strip().upper()

        if clean not in allowed:
            raise ValueError(f"JWT_ALGORITHM must be one of: {', '.join(sorted(allowed))}")

        return clean

    @field_validator("PAYMENT_PROVIDER")
    @classmethod
    def validate_payment_provider(cls, value: str) -> str:
        allowed = {"stub", "yookassa", "stars", "telegram", "stripe"}
        clean = str(value or "").strip().lower()

        if clean not in allowed:
            raise ValueError(f"PAYMENT_PROVIDER must be one of: {', '.join(sorted(allowed))}")

        return clean

    @field_validator("PAYMENT_PLAN_DURATION_DAYS")
    @classmethod
    def validate_payment_plan_duration_days(cls, value: int) -> int:
        if int(value) <= 0:
            raise ValueError("PAYMENT_PLAN_DURATION_DAYS must be positive")
        return int(value)

    @field_validator("PAYMENT_PLAN_PREMIUM_PRICE", "PAYMENT_PLAN_PRO_PRICE")
    @classmethod
    def validate_payment_plan_prices(cls, value: Decimal) -> Decimal:
        if Decimal(value) <= 0:
            raise ValueError("payment plan prices must be positive")
        return Decimal(value)

    @property
    def is_prod(self) -> bool:
        return self.APP_ENV.lower().strip() in {"prod", "production"}

    @property
    def admin_telegram_ids(self) -> List[int]:
        ids: List[int] = []

        for part in self.ADMIN_TELEGRAM_IDS_RAW.split(","):
            value = part.strip()

            if not value:
                continue

            try:
                ids.append(int(value))
            except ValueError:
                continue

        return ids

    @property
    def cors_origins(self) -> List[str]:
        values = [x.strip() for x in self.CORS_ORIGINS_RAW.split(",") if x.strip()]

        if not values:
            return ["*"]

        if self.is_prod and "*" in values:
            return []

        return values

    @staticmethod
    def _normalize_prefix(prefix: str) -> str:
        clean = str(prefix or "").strip()

        if not clean:
            return ""

        if not clean.startswith("/"):
            clean = f"/{clean}"

        return clean.rstrip("/")

    @property
    def search_base_with_prefix(self) -> str:
        base_url = self.SEARCH_API_BASE_URL.rstrip("/")
        prefix = self._normalize_prefix(self.SEARCH_API_PREFIX)

        if not prefix:
            return base_url

        if base_url.endswith(prefix):
            return base_url

        return f"{base_url}{prefix}"

    @property
    def search_url(self) -> str:
        return f"{self.search_base_with_prefix}/search"

    @property
    def search_health_url(self) -> str:
        return f"{self.search_base_with_prefix}/health"

    @property
    def payment_success_url(self) -> Optional[str]:
        return self.PAYMENT_SUCCESS_URL or self.PAYMENT_RETURN_URL

    @property
    def payment_fail_url(self) -> Optional[str]:
        return self.PAYMENT_FAIL_URL or self.PAYMENT_RETURN_URL

    @property
    def payment_plan_prices(self) -> dict[str, Decimal]:
        return {
            "premium": self.PAYMENT_PLAN_PREMIUM_PRICE,
            "pro": self.PAYMENT_PLAN_PRO_PRICE,
        }

    def validate_security(self) -> list[str]:
        issues: list[str] = []

        if self.is_prod or self.SECURITY_STRICT_ENV:
            if not self.INTERNAL_API_KEY or self.INTERNAL_API_KEY == "change-me-in-env":
                issues.append("INTERNAL_API_KEY is not configured")

            if not self.JWT_SECRET or self.JWT_SECRET == "change-me-in-env":
                issues.append("JWT_SECRET is not configured")

            if len(str(self.JWT_SECRET or "")) < 32:
                issues.append("JWT_SECRET must be at least 32 characters")

            non_stub_providers = {"yookassa", "telegram", "stripe"}
            if self.PAYMENT_PROVIDER in non_stub_providers and not self.PAYMENT_WEBHOOK_SECRET:
                issues.append("PAYMENT_WEBHOOK_SECRET is required for real payment provider")

            if self.PAYMENT_PROVIDER == "yookassa":
                if not self.PAYMENT_YOOKASSA_SHOP_ID:
                    issues.append("PAYMENT_YOOKASSA_SHOP_ID is required")
                if not self.PAYMENT_YOOKASSA_SECRET_KEY:
                    issues.append("PAYMENT_YOOKASSA_SECRET_KEY is required")

            if self.PAYMENT_PROVIDER == "stripe":
                if not self.PAYMENT_STRIPE_SECRET_KEY:
                    issues.append("PAYMENT_STRIPE_SECRET_KEY is required")
                if not self.PAYMENT_STRIPE_WEBHOOK_SECRET:
                    issues.append("PAYMENT_STRIPE_WEBHOOK_SECRET is required")

            if self.PAYMENT_PROVIDER == "telegram" and not self.PAYMENT_TELEGRAM_PROVIDER_TOKEN:
                issues.append("PAYMENT_TELEGRAM_PROVIDER_TOKEN is required")

            if self.PAYMENT_STUB_SUCCESS_ENABLED and self.PAYMENT_PROVIDER != "stub":
                issues.append("PAYMENT_STUB_SUCCESS_ENABLED can be enabled only with PAYMENT_PROVIDER=stub")

        return issues


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    issues = settings.validate_security()

    if issues and (settings.is_prod or settings.SECURITY_STRICT_ENV):
        raise RuntimeError("Security configuration errors: " + "; ".join(issues))

    return settings


settings = get_settings()