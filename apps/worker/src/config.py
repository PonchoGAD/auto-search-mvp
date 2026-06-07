from __future__ import annotations

from functools import lru_cache
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    APP_NAME: str = "auto-search-worker"
    APP_ENV: str = Field(default="dev")
    DEBUG: bool = Field(default=False)
    LOG_LEVEL: str = Field(default="INFO")

    BOT_API_BASE_URL: str = Field(default="http://bot-api:8100")
    BOT_API_PREFIX: str = Field(default="/api/v1")
    BOT_API_TIMEOUT_SEC: float = Field(default=20.0)
    BOT_API_RETRIES: int = Field(default=3)
    BOT_API_RETRY_DELAY_SEC: float = Field(default=1.0)

    SEARCH_API_BASE_URL: str = Field(default="http://auto-search-api:8000")
    SEARCH_API_PREFIX: str = Field(default="/api/v1")
    SEARCH_API_TIMEOUT_SEC: float = Field(default=20.0)
    SEARCH_API_RETRIES: int = Field(default=3)
    SEARCH_API_RETRY_DELAY_SEC: float = Field(default=1.0)
    SEARCH_API_KEY: Optional[str] = Field(default=None)

    REDIS_URL: Optional[str] = Field(default=None)
    REDIS_TIMEOUT_SEC: float = Field(default=2.0)

    TELEGRAM_BOT_TOKEN: str = Field(default="")
    TELEGRAM_API_TIMEOUT_SEC: float = Field(default=20.0)

    INTERNAL_API_KEY: str = Field(default="change-me-in-env")

    ALERTS_BATCH_SIZE: int = Field(default=100)
    ALERTS_MATCH_LIMIT_PER_SEARCH: int = Field(default=30)
    ALERTS_MAX_SAVED_SEARCHES_PER_RUN: int = Field(default=100)
    ALERTS_MAX_ITEMS_PER_MESSAGE: int = Field(default=5)
    ALERTS_MAX_NEW_ITEMS_PER_SEARCH: int = Field(default=20)

    SUBSCRIPTION_EXPIRY_LOOKAHEAD_DAYS: int = Field(default=3)

    SCHEDULER_POLL_INTERVAL_SEC: int = Field(default=60)
    SCHEDULER_RUN_ON_STARTUP: bool = Field(default=True)

    SECURITY_STRICT_ENV: bool = Field(default=False)

    @field_validator("BOT_API_BASE_URL", "SEARCH_API_BASE_URL")
    @classmethod
    def validate_base_url(cls, value: str) -> str:
        clean = str(value or "").strip().rstrip("/")

        if not clean:
            raise ValueError("base url cannot be empty")

        if not clean.startswith(("http://", "https://")):
            raise ValueError("base url must start with http:// or https://")

        return clean

    @property
    def is_prod(self) -> bool:
        return self.APP_ENV.lower().strip() in {"prod", "production"}

    @property
    def bot_api_url(self) -> str:
        return f"{self.BOT_API_BASE_URL.rstrip('/')}{self.BOT_API_PREFIX}"

    @property
    def search_api_url(self) -> str:
        return f"{self.SEARCH_API_BASE_URL.rstrip('/')}{self.SEARCH_API_PREFIX}"

    @property
    def bot_api_headers(self) -> dict[str, str]:
        return {
            "X-INTERNAL-KEY": self.INTERNAL_API_KEY,
            "Content-Type": "application/json",
        }

    @property
    def search_api_headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}

        if self.SEARCH_API_KEY:
            headers["X-API-Key"] = self.SEARCH_API_KEY

        if self.INTERNAL_API_KEY:
            headers["X-INTERNAL-KEY"] = self.INTERNAL_API_KEY

        return headers

    def validate_security(self) -> list[str]:
        issues: list[str] = []

        if self.is_prod or self.SECURITY_STRICT_ENV:
            if not self.INTERNAL_API_KEY or self.INTERNAL_API_KEY == "change-me-in-env":
                issues.append("INTERNAL_API_KEY is not configured")

            if not self.TELEGRAM_BOT_TOKEN:
                issues.append("TELEGRAM_BOT_TOKEN is not configured")

            if self.SCHEDULER_POLL_INTERVAL_SEC < 10:
                issues.append("SCHEDULER_POLL_INTERVAL_SEC should be >= 10 in production")

            if self.ALERTS_MAX_ITEMS_PER_MESSAGE > 10:
                issues.append("ALERTS_MAX_ITEMS_PER_MESSAGE should be <= 10")

        return issues


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    settings = Settings()
    issues = settings.validate_security()

    if issues and (settings.is_prod or settings.SECURITY_STRICT_ENV):
        raise RuntimeError("Worker security configuration errors: " + "; ".join(issues))

    return settings


settings = get_settings()