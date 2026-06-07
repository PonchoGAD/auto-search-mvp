from __future__ import annotations

from functools import lru_cache
from typing import List, Optional

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        case_sensitive=False,
    )

    APP_NAME: str = "auto-search-tg-bot"
    APP_ENV: str = Field(default="dev")
    DEBUG: bool = Field(default=False)

    BOT_TOKEN: str = Field(default="")
    BOT_USERNAME: Optional[str] = Field(default=None)

    BOT_API_BASE_URL: str = Field(default="http://bot-api:8100")
    BOT_API_PREFIX: str = Field(default="/api/v1")
    BOT_API_TIMEOUT_SEC: float = Field(default=20.0)

    INTERNAL_API_KEY: str = Field(default="change-me-in-env")

    DEFAULT_PAGE_SIZE: int = Field(default=10)
    MAX_PAGE_SIZE: int = Field(default=50)

    SEARCH_RESULTS_PER_PAGE: int = Field(default=3)
    FAVORITES_PER_PAGE: int = Field(default=5)
    SAVED_SEARCHES_PER_PAGE: int = Field(default=5)

    THROTTLE_SEARCH_SEC: float = Field(default=1.0)
    THROTTLE_CALLBACK_SEC: float = Field(default=0.3)

    PAYMENT_PROVIDER: str = Field(default="stub")
    PAYMENT_TELEGRAM_PROVIDER_TOKEN: Optional[str] = Field(default=None)
    PAYMENT_WEBHOOK_SECRET: Optional[str] = Field(default=None)
    PAYMENT_PLAN_DURATION_DAYS: int = Field(default=30)

    ADMIN_TELEGRAM_IDS_RAW: str = Field(default="")

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
    def bot_api_url(self) -> str:
        return f"{self.BOT_API_BASE_URL.rstrip('/')}{self.BOT_API_PREFIX}"

    @property
    def health_url(self) -> str:
        return f"{self.bot_api_url}/health"

    @property
    def users_upsert_url(self) -> str:
        return f"{self.bot_api_url}/users/telegram/upsert"

    @property
    def auth_telegram_url(self) -> str:
        return f"{self.bot_api_url}/auth/telegram"

    @property
    def favorites_url(self) -> str:
        return f"{self.bot_api_url}/favorites"

    @property
    def saved_searches_url(self) -> str:
        return f"{self.bot_api_url}/saved-searches"

    @property
    def subscriptions_me_url(self) -> str:
        return f"{self.bot_api_url}/subscriptions/me"


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    return Settings()


settings = get_settings()