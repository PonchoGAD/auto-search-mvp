# apps/api/src/core/settings.py

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """
    ЕДИНЫЙ источник конфигурации для всего API.

    - Pydantic v2 (pydantic-settings)
    - читает .env / env vars
    - стабильно в Docker compose
    """

    # =========================
    # APP
    # =========================
    app_name: str = Field(default="auto-search-mvp", alias="APP_NAME")
    env: str = Field(default="local", alias="ENV")
    DEBUG: bool = Field(default=False, alias="DEBUG")
    LOG_LEVEL: str = Field(default="INFO", alias="LOG_LEVEL")

    API_HOST: str = Field(default="0.0.0.0", alias="API_HOST")
    API_PORT: int = Field(default=8000, alias="API_PORT")
    API_KEY: str = Field(default="", alias="API_KEY")

    # =========================
    # DATABASE
    # =========================
    DATABASE_URL: str = Field(
        default="postgresql+psycopg2://auto:auto@postgres:5432/auto_search",
        alias="DATABASE_URL",
    )

    # =========================
    # REDIS
    # =========================
    redis_url: str = Field(
        default="redis://redis:6379/0",
        alias="REDIS_URL",
    )

    # =========================
    # QDRANT
    # =========================
    qdrant_url: str = Field(
        default="http://qdrant:6333",
        alias="QDRANT_URL",
    )
    qdrant_host: str = Field(
        default="qdrant",
        alias="QDRANT_HOST",
    )
    qdrant_port: int = Field(
        default=6333,
        alias="QDRANT_PORT",
    )
    qdrant_collection: str = Field(
        default="auto_search_chunks",
        alias="QDRANT_COLLECTION",
    )

    # =========================
    # SEARCH SETTINGS
    # =========================
    search_limit: int = Field(default=20, alias="SEARCH_LIMIT")
    search_top_k: int = Field(default=100, alias="SEARCH_TOP_K")
    search_min_candidates: int = Field(default=50, alias="SEARCH_MIN_CANDIDATES")
    search_rerank_max_candidates: int = Field(default=250, alias="SEARCH_RERANK_MAX_CANDIDATES")
    listings_fallback_max_scan: int = Field(default=500, alias="LISTINGS_FALLBACK_MAX_SCAN")

    # =========================
    # INGEST LIMITS (VPS SAFE)
    # =========================
    ingest_max_docs_per_source: int = Field(
        default=200,
        alias="INGEST_MAX_DOCS_PER_SOURCE",
    )
    ingest_max_pages: int = Field(
        default=2,
        alias="INGEST_MAX_PAGES",
    )
    ingest_throttle_sec: float = Field(
        default=1.5,
        alias="INGEST_THROTTLE_SEC",
    )

    # =========================
    # TELEGRAM (optional)
    # =========================
    tg_api_id: int = Field(default=0, alias="TG_API_ID")
    tg_api_hash: str = Field(default="", alias="TG_API_HASH")
    tg_session_string: str = Field(default="", alias="TG_SESSION_STRING")
    tg_channels: str = Field(default="", alias="TG_CHANNELS")
    tg_fetch_limit: int = Field(default=50, alias="TG_FETCH_LIMIT")

    model_config = SettingsConfigDict(
        env_file=".env",
        case_sensitive=False,
        extra="ignore",
    )


settings = Settings()