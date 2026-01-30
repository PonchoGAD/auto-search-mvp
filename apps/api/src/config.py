from pydantic import Field
from pydantic_settings import BaseSettings
from typing import List, Optional


class Settings(BaseSettings):
    # =========================
    # ENV
    # =========================
    ENV: str = Field(default="dev", env="ENV")
    DEBUG: bool = Field(default=False, env="DEBUG")

    # =========================
    # API
    # =========================
    API_HOST: str = Field(default="0.0.0.0", env="API_HOST")
    API_PORT: int = Field(default=8000, env="API_PORT")

    # =========================
    # DATABASE
    # =========================
    DATABASE_URL: str = Field(
        default="postgresql://postgres:postgres@postgres:5432/auto_search",
        env="DATABASE_URL",
    )

    # =========================
    # REDIS
    # =========================
    REDIS_URL: str = Field(default="redis://redis:6379/0", env="REDIS_URL")

    # =========================
    # QDRANT
    # =========================
    QDRANT_URL: str = Field(default="http://qdrant:6333", env="QDRANT_URL")
    QDRANT_COLLECTION: str = Field(default="auto_docs", env="QDRANT_COLLECTION")

    # =========================
    # INGEST LIMITS (VPS SAFE)
    # =========================
    INGEST_MAX_DOCS_PER_SOURCE: int = 200
    INGEST_MAX_PAGES: int = 2
    INGEST_THROTTLE_SEC: float = 1.5

    # =========================
    # QUALITY FILTERS
    # =========================
    MIN_TEXT_LEN: int = 80
    MIN_PRICE_RUB: int = 150_000
    MAX_PRICE_RUB: int = 20_000_000
    MIN_YEAR: int = 1995
    MAX_MILEAGE_KM: int = 400_000

    BLACKLIST_WORDS: List[str] = [
        "ищу",
        "куплю",
        "вопрос",
        "ремонт",
        "диагностика",
        "запчасти",
        "разбор",
        "ошибка",
        "проблема",
    ]

    # =========================
    # SECURITY (FUTURE)
    # =========================
    API_KEY: Optional[str] = None

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        "extra": "ignore",
    }


settings = Settings()
