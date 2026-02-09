from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.settings import settings
from shared.db.base import Base   # ← ВАЖНО

engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)
