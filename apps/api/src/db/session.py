from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from core.settings import settings
from shared.db.base import Base

engine = create_engine(
    settings.DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_recycle=1800,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=engine,
)

_all_ = [
    "Base",
    "engine",
    "SessionLocal",
]
