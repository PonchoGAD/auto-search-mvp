import os

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from shared.db.base import Base

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set for ingest-worker")

engine = create_engine(
    DATABASE_URL,
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
