from __future__ import annotations

from sqlalchemy.orm import Session

from src.db.session import get_db


def db_session() -> Session:
    return next(get_db())