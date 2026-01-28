# apps/api/src/api/v1/search_history.py

from fastapi import APIRouter
from typing import List
from pydantic import BaseModel
from datetime import datetime

from db.session import SessionLocal
from db.models import SearchHistory

router = APIRouter(prefix="/search", tags=["Retention"])


# =========================
# RESPONSE SCHEMA
# =========================

class SearchHistoryItem(BaseModel):
    id: int
    raw_query: str
    structured_query: dict
    results_count: int
    empty_result: bool
    created_at: datetime

    class Config:
        orm_mode = True


# =========================
# ENDPOINT
# =========================

@router.get(
    "/history",
    response_model=List[SearchHistoryItem],
    summary="Search history (retention)"
)
def get_search_history(limit: int = 50):
    """
    Возвращает историю поисков.
    Используется для:
    - повторных запросов
    - retention
    - аналитики спроса
    """

    session = SessionLocal()
    try:
        rows = (
            session.query(SearchHistory)
            .order_by(SearchHistory.id.desc())
            .limit(limit)
            .all()
        )

        return rows

    finally:
        session.close()
