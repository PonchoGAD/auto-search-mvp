from fastapi import APIRouter
from typing import List, Optional
from pydantic import BaseModel
from datetime import datetime

from db.session import SessionLocal
from db.models import SearchHistory

router = APIRouter()


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


# =========================
# ENDPOINT
# =========================

@router.get(
    "/search/history",
    response_model=List[SearchHistoryItem],
    summary="Search history (retention)"
)
def get_search_history(limit: int = 50):
    session = SessionLocal()
    try:
        rows = (
            session.query(SearchHistory)
            .order_by(SearchHistory.id.desc())
            .limit(limit)
            .all()
        )

        return [
            {
                "id": r.id,
                "raw_query": r.raw_query,
                "structured_query": r.structured_query,
                "results_count": r.results_count,
                "empty_result": r.empty_result,
                "created_at": r.created_at,
            }
            for r in rows
        ]
    finally:
        session.close()
