from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from sqlalchemy import Integer, cast, desc, func, select
from sqlalchemy.orm import Session

from src.db.models import SearchHistory


class SearchHistoryRepository:
    def __init__(self, db: Session) -> None:
        self.db = db

    def create(
        self,
        user_id: int,
        raw_query: str,
        query_payload: dict[str, Any] | None = None,
        results_count: int = 0,
        latency_ms: Optional[int] = None,
        empty_result: bool = False,
    ) -> SearchHistory:
        entity = SearchHistory(
            user_id=user_id,
            raw_query=raw_query,
            query_payload=query_payload or {},
            results_count=results_count,
            latency_ms=latency_ms,
            empty_result=empty_result,
        )
        self.db.add(entity)
        self.db.commit()
        self.db.refresh(entity)
        return entity

    def list_by_user(self, user_id: int, limit: int = 50) -> list[SearchHistory]:
        stmt = (
            select(SearchHistory)
            .where(SearchHistory.user_id == user_id)
            .order_by(SearchHistory.created_at.desc())
            .limit(limit)
        )
        return list(self.db.execute(stmt).scalars().all())

    def last_by_user(self, user_id: int) -> Optional[SearchHistory]:
        stmt = (
            select(SearchHistory)
            .where(SearchHistory.user_id == user_id)
            .order_by(SearchHistory.created_at.desc())
            .limit(1)
        )
        return self.db.execute(stmt).scalars().first()

    def total_searches(self) -> int:
        stmt = select(func.count(SearchHistory.id))
        return int(self.db.execute(stmt).scalar() or 0)

    def searches_today(self) -> int:
        now = datetime.now(timezone.utc)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)

        stmt = select(func.count(SearchHistory.id)).where(SearchHistory.created_at >= start)
        return int(self.db.execute(stmt).scalar() or 0)

    def searches_last_24h(self) -> int:
        since = datetime.now(timezone.utc) - timedelta(hours=24)

        stmt = select(func.count(SearchHistory.id)).where(SearchHistory.created_at >= since)
        return int(self.db.execute(stmt).scalar() or 0)

    def empty_results_today(self) -> int:
        now = datetime.now(timezone.utc)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)

        stmt = select(func.count(SearchHistory.id)).where(
            SearchHistory.created_at >= start,
            SearchHistory.empty_result.is_(True),
        )
        return int(self.db.execute(stmt).scalar() or 0)

    def avg_latency_today(self) -> int | None:
        now = datetime.now(timezone.utc)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)

        stmt = select(func.avg(SearchHistory.latency_ms)).where(
            SearchHistory.created_at >= start,
            SearchHistory.latency_ms.is_not(None),
        )
        value = self.db.execute(stmt).scalar()

        if value is None:
            return None

        return int(value)

    def top_queries_today(self, limit: int = 20) -> list[dict[str, Any]]:
        now = datetime.now(timezone.utc)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)

        stmt = (
            select(
                SearchHistory.raw_query,
                func.count(SearchHistory.id).label("count"),
                func.avg(SearchHistory.latency_ms).label("avg_latency_ms"),
                func.sum(cast(SearchHistory.empty_result, Integer)).label("empty_count"),
            )
            .where(SearchHistory.created_at >= start)
            .group_by(SearchHistory.raw_query)
            .order_by(desc("count"))
            .limit(limit)
        )

        rows = self.db.execute(stmt).all()

        result: list[dict[str, Any]] = []
        for row in rows:
            result.append(
                {
                    "query": row.raw_query,
                    "count": int(row.count or 0),
                    "avg_latency_ms": int(row.avg_latency_ms) if row.avg_latency_ms is not None else None,
                    "empty_count": int(row.empty_count or 0),
                }
            )

        return result

    def latest_searches(self, limit: int = 50) -> list[dict[str, Any]]:
        stmt = (
            select(SearchHistory)
            .order_by(SearchHistory.created_at.desc(), SearchHistory.id.desc())
            .limit(limit)
        )

        rows = list(self.db.execute(stmt).scalars().all())
        return [self._to_dict(row) for row in rows]

    def search_stats(self) -> dict[str, Any]:
        return {
            "searches_today": self.searches_today(),
            "searches_24h": self.searches_last_24h(),
            "total_searches": self.total_searches(),
            "empty_results_today": self.empty_results_today(),
            "avg_latency_today_ms": self.avg_latency_today(),
            "top_queries_today": self.top_queries_today(limit=20),
        }

    def _to_dict(self, row: SearchHistory) -> dict[str, Any]:
        return {
            "id": row.id,
            "user_id": row.user_id,
            "raw_query": row.raw_query,
            "query_payload": row.query_payload,
            "results_count": row.results_count,
            "latency_ms": row.latency_ms,
            "empty_result": row.empty_result,
            "created_at": row.created_at,
        }