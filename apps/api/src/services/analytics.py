from typing import Dict, List, Optional
from sqlalchemy import func, case

from db.session import SessionLocal
from db.models import SearchHistory, NormalizedDocument, RawDocument


class AnalyticsService:
    """
    Analytics / Retention service.

    Принципы:
    - read-only
    - никаких зависимостей от поиска
    - стабильный JSON-контракт под UI
    """

    def __init__(self):
        self.session = SessionLocal()

    # =====================================================
    # RETENTION / REPEAT SEARCH
    # =====================================================

    def get_recent_searches(
        self,
        user_id: Optional[int] = None,
        limit: int = 10,
    ) -> List[Dict]:
        q = self.session.query(SearchHistory)

        if user_id is not None:
            q = q.filter(SearchHistory.user_id == user_id)

        rows = (
            q.order_by(SearchHistory.created_at.desc())
            .limit(min(max(limit, 1), 20))
            .all()
        )

        return [
            {
                "id": r.id,
                "query": r.raw_query,
                "structured_query": r.structured_query,
                "results_count": r.results_count,
                "empty_result": r.empty_result,
                "created_at": r.created_at.isoformat()
                if r.created_at
                else None,
            }
            for r in rows
        ]

    # =====================================================
    # SEARCH ANALYTICS
    # =====================================================

    def top_queries(self, limit: int = 10) -> List[Dict]:
        rows = (
            self.session.query(
                SearchHistory.raw_query.label("query"),
                func.count(SearchHistory.id).label("count"),
            )
            .filter(SearchHistory.raw_query.isnot(None))
            .group_by(SearchHistory.raw_query)
            .order_by(func.count(SearchHistory.id).desc())
            .limit(limit)
            .all()
        )

        return [
            {
                "query": r.query,
                "count": int(r.count),
            }
            for r in rows
        ]

    def empty_queries(self, limit: int = 10) -> List[Dict]:
        rows = (
            self.session.query(
                SearchHistory.raw_query.label("query"),
                func.count(SearchHistory.id).label("count"),
            )
            .filter(SearchHistory.empty_result.is_(True))
            .group_by(SearchHistory.raw_query)
            .order_by(func.count(SearchHistory.id).desc())
            .limit(limit)
            .all()
        )

        return [
            {
                "query": r.query,
                "count": int(r.count),
            }
            for r in rows
        ]

    # =====================================================
    # BRAND ANALYTICS
    # =====================================================

    def top_brands(self, limit: int = 10) -> List[Dict]:
        rows = (
            self.session.query(
                NormalizedDocument.brand.label("brand"),
                func.count(NormalizedDocument.id).label("count"),
            )
            .filter(NormalizedDocument.brand.isnot(None))
            .group_by(NormalizedDocument.brand)
            .order_by(func.count(NormalizedDocument.id).desc())
            .limit(limit)
            .all()
        )

        return [
            {
                "brand": r.brand,
                "count": int(r.count),
            }
            for r in rows
        ]

    # =====================================================
    # SOURCE QUALITY
    # =====================================================

    def source_noise_ratio(self) -> List[Dict]:
        rows = (
            self.session.query(
                SearchHistory.source.label("source"),
                func.count(SearchHistory.id).label("total"),
                func.sum(
                    case(
                        (SearchHistory.empty_result.is_(True), 1),
                        else_=0,
                    )
                ).label("empty"),
            )
            .group_by(SearchHistory.source)
            .all()
        )

        result: List[Dict] = []

        for r in rows:
            total = int(r.total or 0)
            empty = int(r.empty or 0)
            noise_ratio = round(empty / total, 3) if total else 0.0

            result.append(
                {
                    "source": r.source,
                    "total_queries": total,
                    "empty_queries": empty,
                    "noise_ratio": noise_ratio,
                }
            )

        return result

    # =====================================================
    # DATA SIGNALS (KILLER FEATURE)
    # =====================================================

    def no_results_rate(self) -> Dict:
        total = self.session.query(func.count(SearchHistory.id)).scalar() or 0
        empty = (
            self.session.query(func.count(SearchHistory.id))
            .filter(SearchHistory.empty_result.is_(True))
            .scalar()
            or 0
        )

        rate = round(empty / total, 3) if total else 0.0

        return {
            "total_searches": total,
            "empty_searches": empty,
            "no_results_rate": rate,
        }

    def brand_gap(self, limit: int = 10) -> List[Dict]:
        searched = (
            self.session.query(
                SearchHistory.structured_query["brand"]
                .as_string()
                .label("brand"),
                func.count(SearchHistory.id).label("count"),
            )
            .filter(SearchHistory.structured_query["brand"].isnot(None))
            .group_by("brand")
            .order_by(func.count(SearchHistory.id).desc())
            .limit(limit)
            .all()
        )

        existing_brands = {
            r.brand
            for r in self.session.query(NormalizedDocument.brand)
            .filter(NormalizedDocument.brand.isnot(None))
            .distinct()
            .all()
        }

        gaps: List[Dict] = []

        for r in searched:
            if r.brand not in existing_brands:
                gaps.append(
                    {
                        "brand": r.brand,
                        "search_count": int(r.count),
                        "documents": 0,
                        "signal": "brand_gap",
                    }
                )

        return gaps

    def noisy_source(self) -> List[Dict]:
        raw_counts = {
            r.source: int(r.count)
            for r in self.session.query(
                RawDocument.source.label("source"),
                func.count(RawDocument.id).label("count"),
            )
            .group_by(RawDocument.source)
            .all()
        }

        norm_counts = {
            r.source: int(r.count)
            for r in self.session.query(
                NormalizedDocument.source.label("source"),
                func.count(NormalizedDocument.id).label("count"),
            )
            .group_by(NormalizedDocument.source)
            .all()
        }

        result: List[Dict] = []

        for source, raw_cnt in raw_counts.items():
            norm_cnt = norm_counts.get(source, 0)
            ratio = round(norm_cnt / raw_cnt, 3) if raw_cnt else 0.0

            if ratio < 0.5:
                result.append(
                    {
                        "source": source,
                        "raw_documents": raw_cnt,
                        "normalized_documents": norm_cnt,
                        "quality_ratio": ratio,
                        "signal": "noisy_source",
                    }
                )

        return result

    def data_signals(self) -> Dict:
        return {
            "no_results_rate": self.no_results_rate(),
            "brand_gap": self.brand_gap(),
            "noisy_source": self.noisy_source(),
        }

    # =====================================================
    # CLEANUP
    # =====================================================

    def close(self):
        self.session.close()
