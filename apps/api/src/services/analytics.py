# apps/api/src/services/analytics.py

from __future__ import annotations

from typing import Any, Dict, List, Optional

from sqlalchemy import case, func
from sqlalchemy.orm import Session

from db.session import SessionLocal
from db.models import SearchHistory, NormalizedDocument, RawDocument


class AnalyticsService:
    """
    Analytics / Retention service.

    Принципы:
    - read-only
    - никаких зависимостей от поиска
    - стабильный JSON-контракт под UI
    - безопасные значения по умолчанию (если таблиц/полей пока нет)
    """

    def __init__(self, session: Optional[Session] = None):
        self.session: Session = session or SessionLocal()

    # =====================================================
    # RETENTION / REPEAT SEARCH
    # =====================================================

    def get_recent_searches(
        self,
        user_id: Optional[int] = None,
        limit: int = 10,
    ) -> List[Dict[str, Any]]:
        """
        Последние поиски — основа retention / repeat search UX.

        Возвращает:
        - id
        - query (raw_query)
        - structured_query
        - results_count
        - empty_result
        - created_at (ISO)
        """
        limit = min(max(int(limit), 1), 20)

        q = self.session.query(SearchHistory)

        # user_id может отсутствовать в модели SearchHistory (в MVP). Защищаемся:
        if user_id is not None and hasattr(SearchHistory, "user_id"):
            q = q.filter(SearchHistory.user_id == user_id)

        rows = q.order_by(SearchHistory.created_at.desc()).limit(limit).all()

        out: List[Dict[str, Any]] = []
        for r in rows:
            created_at_iso = None
            try:
                if getattr(r, "created_at", None):
                    created_at_iso = r.created_at.isoformat()
            except Exception:
                created_at_iso = None

            out.append(
                {
                    "id": getattr(r, "id", None),
                    "query": getattr(r, "raw_query", None),
                    "structured_query": getattr(r, "structured_query", None),
                    "results_count": int(getattr(r, "results_count", 0) or 0),
                    "empty_result": bool(getattr(r, "empty_result", False)),
                    "created_at": created_at_iso,
                }
            )

        return out

    # =====================================================
    # SEARCH ANALYTICS
    # =====================================================

    def top_queries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Топ запросов по raw_query.
        """
        limit = min(max(int(limit), 1), 50)

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

        return [{"query": r.query, "count": int(r.count)} for r in rows]

    def empty_queries(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Запросы, которые чаще всего приводят к 0 результатов.
        """
        limit = min(max(int(limit), 1), 50)

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

        return [{"query": r.query, "count": int(r.count)} for r in rows]

    # =====================================================
    # BRAND ANALYTICS
    # =====================================================

    def top_brands(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        Топ брендов в нормализованных документах.
        """
        limit = min(max(int(limit), 1), 50)

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

        return [{"brand": r.brand, "count": int(r.count)} for r in rows]

    # =====================================================
    # SOURCE QUALITY
    # =====================================================

    def source_noise_ratio(self) -> List[Dict[str, Any]]:
        """
        "Noise" в терминах поиска: доля запросов с пустой выдачей по источнику.
        (По факту, это качество/релевантность + полнота индекса под запросы аудитории.)

        Возвращает:
        - source
        - total_queries
        - empty_queries
        - noise_ratio
        """
        # source может быть None/пустой — нормализуем
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

        result: List[Dict[str, Any]] = []

        for r in rows:
            total = int(r.total or 0)
            empty = int(r.empty or 0)
            ratio = round(empty / total, 3) if total else 0.0

            src = r.source or "unknown"
            result.append(
                {
                    "source": src,
                    "total_queries": total,
                    "empty_queries": empty,
                    "noise_ratio": ratio,
                }
            )

        # Чтобы UI стабильно показывал сначала “хуже”:
        result.sort(key=lambda x: x["noise_ratio"], reverse=True)
        return result

    # =====================================================
    # DATA SIGNALS (KILLER FEATURE)
    # =====================================================

    def no_results_rate(self) -> Dict[str, Any]:
        """
        Общая доля пустых результатов.
        """
        total = self.session.query(func.count(SearchHistory.id)).scalar() or 0
        empty = (
            self.session.query(func.count(SearchHistory.id))
            .filter(SearchHistory.empty_result.is_(True))
            .scalar()
            or 0
        )

        rate = round(float(empty) / float(total), 3) if total else 0.0

        return {
            "total_searches": int(total),
            "empty_searches": int(empty),
            "no_results_rate": float(rate),
        }

    def brand_gap(self, limit: int = 10) -> List[Dict[str, Any]]:
        """
        "Спрос есть, предложения нет": бренды, которые ищут, но которых нет в NormalizedDocument.

        Важно:
        - structured_query хранится как JSON. В Postgres это JSONB.
        - Используем ->> (as_string) чтобы достать строку.
        """
        limit = min(max(int(limit), 1), 50)

        # В некоторых MVP structured_query может быть None или не содержать brand.
        searched = (
            self.session.query(
                SearchHistory.structured_query["brand"].as_string().label("brand"),
                func.count(SearchHistory.id).label("count"),
            )
            .filter(SearchHistory.structured_query.isnot(None))
            .filter(SearchHistory.structured_query["brand"].isnot(None))
            .group_by("brand")
            .order_by(func.count(SearchHistory.id).desc())
            .limit(limit)
            .all()
        )

        existing_brands = {
            r[0]
            for r in self.session.query(NormalizedDocument.brand)
            .filter(NormalizedDocument.brand.isnot(None))
            .distinct()
            .all()
        }

        gaps: List[Dict[str, Any]] = []
        for r in searched:
            brand = r.brand
            if not brand:
                continue

            if brand not in existing_brands:
                gaps.append(
                    {
                        "brand": brand,
                        "search_count": int(r.count),
                        "documents": 0,
                        "signal": "brand_gap",
                    }
                )

        return gaps

    def noisy_source(self, quality_threshold: float = 0.5) -> List[Dict[str, Any]]:
        """
        "Шумный источник" = в raw много, в normalized мало.
        quality_ratio = normalized / raw

        quality_threshold по умолчанию 0.5 (если < 0.5 — источник подозрительный)
        """
        try:
            thr = float(quality_threshold)
        except Exception:
            thr = 0.5

        raw_counts = {
            r.source or "unknown": int(r.count or 0)
            for r in self.session.query(
                RawDocument.source.label("source"),
                func.count(RawDocument.id).label("count"),
            )
            .group_by(RawDocument.source)
            .all()
        }

        norm_counts = {
            r.source or "unknown": int(r.count or 0)
            for r in self.session.query(
                NormalizedDocument.source.label("source"),
                func.count(NormalizedDocument.id).label("count"),
            )
            .group_by(NormalizedDocument.source)
            .all()
        }

        result: List[Dict[str, Any]] = []

        for source, raw_cnt in raw_counts.items():
            norm_cnt = norm_counts.get(source, 0)
            ratio = round(float(norm_cnt) / float(raw_cnt), 3) if raw_cnt else 0.0

            if raw_cnt > 0 and ratio < thr:
                result.append(
                    {
                        "source": source,
                        "raw_documents": int(raw_cnt),
                        "normalized_documents": int(norm_cnt),
                        "quality_ratio": float(ratio),
                        "signal": "noisy_source",
                    }
                )

        # сначала самые “плохие”
        result.sort(key=lambda x: x["quality_ratio"])
        return result

    def data_signals(self) -> Dict[str, Any]:
        """
        Единая точка для UI "инсайтов".
        """
        return {
            "no_results_rate": self.no_results_rate(),
            "brand_gap": self.brand_gap(limit=10),
            "noisy_source": self.noisy_source(quality_threshold=0.5),
        }

    # =====================================================
    # CLEANUP
    # =====================================================

    def close(self) -> None:
        try:
            self.session.close()
        except Exception:
            pass
