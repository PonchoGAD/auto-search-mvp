# apps/api/src/api/v1/analytics.py

from fastapi import APIRouter
from typing import List, Dict, Any, Optional

from services.analytics import AnalyticsService

router = APIRouter(prefix="/analytics", tags=["Analytics"])


# =====================================================
# RETENTION / REPEAT SEARCH
# =====================================================

@router.get(
    "/recent-searches",
    summary="Recent searches (retention / repeat search UX)",
)
def recent_searches(
    limit: int = 10,
    user_id: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    Последние поиски — основа retention / repeat search UX.

    Возвращает минимум:
    - raw_query
    - structured_query
    + полезные поля (created_at, results_count, empty_result)
    """
    # жёсткое ограничение: 1..20
    limit = min(max(limit, 1), 20)

    service = AnalyticsService()
    try:
        # Важно: твой AnalyticsService (полный) уже поддерживает user_id опционально
        return service.get_recent_searches(user_id=user_id, limit=limit)
    finally:
        service.close()


# =====================================================
# TOP QUERIES
# =====================================================

@router.get(
    "/top-queries",
    summary="Top search queries",
)
def top_queries(limit: int = 10) -> List[Dict[str, Any]]:
    limit = min(max(limit, 1), 50)

    service = AnalyticsService()
    try:
        return service.top_queries(limit=limit)
    finally:
        service.close()


# =====================================================
# EMPTY QUERIES
# =====================================================

@router.get(
    "/empty-queries",
    summary="Queries with zero results",
)
def empty_queries(limit: int = 10) -> List[Dict[str, Any]]:
    limit = min(max(limit, 1), 50)

    service = AnalyticsService()
    try:
        return service.empty_queries(limit=limit)
    finally:
        service.close()


# =====================================================
# BRAND ANALYTICS
# =====================================================

@router.get(
    "/top-brands",
    summary="Top brands in results",
)
def top_brands(limit: int = 10) -> List[Dict[str, Any]]:
    limit = min(max(limit, 1), 50)

    service = AnalyticsService()
    try:
        return service.top_brands(limit=limit)
    finally:
        service.close()


# =====================================================
# SOURCE QUALITY
# =====================================================

@router.get(
    "/source-noise",
    summary="Source noise ratio",
)
def source_noise() -> List[Dict[str, Any]]:
    service = AnalyticsService()
    try:
        return service.source_noise_ratio()
    finally:
        service.close()


# =====================================================
# DATA SIGNALS (KILLER FEATURE)
# =====================================================

@router.get(
    "/data-signals",
    summary="Product growth signals",
)
def data_signals() -> Dict[str, Any]:
    service = AnalyticsService()
    try:
        return service.data_signals()
    finally:
        service.close()
