# apps/api/src/api/v1/analytics.py

from fastapi import APIRouter
from typing import List, Dict

from services.analytics import AnalyticsService

router = APIRouter(prefix="/analytics", tags=["Analytics"])


# =====================================================
# SEARCH ANALYTICS
# =====================================================

@router.get(
    "/top-queries",
    summary="Top search queries",
)
def top_queries(limit: int = 10) -> List[Dict]:
    """
    Самые частые поисковые запросы пользователей.
    """
    service = AnalyticsService()
    try:
        return service.top_queries(limit=limit)
    finally:
        service.close()


@router.get(
    "/empty-queries",
    summary="Queries with zero results",
)
def empty_queries(limit: int = 10) -> List[Dict]:
    """
    Запросы без результатов — точки роста продукта.
    """
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
def top_brands(limit: int = 10) -> List[Dict]:
    """
    Самые популярные бренды в результатах поиска.
    """
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
def source_noise_ratio() -> List[Dict]:
    """
    Качество источников:
    - сколько запросов
    - сколько пустых результатов
    - noise_ratio
    """
    service = AnalyticsService()
    try:
        return service.source_noise_ratio()
    finally:
        service.close()


# =====================================================
# RETENTION
# =====================================================

@router.get(
    "/recent-searches",
    summary="Recent searches (retention)",
)
def recent_searches(limit: int = 5):
    """
    Последние поиски — основа retention / repeat search UX.
    """
    service = AnalyticsService()
    try:
        return service.get_recent_searches(limit=limit)
    finally:
        service.close()


# =====================================================
# DATA SIGNALS (PROMPT 23.4)
# =====================================================

@router.get(
    "/data-signals",
    summary="Product growth signals",
)
def data_signals():
    """
    Продуктовые сигналы роста:
    - no_results_rate
    - brand_gap
    - noisy_source
    """
    service = AnalyticsService()
    try:
        return service.data_signals()
    finally:
        service.close()
