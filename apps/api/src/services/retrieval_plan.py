# apps/api/src/services/retrieval_plan.py
from __future__ import annotations

from typing import Any, Dict, List

from domain.query_schema import StructuredQuery
from services.taxonomy_service import taxonomy_service


def build_retrieval_plan(query: StructuredQuery) -> Dict[str, Any]:
    brand_aliases: List[str] = []
    model_aliases: List[str] = []

    if query.brand:
        brand_aliases = taxonomy_service.get_brand_aliases(query.brand)

    if query.brand and query.model:
        model_aliases = taxonomy_service.get_model_aliases(query.brand, query.model)

    return {
        "brand": query.brand,
        "model": query.model,
        "brand_aliases": brand_aliases,
        "model_aliases": model_aliases,
        "keywords": list(query.keywords or []),
        "exclusions": list(query.exclusions or []),
        "year_min": query.year_min,
        "price_max": query.price_max,
        "mileage_max": query.mileage_max,
        "fuel": query.fuel,
        "paint_condition": query.paint_condition,
        "city": query.city,
    }