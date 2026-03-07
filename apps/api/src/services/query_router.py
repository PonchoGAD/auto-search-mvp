# apps/api/src/services/query_router.py

from typing import Literal

from services.query_parser import StructuredQuery


RouteType = Literal["structured", "brand_only", "semantic"]


def route_query(structured: StructuredQuery) -> RouteType:
    """
    structured:
      brand/model + numeric filters
    brand_only:
      only brand/model search
    semantic:
      natural language / weak structure
    """

    has_brand = bool(getattr(structured, "brand", None))
    has_model = bool(getattr(structured, "model", None))
    has_numeric = any([
        getattr(structured, "price_max", None) is not None,
        getattr(structured, "mileage_max", None) is not None,
        getattr(structured, "year_min", None) is not None,
    ])
    has_fuel = bool(getattr(structured, "fuel", None))
    has_keywords = bool(getattr(structured, "keywords", []))

    if has_brand and (has_model or has_numeric or has_fuel):
        return "structured"

    if has_brand and not has_numeric and not has_fuel and not has_keywords:
        return "brand_only"

    if has_brand and not has_numeric and not has_fuel:
        return "brand_only"

    return "semantic"