from pydantic import BaseModel, Field
from typing import List, Dict, Any

from domain.query_schema import StructuredQuery
from services.taxonomy_service import taxonomy_service


class RetrievalPlan(BaseModel):
    semantic_query: str
    expanded_terms: List[str] = Field(default_factory=list)
    negative_terms: List[str] = Field(default_factory=list)
    filters: Dict[str, Any] = Field(default_factory=dict)


def build_retrieval_plan(query: StructuredQuery) -> RetrievalPlan:
    expanded_terms: List[str] = []

    if query.brand:
        expanded_terms.extend(taxonomy_service.get_brand_aliases(query.brand))

    if query.brand and query.model:
        expanded_terms.extend(taxonomy_service.get_model_aliases(query.brand, query.model))

    semantic_parts = []

    if query.raw_query:
        semantic_parts.append(query.raw_query)

    if query.brand:
        semantic_parts.append(f"brand {query.brand}")

    if query.model:
        semantic_parts.append(f"model {query.model}")

    if query.fuel:
        semantic_parts.append(f"fuel {query.fuel}")

    if query.paint_condition:
        semantic_parts.append(f"paint {query.paint_condition}")

    filters = {}
    if query.brand:
        filters["brand"] = query.brand
    if query.model:
        filters["model"] = query.model
    if query.fuel:
        filters["fuel"] = query.fuel
    if query.price_max is not None:
        filters["price_max"] = query.price_max
    if query.mileage_max is not None:
        filters["mileage_max"] = query.mileage_max
    if query.year_min is not None:
        filters["year_min"] = query.year_min
    if query.city:
        filters["city"] = query.city

    return RetrievalPlan(
        semantic_query=" | ".join(semantic_parts).strip(),
        expanded_terms=list(dict.fromkeys(expanded_terms)),
        negative_terms=query.exclusions[:],
        filters=filters,
    )
