from typing import Optional, List
from pydantic import BaseModel, Field, ConfigDict


class StructuredQuery(BaseModel):
    """
    Canonical structured query used across the search pipeline.

    query_parser → StructuredQuery → search_service → vector_db
    """

    model_config = ConfigDict(extra="forbid")

    # raw input
    raw_query: Optional[str] = None

    # brand + model
    brand: Optional[str] = None
    brands: List[str] = Field(default_factory=list) # 🔥 ДОБАВЛЕНО: для поиска "или"
    brand_confidence: float = 0.0
    model: Optional[str] = None

    # numeric filters
    price_max: Optional[int] = None
    mileage_max: Optional[int] = None
    year_min: Optional[int] = None
    year_max: Optional[int] = None # 🔥 ДОБАВЛЕНО: для "2023 года" (точно)

    # categorical filters
    fuel: Optional[str] = None
    paint_condition: Optional[str] = None

    # geo
    city: Optional[str] = None
    region: Optional[str] = None

    # semantic hints
    keywords: List[str] = Field(default_factory=list)

    # negative filters
    exclusions: List[str] = Field(default_factory=list)