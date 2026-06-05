from typing import Optional, List
from pydantic import BaseModel, Field, ConfigDict


class StructuredQuery(BaseModel):
    """
    Canonical structured query used across the search pipeline.

    query_parser → StructuredQuery
    → search_service
    → vector_db
    → bot_api
    → telegram_bot
    """

    model_config = ConfigDict(extra="forbid")

    # raw input
    raw_query: Optional[str] = None

    # brand + model
    brand: Optional[str] = None

    # OR search:
    # bmw или audi
    brands: List[str] = Field(default_factory=list)

    brand_confidence: float = 0.0

    model: Optional[str] = None

    # numeric filters
    price_min: Optional[int] = None
    price_max: Optional[int] = None

    mileage_min: Optional[int] = None
    mileage_max: Optional[int] = None

    year_min: Optional[int] = None
    year_max: Optional[int] = None

    # categorical filters
    fuel: Optional[str] = None
    paint_condition: Optional[str] = None

    # geo
    city: Optional[str] = None
    region: Optional[str] = None

    # semantic hints
    keywords: List[str] = Field(
        default_factory=list
    )

    # exclusions
    exclusions: List[str] = Field(
        default_factory=list
    )

    # bot-api debug
    request_id: Optional[str] = None

    debug: dict = Field(
        default_factory=dict
    )