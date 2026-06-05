from __future__ import annotations

from typing import Optional, List, Dict, Any

from pydantic import BaseModel, Field


class ListingResult(BaseModel):
    """
    Unified search result contract.

    Используется:

    parser
      ↓
    search_service
      ↓
    qdrant
      ↓
    api
      ↓
    bot_api
      ↓
    telegram bot

    Нельзя ломать контракт.
    Новые поля только добавляются.
    """

    # =====================================================
    # STABLE IDS
    # =====================================================

    listing_id: str = Field(
        ...,
        description="Stable listing id"
    )

    raw_id: Optional[int] = None
    normalized_id: Optional[int] = None
    doc_id: Optional[int] = None
    chunk_id: Optional[int] = None

    # =====================================================
    # MAIN
    # =====================================================

    title: str = ""

    brand: Optional[str] = None
    model: Optional[str] = None

    price: Optional[int] = None
    mileage: Optional[int] = None
    year: Optional[int] = None

    fuel: Optional[str] = None

    city: Optional[str] = None
    region: Optional[str] = None

    color: Optional[str] = None
    condition: Optional[str] = None
    paint_condition: Optional[str] = None

    currency: Optional[str] = None

    # =====================================================
    # SOURCE
    # =====================================================

    source: Optional[str] = None
    source_name: Optional[str] = None

    source_url: Optional[str] = None

    # =====================================================
    # TELEGRAM / MEDIA
    # =====================================================

    image_url: Optional[str] = None

    photos: Optional[List[str]] = None

    # =====================================================
    # CONTENT
    # =====================================================

    content: Optional[str] = None
    title_text: Optional[str] = None

    # =====================================================
    # QUALITY
    # =====================================================

    score: Optional[float] = 0.0

    why_match: Optional[str] = None

    score_breakdown: Optional[
        Dict[str, float]
    ] = None

    quality_score: Optional[float] = None
    doc_quality: Optional[int] = None

    # =====================================================
    # TIME
    # =====================================================

    created_at: Optional[str] = None

    created_at_ts: Optional[int] = None

    created_at_source: Optional[str] = None

    # =====================================================
    # EXTRA
    # =====================================================

    sale_intent: Optional[int] = None

    vector_type: Optional[str] = None

    brand_model: Optional[str] = None

    chunk_index: Optional[int] = None

    # =====================================================
    # DEBUG
    # =====================================================

    debug: Optional[
        Dict[str, Any]
    ] = None

    # =====================================================
    # TG BOT CONTRACT
    # =====================================================

    def to_telegram_dict(
        self
    ) -> Dict[str, Any]:

        return {

            "id":
                self.listing_id,

            "title":
                self.title,

            "price":
                self.price,

            "image_url":
                self.image_url,

            "url":
                self.source_url
        }

    # =====================================================
    # SAFE JSON
    # =====================================================

    def to_dict(
        self
    ) -> Dict[str, Any]:

        if hasattr(
            self,
            "model_dump"
        ):

            return self.model_dump()

        return self.dict()

    class Config:

        populate_by_name = True

        extra = "allow"

        json_schema_extra = {

            "example": {

                "listing_id":
                    "avito_123456",

                "title":
                    "BMW X5 2020",

                "brand":
                    "bmw",

                "model":
                    "x5",

                "price":
                    4200000,

                "mileage":
                    68000,

                "year":
                    2020,

                "fuel":
                    "diesel",

                "city":
                    "Москва",

                "region":
                    "Московская область",

                "source":
                    "avito",

                "source_url":
                    "https://...",

                "image_url":
                    "https://img.jpg",

                "photos": [
                    "https://1.jpg",
                    "https://2.jpg"
                ],

                "score":
                    0.93,

                "created_at":
                    "2026-05-25T10:00:00+00:00",

                "created_at_ts":
                    1770000000
            }
        }