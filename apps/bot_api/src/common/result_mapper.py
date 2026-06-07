from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, List
from src.config import get_settings

logger = logging.getLogger(__name__)
settings = get_settings()

class ImageService:
    @staticmethod
    def normalize_images(raw_image_url: Any, raw_photos: Any) -> tuple[str | None, list[str]]:
        """
        Normalizes single image URLs and lists of photos.
        Ensures fallbacks and safe types are returned.
        """
        photos: list[str] = []
        if isinstance(raw_photos, list):
            photos = [str(p).strip() for p in raw_photos if p]
        elif isinstance(raw_photos, str) and raw_photos.strip():
            # Handle comma-separated lists if any
            photos = [p.strip() for p in raw_photos.split(",") if p.strip()]

        image_url: str | None = None
        if raw_image_url and isinstance(raw_image_url, str):
            image_url = raw_image_url.strip()
        elif photos:
            image_url = photos[0]

        return image_url, photos

class ResultMapper:
    @staticmethod
    def map_to_listing_result(payload: dict[str, Any]) -> dict[str, Any]:
        """
        Extracts, normalizes, and maps raw item payloads into a standard dict
        conforming to the expected schema without raising KeyError exceptions.
        """
        if not isinstance(payload, dict):
            logger.warning("invalid_payload_type type=%s", type(payload))
            payload = {}

        # Safe extraction of basic identification
        source_url = str(payload.get("source_url") or payload.get("url") or "").strip()
        title = str(payload.get("title") or "").strip()
        brand = str(payload.get("brand") or "").strip()
        model = str(payload.get("model") or "").strip()
        
        # Safe numeric parsing
        price = payload.get("price")
        year = payload.get("year")
        mileage = payload.get("mileage")

        # Generate custom unique listing_id if missing
        listing_id = str(payload.get("listing_id") or payload.get("id") or "").strip()
        if not listing_id:
            # Fallback hash generation
            hash_base = f"{source_url}_{title}_{price}"
            listing_id = hashlib.sha1(hash_base.encode("utf-8")).hexdigest()[:24]

        image_url, photos = ImageService.normalize_images(
            payload.get("image_url"),
            payload.get("photos")
        )

        # Standardized schema matching SearchResultItem
        return {
            "listing_id": listing_id,
            "title": title or f"{brand} {model}".strip() or "Объявление",
            "brand": brand or None,
            "model": model or None,
            "year": int(year) if year is not None and str(year).isdigit() else None,
            "mileage": int(mileage) if mileage is not None and str(mileage).isdigit() else None,
            "price": str(price) if price is not None else None,
            "currency": str(payload.get("currency") or settings.DEFAULT_CURRENCY).upper().strip(),
            "fuel": str(payload.get("fuel") or "").strip() or None,
            "region": str(payload.get("region") or payload.get("city") or "").strip() or None,
            "city": str(payload.get("city") or "").strip() or None,
            "color": str(payload.get("color") or "").strip() or None,
            "condition": str(payload.get("condition") or "").strip() or None,
            "paint_condition": str(payload.get("paint_condition") or "").strip() or None,
            "score": payload.get("score"),
            "why_match": str(payload.get("why_match") or "").strip() or None,
            "source_url": source_url or None,
            "source_name": str(payload.get("source_name") or payload.get("source") or "Источник").strip(),
            "image_url": image_url,
            "photos": photos,
            "created_at": payload.get("created_at"),
            "created_at_ts": payload.get("created_at_ts"),
            "score_breakdown": payload.get("score_breakdown") or {},
            "raw_payload": payload,
        }

    @classmethod
    def map_many(cls, payloads: list[dict[str, Any]]) -> list[dict[str, Any]]:
        """
        Batch transformation helper.
        """
        if not isinstance(payloads, list):
            return []
        return [cls.map_to_listing_result(item) for item in payloads if item]