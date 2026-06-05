from __future__ import annotations

import hashlib
import os
from typing import Any, Dict, Optional

from fastapi import APIRouter, HTTPException, status
from pydantic import BaseModel

from integrations.vector_db.qdrant import COLLECTION_NAME, QdrantStore

try:
    from qdrant_client.models import Filter, FieldCondition, MatchValue
except Exception:
    Filter = None
    FieldCondition = None
    MatchValue = None

router = APIRouter()

LISTINGS_FALLBACK_MAX_SCAN = int(os.getenv("LISTINGS_FALLBACK_MAX_SCAN", "20000"))


class ListingDetailsResponse(BaseModel):
    listing_id: str

    title: Optional[str] = None
    brand: Optional[str] = None
    model: Optional[str] = None
    year: Optional[int] = None
    mileage: Optional[int] = None
    price: Optional[int] = None
    currency: Optional[str] = "RUB"

    fuel: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None
    paint_condition: Optional[str] = None

    source_url: Optional[str] = None
    source_name: Optional[str] = None

    image_url: Optional[str] = None
    photos: Optional[list[str]] = None

    created_at: Optional[str] = None
    created_at_ts: Optional[int] = None

    raw_payload: Optional[dict[str, Any]] = None


def build_listing_id(
    source_url: Optional[str],
    brand: Optional[str],
    model: Optional[str],
    year: Optional[int],
    price: Optional[int],
    mileage: Optional[int],
) -> str:
    raw = "|".join(
        [
            str(source_url or "").strip().lower(),
            str(brand or "").strip().lower(),
            str(model or "").strip().lower(),
            str(year or ""),
            str(price or ""),
            str(mileage or ""),
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()[:24]


def _payload_listing_id(payload: Dict[str, Any]) -> str:
    existing = payload.get("listing_id")

    if existing:
        return str(existing).strip()

    return build_listing_id(
        source_url=payload.get("source_url"),
        brand=payload.get("brand"),
        model=payload.get("model"),
        year=payload.get("year"),
        price=payload.get("price"),
        mileage=payload.get("mileage"),
    )


def _normalize_photos(payload: Dict[str, Any]) -> list[str]:
    photos = payload.get("photos")

    if isinstance(photos, list):
        return [str(x).strip() for x in photos if str(x).strip()]

    image_url = payload.get("image_url")

    if isinstance(image_url, str) and image_url.strip():
        return [image_url.strip()]

    return []


def _build_response(payload: Dict[str, Any]) -> ListingDetailsResponse:
    listing_id = _payload_listing_id(payload)
    photos = _normalize_photos(payload)

    return ListingDetailsResponse(
        listing_id=listing_id,
        title=payload.get("title") or payload.get("title_text"),
        brand=payload.get("brand"),
        model=payload.get("model"),
        year=payload.get("year"),
        mileage=payload.get("mileage"),
        price=payload.get("price"),
        currency=payload.get("currency") or "RUB",
        fuel=payload.get("fuel"),
        region=payload.get("region"),
        city=payload.get("city") or payload.get("region"),
        paint_condition=payload.get("paint_condition"),
        source_url=payload.get("source_url"),
        source_name=payload.get("source") or payload.get("source_name"),
        image_url=payload.get("image_url") or (photos[0] if photos else None),
        photos=photos,
        created_at=payload.get("created_at"),
        created_at_ts=payload.get("created_at_ts"),
        raw_payload=payload,
    )


def _find_by_listing_id_exact(store: QdrantStore, listing_id: str) -> Optional[Dict[str, Any]]:
    if not Filter or not FieldCondition or not MatchValue:
        return None

    try:
        # 1. Попытка точного поиска по ключу listing_id
        response = store.client.scroll(
            collection_name=COLLECTION_NAME,
            scroll_filter=Filter(
                must=[
                    FieldCondition(
                        key="listing_id",
                        match=MatchValue(value=listing_id),
                    )
                ]
            ),
            limit=1,
            with_payload=True,
            with_vectors=False,
        )

        points = response[0] if isinstance(response, tuple) else []
        if points:
            return points[0].payload or {}

        # 2. Быстрый fallback: Попытка точного поиска по source_url (если в качестве id передан URL)
        response_url = store.client.scroll(
            collection_name=COLLECTION_NAME,
            scroll_filter=Filter(
                must=[
                    FieldCondition(
                        key="source_url",
                        match=MatchValue(value=listing_id),
                    )
                ]
            ),
            limit=1,
            with_payload=True,
            with_vectors=False,
        )

        points_url = response_url[0] if isinstance(response_url, tuple) else []
        if points_url:
            return points_url[0].payload or {}

    except Exception as exc:
        print(f"[LISTINGS][WARN] exact lookup failed: {exc}", flush=True)

    return None


def _find_by_listing_id_fallback(store: QdrantStore, listing_id: str) -> Optional[Dict[str, Any]]:
    offset = None
    scanned = 0
    max_scan = LISTINGS_FALLBACK_MAX_SCAN

    while scanned < max_scan:
        try:
            points, offset = store.client.scroll(
                collection_name=COLLECTION_NAME,
                offset=offset,
                limit=256,
                with_payload=True,
                with_vectors=False,
            )
        except Exception as exc:
            print(f"[LISTINGS][WARN] fallback scroll failed: {exc}", flush=True)
            return None

        if not points:
            return None

        for point in points:
            payload = point.payload or {}

            # Сравнение как со сохраненным listing_id, так и со сгенерированным fingerprint
            if str(payload.get("listing_id")).strip() == listing_id:
                return payload

            if _payload_listing_id(payload) == listing_id:
                return payload

        scanned += len(points)

        if offset is None:
            break

    return None


@router.get(
    "/listings/{listing_id}",
    response_model=ListingDetailsResponse,
    summary="Get listing details by listing_id",
)
def get_listing(listing_id: str) -> ListingDetailsResponse:
    listing_id = (listing_id or "").strip()

    if not listing_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="listing_id is required",
        )

    store = QdrantStore()

    payload = _find_by_listing_id_exact(store, listing_id)

    if payload is None:
        payload = _find_by_listing_id_fallback(store, listing_id)

    if payload is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Listing not found",
        )

    return _build_response(payload)