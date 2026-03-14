#  apps\api\src\data_pipeline\index.py

import os
import hashlib
import re
def _clean_text(text: str) -> str:
    if not text:
        return ""

    text = text.replace("\u00A0", " ").replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()

from typing import List
from datetime import datetime, timezone
from pathlib import Path
import yaml

from qdrant_client.models import PointStruct

from db.session import SessionLocal, engine
from db.models import Base, NormalizedDocument, DocumentChunk
from integrations.vector_db.qdrant import QdrantStore
from shared.embeddings.provider import embed_text


# =====================================================
# VECTOR CONFIG (FIXED)
# =====================================================

VECTOR_SIZE = 768


# =====================================================
# ⚠️ CRITICAL ADAPTER (DO NOT REMOVE)
# =====================================================

def deterministic_embedding(text: str) -> List[float]:
    """
    ✅ ADAPTER FOR SearchService.

    SearchService IMPORTS THIS FUNCTION.
    Therefore it MUST return the SAME embedding
    that was used during indexing.
    """
    return embed_text(text)


# =====================================================
# PAYLOAD NORMALIZATION HELPERS
# =====================================================

ALLOWED_FUELS = {"petrol", "diesel", "hybrid", "electric", "gas", "gas_petrol"}

def _norm_str(v: object) -> str | None:
    if isinstance(v, str):
        v = v.strip().lower()
        return v or None
    return None

def _norm_int(v: object) -> int | None:
    if v is None:
        return None
    try:
        if isinstance(v, str):
            v = v.replace(" ", "")
        return int(v)
    except Exception:
        return None


# =====================================================
# STRUCTURED TEXT BUILDER
# =====================================================

def build_structured_text(doc: NormalizedDocument) -> str:
    """
    Structured semantic representation.
    Улучшает recall для фильтров и semantic matching.
    """

    parts = []

    if getattr(doc, "brand", None) and getattr(doc, "model", None):
        parts.append(f"{doc.brand} {doc.model}")

    if getattr(doc, "brand", None):
        parts.append(f"brand {doc.brand}")

    if getattr(doc, "model", None):
        parts.append(f"model {doc.model}")

    if getattr(doc, "fuel", None):
        parts.append(f"fuel {doc.fuel}")

    if getattr(doc, "year", None):
        parts.append(f"year {doc.year}")

    if getattr(doc, "price", None):
        parts.append(f"price {doc.price}")

    if getattr(doc, "mileage", None):
        parts.append(f"mileage {doc.mileage}")

    if getattr(doc, "paint_condition", None):
        parts.append(f"paint {doc.paint_condition}")

    return " ".join(parts).strip()


# =====================================================
# INDEX DOCUMENT CHUNKS
# =====================================================

VECTOR_SIZE = 768

def index_document_chunks(
    limit: int = 2000,
    force_rebuild: bool = False
) -> int:
    """
    Index pipeline v2 (production):
    NormalizedDocument -> DocumentChunk -> single embedding per chunk -> Qdrant payload

    Payload MUST contain fields used by filters:
    brand, model, price, mileage, year, fuel, source, source_url
    """

    Base.metadata.create_all(bind=engine)
    session = SessionLocal()

    try:
        store = QdrantStore()

        if force_rebuild:
            try:
                print("[INDEX] force rebuild: clearing collection")
                store.client.delete_collection("auto_search_chunks")
            except Exception:
                pass

            store.create_collection(VECTOR_SIZE)

        store.create_collection(VECTOR_SIZE)

        chunks = (
            session.query(DocumentChunk, NormalizedDocument)
            .join(NormalizedDocument, DocumentChunk.normalized_id == NormalizedDocument.id)
            .order_by(DocumentChunk.id.desc())
            .limit(limit)
            .all()
        )

        if not chunks:
            print("[INDEX][WARN] no document chunks found")
            return 0

        points: List[PointStruct] = []
        now = datetime.now(tz=timezone.utc)

        skipped_empty_text = 0
        skipped_missing_url = 0

        for ch, doc in chunks:
            chunk_text = _clean_text(ch.chunk_text or "")[:800]
            if not chunk_text:
                skipped_empty_text += 1
                continue

            source_url = getattr(doc, "source_url", None)
            title_text = _clean_text(getattr(doc, "title", "") or "")

            if not source_url or not title_text:
                skipped_missing_url += 1
                continue

            brand = _norm_str(getattr(doc, "brand", None))
            model = _norm_str(getattr(doc, "model", None))

            fuel = _norm_str(getattr(doc, "fuel", None))
            if fuel not in ALLOWED_FUELS:
                fuel = None

            price = _norm_int(getattr(doc, "price", None))

            if price is not None and price <= 0:
                price = None

            mileage = _norm_int(getattr(doc, "mileage", None))
            year = _norm_int(getattr(doc, "year", None))

            if brand is None:
                brand = None

            structured_text = build_structured_text(doc)

            embedding_text = f"""
title {title_text}
brand {brand or ''}
model {model or ''}
structured {structured_text or ''}
content {chunk_text}
""".strip()

            embedding = deterministic_embedding(embedding_text)

            if not embedding or len(embedding) != VECTOR_SIZE:
                continue

            created_at_ts = getattr(doc, "created_at_ts", None)
            if created_at_ts is None:
                created_at_ts = int(now.timestamp())

            created_at = getattr(doc, "created_at", None)
            if not created_at:
                created_at = now.isoformat()

            currency = getattr(doc, "currency", None)
            if not currency:
                currency = "RUB"

            payload = {
                "source": getattr(doc, "source", None),
                "source_url": source_url,
                "title": getattr(doc, "title", None),

                "brand": brand,
                "model": model,
                "brand_model": f"{brand or ''} {model or ''}".strip(),
                "price": price,
                "currency": currency,
                "mileage": mileage,
                "year": year,
                "fuel": fuel,
                "region": _norm_str(getattr(doc, "region", None)),
                "paint_condition": getattr(doc, "paint_condition", None),

                "sale_intent": getattr(doc, "sale_intent", None),
                "quality_score": getattr(doc, "quality_score", None),
                "doc_quality": (
                    1 if price else 0
                ) + (
                    1 if mileage else 0
                ) + (
                    1 if year else 0
                ),

                "doc_id": getattr(doc, "id", None),
                "normalized_id": getattr(doc, "id", None),
                "chunk_id": getattr(ch, "id", None),
                "chunk_index": getattr(ch, "chunk_index", None),
                "content": chunk_text,
                "title_text": title_text,

                "created_at": created_at if isinstance(created_at, str) else now.isoformat(),
                "created_at_ts": _norm_int(created_at_ts) or int(now.timestamp()),
                "created_at_source": "normalized",
            }

            point_hash = hashlib.sha1(f"{ch.id}".encode()).hexdigest()
            point_id = int(point_hash[:16], 16)

            points.append(
                PointStruct(
                    id=point_id,
                    vector=embedding,
                    payload=payload,
                )
            )

        if not points:
            print("[INDEX][WARN] no valid points generated from chunks")
            return 0

        store.upsert(points)
        print(f"[INDEX] indexed chunks: {len(points)}")

        print(
            f"[INDEX][DEBUG] skipped_empty_text={skipped_empty_text} "
            f"skipped_missing_url={skipped_missing_url}",
            flush=True,
        )

        return len(points)

    finally:
        try:
            session.close()
        except Exception:
            pass


# =====================================================
# RUN INDEX
# =====================================================

def run_index(limit: int = 2000, force_rebuild: bool = False):
    print("[INDEX] run_index -> index_document_chunks", flush=True)
    return index_document_chunks(
        limit=limit,
        force_rebuild=force_rebuild
    )