#  apps\api\src\data_pipeline\index.py

import os
import hashlib
import re
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

ALLOWED_FUELS = {"petrol", "diesel", "hybrid", "electric"}

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
# STRUCTURED TEXT BUILDER (MULTI VECTOR)
# =====================================================

def build_structured_text(doc: NormalizedDocument) -> str:
    """
    Builds structured text representation for embedding.
    Improves recall for structured queries.
    """

    parts = []

    if getattr(doc, "brand", None):
        parts.append(str(doc.brand))

    if getattr(doc, "model", None):
        parts.append(str(doc.model))

    if getattr(doc, "fuel", None):
        parts.append(str(doc.fuel))

    if getattr(doc, "year", None):
        parts.append(str(doc.year))

    if getattr(doc, "price", None):
        parts.append(f"price {doc.price}")

    if getattr(doc, "mileage", None):
        parts.append(f"mileage {doc.mileage}")

    return " ".join(parts).strip()


# =====================================================
# INDEX DOCUMENT CHUNKS
# =====================================================

VECTOR_SIZE = 768

def index_document_chunks(limit: int = 2000) -> int:
    """
    Index pipeline v2 (production):
    NormalizedDocument -> DocumentChunk -> embedding(chunk_text) -> Qdrant payload

    Payload MUST contain fields used by filters:
    brand, model, price, mileage, year, fuel, source, source_url
    """

    Base.metadata.create_all(bind=engine)
    session = SessionLocal()

    try:
        store = QdrantStore()
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

        for ch, doc in chunks:
            chunk_text = (ch.chunk_text or "").strip()
            if not chunk_text:
                continue

            # =====================================================
            # MULTI VECTOR EMBEDDINGS
            # =====================================================

            title_text = (getattr(doc, "title", "") or "").strip()
            structured_text = build_structured_text(doc)

            vectors = []

            if title_text:
                vectors.append(("title", deterministic_embedding(title_text)))

            vectors.append(("content", deterministic_embedding(chunk_text)))

            if structured_text:
                vectors.append(("structured", deterministic_embedding(structured_text)))

            brand = _norm_str(getattr(doc, "brand", None))
            model = _norm_str(getattr(doc, "model", None))

            fuel = _norm_str(getattr(doc, "fuel", None))
            if fuel not in ALLOWED_FUELS:
                fuel = None

            price = _norm_int(getattr(doc, "price", None))
            mileage = _norm_int(getattr(doc, "mileage", None))
            year = _norm_int(getattr(doc, "year", None))

            if brand is None and price is None and mileage is None and year is None:
                continue

            created_at_ts = getattr(doc, "created_at_ts", None)
            if created_at_ts is None:
                created_at_ts = int(now.timestamp())

            created_at = getattr(doc, "created_at", None)
            if not created_at:
                created_at = now.isoformat()

            payload = {
                "source": getattr(doc, "source", None),
                "source_url": getattr(doc, "source_url", None),
                "title": getattr(doc, "title", None),

                "brand": brand,
                "model": model,
                "price": price,
                "currency": getattr(doc, "currency", "RUB"),
                "mileage": mileage,
                "year": year,
                "fuel": fuel,
                "region": getattr(doc, "region", None),
                "paint_condition": getattr(doc, "paint_condition", None),

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

            for vec_type, vec in vectors:

                if not vec or len(vec) != VECTOR_SIZE:
                    continue

                points.append(
                    PointStruct(
                        id=f"{ch.id}_{vec_type}",
                        vector=vec,
                        payload={
                            **payload,
                            "vector_type": vec_type
                        },
                    )
                )

        if not points:
            print("[INDEX][WARN] no valid points generated from chunks")
            return 0

        store.upsert(points)
        print(f"[INDEX] indexed chunks: {len(points)}")
        return len(points)

    finally:
        try:
            session.close()
        except Exception:
            pass


# =====================================================
# RUN INDEX
# =====================================================

def run_index(limit: int = 2000):
    print("[INDEX] run_index -> index_document_chunks", flush=True)
    return index_document_chunks(limit=limit)