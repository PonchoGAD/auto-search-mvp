# apps/api/src/data_pipeline/index.py

import hashlib
import time
from typing import List

from qdrant_client.models import PointStruct

from db.session import SessionLocal
from db.models import DocumentChunk, NormalizedDocument
from integrations.vector_db.qdrant import QdrantStore


VECTOR_SIZE = 32  # фиксированный размер для MOCK эмбеддингов


def deterministic_embedding(text: str, size: int = VECTOR_SIZE) -> List[float]:
    """
    Детерминированный MOCK embedding.
    Один и тот же текст -> всегда один и тот же вектор.
    """
    digest = hashlib.sha256(text.encode("utf-8")).digest()

    vector = []
    for i in range(size):
        value = digest[i % len(digest)]
        vector.append(value / 255.0)

    return vector


def run_index(limit: int = 500):
    session = SessionLocal()

    chunks = (
        session.query(DocumentChunk)
        .order_by(DocumentChunk.id.desc())
        .limit(limit)
        .all()
    )

    if not chunks:
        print("[INDEX][WARN] no document chunks found")
        session.close()
        return

    store = QdrantStore()
    store.create_collection(VECTOR_SIZE)

    points: List[PointStruct] = []

    for chunk in chunks:
        doc = (
            session.query(NormalizedDocument)
            .filter_by(id=chunk.normalized_id)
            .first()
        )

        if not doc:
            continue

        vector = deterministic_embedding(chunk.chunk_text)

        payload = {
            "source": doc.source,
            "url": doc.source_url,
            "brand": doc.brand,
            "model": doc.model,
            "price": doc.price,
            "mileage": doc.mileage,
            "fuel": doc.fuel,
            "city": doc.city,
            "region": doc.region,
            "chunk_text": chunk.chunk_text,
            "timestamp": int(time.time()),
        }

        points.append(
            PointStruct(
                id=chunk.id,
                vector=vector,
                payload=payload,
            )
        )

    store.upsert(points)

    session.close()
    print(f"[INDEX] indexed: {len(points)}")
