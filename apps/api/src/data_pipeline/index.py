import hashlib
from typing import List
from datetime import datetime, timezone

from qdrant_client.models import PointStruct

from db.models import RawDocument
from integrations.vector_db.qdrant import QdrantStore


# =====================================================
# CONFIG
# =====================================================

VECTOR_SIZE = 32  # MOCK embedding size (MVP, deterministic)


# =====================================================
# EMBEDDING
# =====================================================

def deterministic_embedding(text: str, size: int = VECTOR_SIZE) -> List[float]:
    """
    Детерминированный MOCK embedding.
    Один и тот же текст -> всегда один и тот же вектор.

    MVP:
    - без ML
    - быстро
    - стабильно для демо
    """
    digest = hashlib.sha256(text.encode("utf-8")).digest()

    vector: List[float] = []
    for i in range(size):
        value = digest[i % len(digest)]
        vector.append(value / 255.0)

    return vector


# =====================================================
# INDEX RAW DOCUMENTS (MAIN ENTRY)
# =====================================================

def index_raw_documents(raw_docs: List[RawDocument]) -> int:
    """
    Индексирует RawDocument напрямую в Qdrant.

    ГАРАНТИИ:
    - payload полностью совместим с SearchService
    - created_at / created_at_ts / created_at_source ВСЕГДА есть
    - безопасно для demo / prod
    """

    if not raw_docs:
        print("[INDEX][WARN] no raw documents to index")
        return 0

    store = QdrantStore()
    store.create_collection(VECTOR_SIZE)

    points: List[PointStruct] = []
    now = datetime.now(tz=timezone.utc)

    for doc in raw_docs:
        text = (doc.title or "") + "\n" + (doc.content or "")
        text = text.strip()

        if not text:
            continue

        vector = deterministic_embedding(text)

        created_at = doc.created_at or now
        if created_at.tzinfo is None:
            created_at = created_at.replace(tzinfo=timezone.utc)

        payload = {
            # 🔑 REQUIRED BY SEARCH
            "source": doc.source,
            "url": doc.source_url,

            # OPTIONAL STRUCTURE (появится позже при normalize)
            "brand": None,
            "model": None,
            "price": None,
            "mileage": None,
            "fuel": None,
            "region": None,

            # 🔑 RECENCY (HARDENED)
            "created_at": created_at.isoformat(),
            "created_at_ts": int(created_at.timestamp()),
            "created_at_source": doc.created_at_source or "ingested",
        }

        points.append(
            PointStruct(
                id=f"raw_{doc.id}",  # ⛑ уникально и стабильно
                vector=vector,
                payload=payload,
            )
        )

    if not points:
        print("[INDEX][WARN] no valid points generated")
        return 0

    store.upsert(points)

    print(f"[INDEX] indexed raw documents: {len(points)}")
    return len(points)


# =====================================================
# LEGACY / FALLBACK (НЕ ЛОМАЕМ)
# =====================================================

def run_index(limit: int = 500):
    """
    Legacy indexer.
    Оставлен для совместимости / ручного использования.
    """
    print("[INDEX][WARN] run_index() is legacy, prefer index_raw_documents()")
    return 0
