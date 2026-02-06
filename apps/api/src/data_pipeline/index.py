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

    ПРАВИЛА (MVP):
    - используем ТОЛЬКО реальные поля RawDocument
    - fetched_at = базовая временная метка
    - никаких created_at / created_at_ts в модели
    """

    if not raw_docs:
        print("[INDEX][WARN] no raw documents to index")
        return 0

    store = QdrantStore()
    store.create_collection(VECTOR_SIZE)

    points: List[PointStruct] = []
    now = datetime.now(tz=timezone.utc)

    for doc in raw_docs:
        # --- текст ---
        text = ((doc.title or "") + "\n" + (doc.content or "")).strip()
        if not text:
            continue

        vector = deterministic_embedding(text)

        # --- время ---
        fetched_at = doc.fetched_at or now
        if fetched_at.tzinfo is None:
            fetched_at = fetched_at.replace(tzinfo=timezone.utc)

        # --- payload (строго совместим с search) ---
        payload = {
            "source": doc.source,
            "url": doc.source_url,

            # optional (появятся позже при normalize)
            "brand": None,
            "model": None,
            "price": None,
            "mileage": None,
            "fuel": None,
            "region": None,

            # recency (ЕДИНСТВЕННЫЙ источник времени)
            "created_at": fetched_at.isoformat(),
            "created_at_ts": int(fetched_at.timestamp()),
            "created_at_source": "fetched",
        }

        points.append(
            PointStruct(
                id=f"raw_{doc.id}",
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
# LEGACY / FALLBACK
# =====================================================

def run_index(limit: int = 500):
    """
    Legacy indexer.
    Оставлен для совместимости / ручного использования.
    """
    print("[INDEX][WARN] run_index() is legacy, prefer index_raw_documents()")
    return 0
