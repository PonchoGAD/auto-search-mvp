import os
import hashlib
from typing import List
from datetime import datetime, timezone

from qdrant_client.models import PointStruct

from db.models import RawDocument
from integrations.vector_db.qdrant import QdrantStore

# =====================================================
# EMBEDDING PROVIDER CONFIG
# =====================================================

EMBEDDING_PROVIDER = os.getenv("EMBEDDING_PROVIDER", "openai").lower()

# OpenAI
OPENAI_MODEL = os.getenv("OPENAI_EMBEDDING_MODEL", "text-embedding-3-small")
OPENAI_VECTOR_SIZE = 1536

# Local models (BGE / E5)
LOCAL_VECTOR_SIZE = 384  # bge-small / e5-small

# Deterministic (fallback / demo only)
DETERMINISTIC_VECTOR_SIZE = 32

# Cache for local model
_LOCAL_MODEL = None
_LOCAL_MODEL_NAME = None

# =====================================================
# EMBEDDING IMPLEMENTATIONS
# =====================================================

def _deterministic_fallback(text: str, size: int = DETERMINISTIC_VECTOR_SIZE) -> List[float]:
    """
    ❗ INTERNAL ONLY
    Used ONLY as last-resort fallback to keep MVP alive.
    """
    if not (text or "").strip():
        return []

    digest = hashlib.sha256(text.encode("utf-8")).digest()
    return [(digest[i % len(digest)] / 255.0) for i in range(size)]


def embed_openai(text: str) -> List[float]:
    if not text.strip():
        return []

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError("OPENAI_API_KEY is missing")

    from openai import OpenAI
    client = OpenAI(api_key=api_key)

    resp = client.embeddings.create(
        model=OPENAI_MODEL,
        input=text,
    )

    emb = resp.data[0].embedding
    if not isinstance(emb, list) or not emb:
        raise RuntimeError("OpenAI returned empty embedding")

    return emb


def embed_bge_or_e5(text: str) -> List[float]:
    if not text.strip():
        return []

    global _LOCAL_MODEL, _LOCAL_MODEL_NAME
    from sentence_transformers import SentenceTransformer

    model_name = os.getenv(
        "LOCAL_EMBEDDING_MODEL",
        "sentence-transformers/all-MiniLM-L6-v2",
    )

    if _LOCAL_MODEL is None or _LOCAL_MODEL_NAME != model_name:
        _LOCAL_MODEL = SentenceTransformer(model_name)
        _LOCAL_MODEL_NAME = model_name

    vector = _LOCAL_MODEL.encode(text, normalize_embeddings=True)
    return vector.tolist()


def embed_text(text: str) -> List[float]:
    """
    REAL embedding dispatcher.
    Used by BOTH index and search.
    """
    if not text.strip():
        return []

    try:
        if EMBEDDING_PROVIDER == "openai":
            return embed_openai(text)

        if EMBEDDING_PROVIDER in ("bge", "e5", "local"):
            return embed_bge_or_e5(text)

        if EMBEDDING_PROVIDER in ("deterministic", "mock", "demo"):
            return _deterministic_fallback(text)

        raise ValueError(f"Unknown EMBEDDING_PROVIDER={EMBEDDING_PROVIDER}")

    except Exception as e:
        print(f"[EMBED][WARN] provider={EMBEDDING_PROVIDER} failed → fallback: {e}")
        return _deterministic_fallback(text)


# =====================================================
# ⚠️ CRITICAL ADAPTER (DO NOT REMOVE)
# =====================================================

def deterministic_embedding(text: str) -> List[float]:
    """
    ✅ ADAPTER FOR SearchService.

    SearchService IMPORTS THIS FUNCTION.
    Therefore it MUST return the SAME embedding
    that was used during indexing.

    ❌ NOT a hash anymore
    ✅ Delegates to embed_text()
    """
    return embed_text(text)


def resolve_vector_size() -> int:
    if EMBEDDING_PROVIDER == "openai":
        return OPENAI_VECTOR_SIZE
    if EMBEDDING_PROVIDER in ("bge", "e5", "local"):
        return LOCAL_VECTOR_SIZE
    return DETERMINISTIC_VECTOR_SIZE


# =====================================================
# INDEX RAW DOCUMENTS
# =====================================================

def index_raw_documents(raw_docs: List[RawDocument]) -> int:
    if not raw_docs:
        print("[INDEX][WARN] no raw documents to index")
        return 0

    vector_size = resolve_vector_size()
    store = QdrantStore()
    store.create_collection(vector_size)

    points: List[PointStruct] = []
    now = datetime.now(tz=timezone.utc)

    for doc in raw_docs:
        text = ((doc.title or "") + "\n" + (doc.content or "")).strip()
        if not text:
            continue

        vector = embed_text(text)
        if not vector:
            continue

        if len(vector) != vector_size:
            print(
                f"[INDEX][WARN] vector size mismatch doc={doc.id} "
                f"len={len(vector)} expected={vector_size}"
            )
            continue

        fetched_at = doc.fetched_at or now
        if fetched_at.tzinfo is None:
            fetched_at = fetched_at.replace(tzinfo=timezone.utc)

        payload = {
            "source": doc.source,
            "url": doc.source_url,
            "brand": None,
            "model": None,
            "price": None,
            "mileage": None,
            "fuel": None,
            "region": None,
            "paint_condition": None,
            "created_at": fetched_at.isoformat(),
            "created_at_ts": int(fetched_at.timestamp()),
            "created_at_source": "fetched",
        }

        points.append(
            PointStruct(
                id=doc.id,
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
# LEGACY
# =====================================================

def run_index(limit: int = 500):
    print("[INDEX][WARN] run_index() is legacy, prefer index_raw_documents()")
    return 0
