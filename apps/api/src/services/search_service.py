from typing import List, Dict, Any
from sentence_transformers import SentenceTransformer

from integrations.vector_db.qdrant import QdrantStore
from services.query_parser import StructuredQuery


# =========================
# EMBEDDING MODEL
# =========================

_model = None


def get_model():
    global _model
    if _model is None:
        print("[API][EMBED] loading model")
        _model = SentenceTransformer("intfloat/multilingual-e5-base")
    return _model


def embed_query(text: str):
    model = get_model()
    return model.encode(text).tolist()


# =========================
# SEARCH SERVICE
# =========================

class SearchService:
    def __init__(self):
        self.store = QdrantStore()
        self.collection_name = "auto_search_chunks"

    # =====================================================
    # PURE SEMANTIC SEARCH
    # =====================================================

    def search(
        self,
        structured: StructuredQuery,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:

        query_text = structured.raw_query or ""
        if not query_text.strip():
            print("[SEARCH] empty query")
            return []

        # EMBED QUERY
        query_vector = embed_query(query_text)

        print(f"[SEARCH][DEBUG] vector_length={len(query_vector)}")
        print(f"[SEARCH][DEBUG] collection={self.collection_name}")
        print(f"[SEARCH][DEBUG] filter=None")

        try:
            hits = self.store.search(
                vector=query_vector,
                limit=limit,
                query_filter=None,  # 🔥 ФИЛЬТР ВРЕМЕННО ОТКЛЮЧЕН
            )
        except Exception as e:
            print(f"[SEARCH][ERROR] qdrant search failed: {e}")
            return []

        print(f"[SEARCH][DEBUG] points_found={len(hits)}")

        results: List[Dict[str, Any]] = []

        for hit in hits:
            payload = hit.payload or {}

            results.append(
                {
                    "source": payload.get("source"),
                    "source_url": payload.get("source_url"),
                    "title": payload.get("title"),
                    "content": payload.get("content"),
                    "score": float(hit.score or 0.0),
                }
            )

        return results
