from datetime import datetime, timezone
from typing import List

from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct


COLLECTION_NAME = "auto_search_chunks"


class QdrantStore:
    """
    Qdrant vector storage.

    ГАРАНТИИ:
    - created_at ВСЕГДА присутствует в payload
    - created_at_ts (unix) ВСЕГДА присутствует
    - created_at_source фиксируется
    - recency не ломается из-за ingest / источников
    """

    def __init__(self, host: str = "qdrant", port: int = 6333):
        self.client = QdrantClient(
            host=host,
            port=port,
            check_compatibility=False,  # важно для версии сервера 1.9.x
        )

    # =====================================================
    # COLLECTION
    # =====================================================

    def create_collection(self, vector_size: int):
        collections = [
            c.name for c in self.client.get_collections().collections
        ]

        if COLLECTION_NAME in collections:
            return

        self.client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(
                size=vector_size,
                distance=Distance.COSINE,
            ),
        )

        print(f"[QDRANT] collection created: {COLLECTION_NAME}")

    # =====================================================
    # PAYLOAD NORMALIZATION (RECENCY HARDENING)
    # =====================================================

    def _normalize_created_at(self, payload: dict) -> dict:
        """
        Гарантирует наличие:
        - created_at (ISO str)
        - created_at_ts (int)
        - created_at_source (source | ingested)
        """

        raw = payload.get("created_at")

        # 1️⃣ datetime из payload
        if isinstance(raw, datetime):
            dt = raw
            if not dt.tzinfo:
                dt = dt.replace(tzinfo=timezone.utc)

            payload["created_at"] = dt.isoformat()
            payload["created_at_ts"] = int(dt.timestamp())
            payload.setdefault("created_at_source", "source")
            return payload

        # 2️⃣ ISO string из payload
        if isinstance(raw, str):
            try:
                dt = datetime.fromisoformat(raw)
                if not dt.tzinfo:
                    dt = dt.replace(tzinfo=timezone.utc)

                payload["created_at"] = dt.isoformat()
                payload["created_at_ts"] = int(dt.timestamp())
                payload.setdefault("created_at_source", "source")
                return payload
            except Exception:
                pass

        # 3️⃣ Fallback — ingest time
        now = datetime.now(tz=timezone.utc)
        payload["created_at"] = now.isoformat()
        payload["created_at_ts"] = int(now.timestamp())
        payload["created_at_source"] = "ingested"

        return payload

    # =====================================================
    # UPSERT
    # =====================================================

    def upsert(self, points: List[PointStruct]):
        if not points:
            print("[QDRANT] no points to upsert")
            return

        normalized_points: List[PointStruct] = []

        for p in points:
            payload = p.payload or {}

            # 🔑 RECENCY HARDENING
            payload = self._normalize_created_at(payload)

            normalized_points.append(
                PointStruct(
                    id=p.id,
                    vector=p.vector,
                    payload=payload,
                )
            )

        self.client.upsert(
            collection_name=COLLECTION_NAME,
            points=normalized_points,
        )

        print(f"[QDRANT] upserted points: {len(normalized_points)}")

    # =====================================================
    # SEARCH
    # =====================================================

    def search(self, vector: List[float], limit: int = 20):
        """
        Поиск по векторам.
        Recency применяется на уровне ranking (search_service),
        payload полностью готов.
        """
        return self.client.search(
            collection_name=COLLECTION_NAME,
            query_vector=vector,
            limit=limit,
        )
