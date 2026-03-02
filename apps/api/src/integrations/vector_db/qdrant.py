from datetime import datetime, timezone
from typing import List
from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct, Filter
from core.settings import settings
from urllib.parse import urlparse
import os


class QdrantStore:
    """
    Qdrant vector storage.

    ГАРАНТИИ:
    - created_at ВСЕГДА присутствует в payload
    - created_at_ts (unix) ВСЕГДА присутствует
    - created_at_source фиксируется
    - recency не ломается из-за ingest / источников
    """

    def __init__(self):
        # settings.qdrant_url = "http://qdrant:6333"
        u = settings.qdrant_url
        parsed = urlparse(u)

        host = parsed.hostname or os.getenv("QDRANT_HOST", "qdrant")
        port = parsed.port or int(os.getenv("QDRANT_PORT", "6333"))

        self.collection = (
            settings.qdrant_collection
            or os.getenv("QDRANT_COLLECTION", "auto_search_chunks")
        )

        try:
            self.client = QdrantClient(
                host=host,
                port=port,
                check_compatibility=False,
            )
        except TypeError:
            self.client = QdrantClient(
                host=host,
                port=port,
            )

        print(
            f"[QDRANT] api client host={host} port={port} collection={self.collection}",
            flush=True,
        )

    # =====================================================
    # COLLECTION
    # =====================================================

    def create_collection(self, vector_size: int):
        collections = [c.name for c in self.client.get_collections().collections]

        if self.collection not in collections:
            self.client.create_collection(
                collection_name=self.collection,
                vectors_config=VectorParams(
                    size=vector_size,
                    distance=Distance.COSINE,
                ),
            )
            print(
                f"[QDRANT] collection created: {self.collection}",
                flush=True,
            )

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
            return

        normalized_points: List[PointStruct] = []

        for p in points:
            payload = p.payload or {}

            # HARD NORMALIZATION
            if payload.get("brand"):
                payload["brand"] = str(payload["brand"]).strip().lower()

            if payload.get("fuel"):
                payload["fuel"] = str(payload["fuel"]).strip().lower()

            if payload.get("city"):
                payload["city"] = str(payload["city"]).strip().lower()

            if payload.get("region"):
                payload["region"] = str(payload["region"]).strip().lower()

            payload = self._normalize_created_at(payload)

            normalized_points.append(
                PointStruct(
                    id=p.id,
                    vector=p.vector,
                    payload=payload,
                )
            )

        self.client.upsert(
            collection_name=self.collection,
            points=normalized_points,
        )

        print(
            f"[QDRANT] upserted points: {len(normalized_points)}",
            flush=True,
        )

    # =====================================================
    # SEARCH
    # =====================================================

    def search(
        self,
        vector: List[float],
        limit: int = 20,
        query_filter: Filter | None = None,
    ):
        return self.client.search(
            collection_name=self.collection,
            query_vector=vector,
            limit=limit,
            query_filter=query_filter,
            with_payload=True,
        )