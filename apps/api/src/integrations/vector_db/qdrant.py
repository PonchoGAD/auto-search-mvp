from datetime import datetime, timezone
from typing import List, Optional

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

        if COLLECTION_NAME not in collections:
            self.client.create_collection(
                collection_name=COLLECTION_NAME,
                vectors_config=VectorParams(
                    size=vector_size,
                    distance=Distance.COSINE,
                ),
                hnsw_config={
                    "m": 32,
                    "ef_construct": 256
                }
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

    def _normalize_payload_schema(self, payload: dict) -> dict:
        """
        Enforce payload schema for search filters.
        Ensures fields always exist and are normalized.
        """

        # -------------------------
        # brand
        # -------------------------
        brand = payload.get("brand")
        if isinstance(brand, str):
            payload["brand"] = brand.lower().strip()
        else:
            payload["brand"] = None

        # -------------------------
        # model
        # -------------------------
        model = payload.get("model")
        if isinstance(model, str):
            payload["model"] = model.lower().strip()
        else:
            payload["model"] = None

        # -------------------------
        # fuel
        # -------------------------
        fuel = payload.get("fuel")
        if isinstance(fuel, str):
            payload["fuel"] = fuel.lower().strip()
        else:
            payload["fuel"] = None

        # -------------------------
        # price
        # -------------------------
        price = payload.get("price")

        try:
            if isinstance(price, str):
                price = int(price.replace(" ", ""))
            elif isinstance(price, float):
                price = int(price)

            payload["price"] = price
        except Exception:
            payload["price"] = None

        # -------------------------
        # mileage
        # -------------------------
        mileage = payload.get("mileage")

        try:
            if isinstance(mileage, str):
                mileage = int(mileage.replace(" ", ""))
            elif isinstance(mileage, float):
                mileage = int(mileage)

            payload["mileage"] = mileage
        except Exception:
            payload["mileage"] = None

        # -------------------------
        # year
        # -------------------------
        year = payload.get("year")

        try:
            payload["year"] = int(year)
        except Exception:
            payload["year"] = None

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

            # 🔒 SCHEMA ENFORCEMENT
            payload = self._normalize_payload_schema(payload)

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

        if normalized_points:
            sample = normalized_points[0].payload
            print(f"[QDRANT] sample payload schema: {sample}")

        print(f"[QDRANT] upserted points: {len(normalized_points)}")

    # =====================================================
    # SEARCH
    # =====================================================

    def search(
        self,
        vector: List[float],
        limit: int = 20,
        query_filter: dict | None = None,
    ):
        if query_filter:
            response = self.client.query_points(
                collection_name=COLLECTION_NAME,
                query=vector,
                limit=limit,
                query_filter=query_filter,
            )
        else:
            response = self.client.query_points(
                collection_name=COLLECTION_NAME,
                query=vector,
                limit=limit,
            )

        # 🔒 READ-SIDE NORMALIZATION (safety)

        for p in response.points:

            payload = p.payload or {}

            if "brand" in payload and isinstance(payload["brand"], str):
                payload["brand"] = payload["brand"].lower()

            if "model" in payload and isinstance(payload["model"], str):
                payload["model"] = payload["model"].lower()

            if "fuel" in payload and isinstance(payload["fuel"], str):
                payload["fuel"] = payload["fuel"].lower()

        return response.points


# =====================================================
# ✅ SINGLETON CLIENT (для metrics и прямого доступа)
# =====================================================

import os

QDRANT_HOST = os.getenv("QDRANT_HOST", "qdrant")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))

qdrant_client = QdrantClient(
    host=QDRANT_HOST,
    port=QDRANT_PORT,
)