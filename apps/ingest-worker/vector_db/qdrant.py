import os
from datetime import datetime, timezone
from typing import List, Optional
from uuid import uuid4

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
        env_host = os.getenv("QDRANT_HOST")
        env_port = os.getenv("QDRANT_PORT")

        if env_host:
            host = env_host

        if env_port:
            try:
                port = int(env_port)
            except Exception:
                pass

        # 🔒 ВАЖНО: внутри docker-сети нельзя localhost
        if host in ("localhost", "127.0.0.1"):
            host = "qdrant"

        kwargs = {"host": host, "port": port}

        try:
            self.client = QdrantClient(**kwargs, check_compatibility=False)
        except TypeError:
            self.client = QdrantClient(**kwargs)

        print(f"[QDRANT] client init host={host} port={port} collection={COLLECTION_NAME}")

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

        return response.points

    # =====================================================
    # BUILD POINT
    # =====================================================

    def build_point(self, document, chunk_text: str, vector):

        payload = {
            "source": document.source,
            "source_url": document.source_url,
            "title": document.title,
            "content": chunk_text,
        }

        # 🎯 Добавлено: doc_id
        payload["doc_id"] = document.id

        if hasattr(document, "brand"):
            payload["brand"] = getattr(document, "brand", None)

        if hasattr(document, "model"):
            payload["model"] = getattr(document, "model", None)

        if hasattr(document, "price"):
            payload["price"] = getattr(document, "price", None)

        if hasattr(document, "mileage"):
            payload["mileage"] = getattr(document, "mileage", None)

        if hasattr(document, "year"):
            payload["year"] = getattr(document, "year", None)

        if hasattr(document, "fuel"):
            payload["fuel"] = getattr(document, "fuel", None)

        if hasattr(document, "region"):
            payload["region"] = getattr(document, "region", None)

        if hasattr(document, "sale_intent"):
            payload["sale_intent"] = getattr(document, "sale_intent", 1)

        if hasattr(document, "created_at"):
            payload["created_at"] = getattr(document, "created_at", None)

        if hasattr(document, "created_at_ts"):
            payload["created_at_ts"] = getattr(document, "created_at_ts", None)

        # =====================================================
        # 🔥 PAYLOAD NORMALIZATION (PRODUCTION STANDARD)
        # =====================================================

        # --- NORMALIZE BRAND ---
        brand = payload.get("brand")
        if isinstance(brand, str):
            payload["brand"] = brand.lower().strip()

        # --- NORMALIZE MODEL ---
        model = payload.get("model")
        if isinstance(model, str):
            payload["model"] = model.lower().strip()

        # --- NORMALIZE FUEL ---
        fuel = payload.get("fuel")
        if isinstance(fuel, str):
            fuel = fuel.lower().strip()
            allowed = {"petrol", "diesel", "hybrid", "electric"}
            if fuel in allowed:
                payload["fuel"] = fuel
            else:
                payload["fuel"] = None

        # --- SAFE NUMERIC PARSING ---
        for field in ["price", "mileage", "year"]:

            value = payload.get(field)

            if value is None:
                payload[field] = None
                continue

            try:
                if isinstance(value, str):
                    value = value.replace(" ", "")
                payload[field] = int(value)
            except Exception:
                payload[field] = None

        # --- YEAR SANITY CHECK ---
        year = payload.get("year")

        if isinstance(year, int):

            if year < 1985:
                payload["year"] = None

            if year > datetime.now().year + 1:
                payload["year"] = None

        # --- PRICE SANITY CHECK ---
        price = payload.get("price")

        if isinstance(price, int):

            if price < 10000:
                payload["price"] = None

            if price > 200000000:
                payload["price"] = None

        if os.getenv("DEBUG_QDRANT_PAYLOAD") == "1":
            print("[QDRANT][PAYLOAD]", payload)

        return PointStruct(
            id=str(uuid4()),
            vector=vector,
            payload=payload,
        )