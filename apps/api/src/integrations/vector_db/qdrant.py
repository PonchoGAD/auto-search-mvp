from datetime import datetime, timezone
from typing import List, Optional, Any

from qdrant_client import QdrantClient
from qdrant_client.models import (
    VectorParams,
    Distance,
    PointStruct,
    PayloadSchemaType,
)
import os


COLLECTION_NAME = "auto_search_chunks"
ALLOWED_FUELS = {"petrol", "diesel", "hybrid", "electric", "gas", "gas_petrol"}
MAX_MILEAGE = 500000


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

    def _safe_create_payload_index(self, field_name: str, field_schema: "PayloadSchemaType"):
        try:
            self.client.create_payload_index(
                collection_name=COLLECTION_NAME,
                field_name=field_name,
                field_schema=field_schema,
            )
            print(f"[QDRANT] payload index ensured: {field_name}")
        except Exception as e:
            print(f"[QDRANT][WARN] payload index ensure failed for {field_name}: {e}")

    def _ensure_payload_indexes(self):
        self._safe_create_payload_index("brand", PayloadSchemaType.KEYWORD)
        self._safe_create_payload_index("model", PayloadSchemaType.KEYWORD)
        self._safe_create_payload_index("fuel", PayloadSchemaType.KEYWORD)
        self._safe_create_payload_index("year", PayloadSchemaType.INTEGER)
        self._safe_create_payload_index("price", PayloadSchemaType.INTEGER)
        self._safe_create_payload_index("mileage", PayloadSchemaType.INTEGER)
        self._safe_create_payload_index("created_at_ts", PayloadSchemaType.INTEGER)
        self._safe_create_payload_index("normalized_id", PayloadSchemaType.INTEGER)
        self._safe_create_payload_index("source_url", PayloadSchemaType.KEYWORD)

    def _normalize_int_or_none(self, value: Any) -> Optional[int]:
        try:
            if value is None:
                return None
            if isinstance(value, bool):
                return None
            if isinstance(value, str):
                value = value.strip()
                if not value:
                    return None
            return int(value)
        except Exception:
            return None

    def _normalize_str_or_none(self, value: Any) -> Optional[str]:
        if not isinstance(value, str):
            return None
        value = value.strip()
        return value or None

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

        self._ensure_payload_indexes()

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

        current_year = datetime.now(tz=timezone.utc).year

        # -------------------------
        # brand
        # -------------------------
        brand = payload.get("brand")
        if isinstance(brand, str):
            brand = brand.lower().strip()
            payload["brand"] = brand if brand else None
        else:
            payload["brand"] = None

        # -------------------------
        # model
        # -------------------------
        model = payload.get("model")
        if isinstance(model, str):
            model = model.lower().strip()
            payload["model"] = model if model else None
        else:
            payload["model"] = None

        # -------------------------
        # fuel
        # -------------------------
        fuel = payload.get("fuel")
        if isinstance(fuel, str):
            fuel = fuel.lower().strip()
            payload["fuel"] = fuel if fuel in ALLOWED_FUELS else None
        else:
            payload["fuel"] = None

        # -------------------------
        # price
        # -------------------------
        price = payload.get("price")

        try:
            if isinstance(price, str):
                price = price.replace(" ", "").replace("\u00A0", "").replace("\xa0", "")
                price = int(price)
            elif isinstance(price, float):
                price = int(price)
            elif isinstance(price, int):
                price = price
            else:
                raise ValueError("invalid price type")

            if price <= 0 or price < 10000 or price > 200000000:
                payload["price"] = None
            else:
                payload["price"] = price
        except Exception:
            payload["price"] = None

        # -------------------------
        # mileage
        # -------------------------
        mileage = payload.get("mileage")

        try:
            if isinstance(mileage, str):
                mileage = mileage.replace(" ", "").replace("\u00A0", "").replace("\xa0", "")
                mileage = int(mileage)
            elif isinstance(mileage, float):
                mileage = int(mileage)
            elif isinstance(mileage, int):
                mileage = mileage
            else:
                raise ValueError("invalid mileage type")

            if mileage < 0 or mileage > MAX_MILEAGE:
                payload["mileage"] = None
            else:
                payload["mileage"] = mileage
        except Exception:
            payload["mileage"] = None

        # -------------------------
        # year
        # -------------------------
        year = payload.get("year")

        try:
            year = int(year)
            if year < 1985 or year > current_year + 1:
                payload["year"] = None
            else:
                payload["year"] = year
        except Exception:
            payload["year"] = None

        # -------------------------
        # ids
        # -------------------------
        payload["normalized_id"] = self._normalize_int_or_none(payload.get("normalized_id"))
        payload["chunk_id"] = self._normalize_int_or_none(payload.get("chunk_id"))
        payload["doc_id"] = self._normalize_int_or_none(payload.get("doc_id"))

        # -------------------------
        # source_url
        # -------------------------
        payload["source_url"] = self._normalize_str_or_none(payload.get("source_url"))

        # -------------------------
        # brand_model
        # -------------------------
        brand_value = payload.get("brand") or ""
        model_value = payload.get("model") or ""
        brand_model = f"{brand_value} {model_value}".strip()
        payload["brand_model"] = brand_model or None

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

        BATCH_SIZE = 128
        total = len(normalized_points)

        for i in range(0, total, BATCH_SIZE):

            batch = normalized_points[i:i + BATCH_SIZE]

            self.client.upsert(
                collection_name=COLLECTION_NAME,
                points=batch,
            )

        if normalized_points:
            sample = normalized_points[0].payload
            print(f"[QDRANT] sample payload schema: {sample}")

        print(f"[QDRANT] upserted points: {total}")

    # =====================================================
    # SEARCH
    # =====================================================

    def search(
        self,
        vector: List[float],
        limit: int = 20,
        query_filter: dict | None = None,
        query_text: Optional[str] = None,
    ):
        from qdrant_client.models import SearchParams

        if query_filter:

            response = self.client.query_points(
                collection_name=COLLECTION_NAME,
                query=vector,
                limit=limit,
                query_filter=query_filter,
                search_params=SearchParams(
                    hnsw_ef=256,
                    exact=False
                )
            )
        else:

            response = self.client.query_points(
                collection_name=COLLECTION_NAME,
                query=vector,
                limit=limit,
                search_params=SearchParams(
                    hnsw_ef=256,
                    exact=False
                )
            )

        hits = response.points

        # =====================================================
        # BM25 / TEXT FALLBACK
        # =====================================================

        if not hits and query_text:

            try:
                response = self.client.query_points(
                    collection_name=COLLECTION_NAME,
                    query=query_text,
                    limit=limit
                )

                hits = response.points

            except Exception:
                pass

        # 🔒 READ-SIDE NORMALIZATION (safety)

        current_year = datetime.now(tz=timezone.utc).year

        for p in hits:

            payload = p.payload or {}

            if "brand" in payload and isinstance(payload["brand"], str):
                brand = payload["brand"].lower().strip()
                payload["brand"] = brand or None
            else:
                payload["brand"] = None

            if "model" in payload and isinstance(payload["model"], str):
                model = payload["model"].lower().strip()
                payload["model"] = model or None
            else:
                payload["model"] = None

            if "fuel" in payload and isinstance(payload["fuel"], str):
                fuel = payload["fuel"].lower().strip()
                payload["fuel"] = fuel if fuel in ALLOWED_FUELS else None
            else:
                payload["fuel"] = None

            # -------------------------
            # price
            # -------------------------
            price = payload.get("price")
            try:
                if isinstance(price, str):
                    price = int(price.replace(" ", "").replace("\u00A0", "").replace("\xa0", ""))
                elif isinstance(price, float):
                    price = int(price)
                elif not isinstance(price, int):
                    raise ValueError("invalid price type")

                if price <= 0 or price < 10000 or price > 200000000:
                    payload["price"] = None
                else:
                    payload["price"] = price
            except Exception:
                payload["price"] = None

            # -------------------------
            # mileage
            # -------------------------
            mileage = payload.get("mileage")
            try:
                if isinstance(mileage, str):
                    mileage = int(mileage.replace(" ", "").replace("\u00A0", "").replace("\xa0", ""))
                elif isinstance(mileage, float):
                    mileage = int(mileage)
                elif not isinstance(mileage, int):
                    raise ValueError("invalid mileage type")

                if mileage < 0 or mileage > MAX_MILEAGE:
                    payload["mileage"] = None
                else:
                    payload["mileage"] = mileage
            except Exception:
                payload["mileage"] = None

            # -------------------------
            # year
            # -------------------------
            year = payload.get("year")
            try:
                year = int(year)
                if year < 1985 or year > current_year + 1:
                    payload["year"] = None
                else:
                    payload["year"] = year
            except Exception:
                payload["year"] = None

            # -------------------------
            # ids
            # -------------------------
            payload["normalized_id"] = self._normalize_int_or_none(payload.get("normalized_id"))
            payload["chunk_id"] = self._normalize_int_or_none(payload.get("chunk_id"))
            payload["doc_id"] = self._normalize_int_or_none(payload.get("doc_id"))

            # -------------------------
            # source_url
            # -------------------------
            payload["source_url"] = self._normalize_str_or_none(payload.get("source_url"))

            # -------------------------
            # brand_model
            # -------------------------
            brand_value = payload.get("brand") or ""
            model_value = payload.get("model") or ""
            brand_model = f"{brand_value} {model_value}".strip()
            payload["brand_model"] = brand_model or None

        return hits


# =====================================================
# ✅ SINGLETON CLIENT (для metrics и прямого доступа)
# =====================================================

QDRANT_HOST = os.getenv("QDRANT_HOST", "qdrant")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))

qdrant_client = QdrantClient(
    host=QDRANT_HOST,
    port=QDRANT_PORT,
)