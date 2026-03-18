import os
from datetime import datetime, timezone
from typing import List, Optional, Any, Dict

from qdrant_client import QdrantClient
from qdrant_client.models import (
    Distance,
    PointStruct,
    SearchParams,
    VectorParams,
)


COLLECTION_NAME = "auto_search_chunks"
QDRANT_DEBUG = os.getenv("QDRANT_DEBUG", "0").strip() == "1"


class QdrantStore:
    """
    Qdrant vector storage.

    Canonical payload contract:
    {
      "raw_id": int | None,
      "normalized_id": int | None,
      "source": str | None,
      "source_url": str | None,
      "brand": str | None,
      "model": str | None,
      "year": int | None,
      "mileage": int | None,
      "price": int | None,
      "fuel": str | None,
      "sale_intent": int | None,
      "quality_score": float | None,
      "chunk_index": int | None,
      "created_at_ts": int | None,
    }

    Notes:
    - brand/model must be canonical keys only
    - qdrant.py does not recover taxonomy, only validates and normalizes schema
    - created_at fields are hardened for recency-safe retrieval
    """

    ALLOWED_FUELS = {"petrol", "diesel", "hybrid", "electric", "gas", "gas_petrol"}

    def __init__(self, host: str = "qdrant", port: int = 6333):
        self.client = QdrantClient(
            host=host,
            port=port,
            check_compatibility=False,
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
                    "ef_construct": 256,
                    "full_scan_threshold": 1000,
                },
            )
            print(f"[QDRANT] collection created: {COLLECTION_NAME}", flush=True)

    # =====================================================
    # DEBUG HELPERS
    # =====================================================

    def _debug_log(self, message: str) -> None:
        if QDRANT_DEBUG:
            print(f"[QDRANT][DEBUG] {message}", flush=True)

    def _summarize_filter(self, query_filter: object) -> str:
        if query_filter is None:
            return "none"

        try:
            if isinstance(query_filter, dict):
                keys = sorted(query_filter.keys())
                return ",".join(keys) if keys else "dict_empty"

            summary_parts: List[str] = []

            for attr in ("must", "should", "must_not"):
                value = getattr(query_filter, attr, None)
                if value:
                    summary_parts.append(f"{attr}:{len(value)}")

            if summary_parts:
                return ";".join(summary_parts)

            return query_filter.__class__.__name__
        except Exception:
            return "uninspectable_filter"

    # =====================================================
    # CANONICAL NORMALIZATION HELPERS
    # =====================================================

    def _norm_str(self, value: Any) -> Optional[str]:
        if isinstance(value, str):
            value = value.strip().lower()
            return value or None
        return None

    def _norm_int(self, value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            if isinstance(value, str):
                value = value.replace(" ", "").replace("\u00A0", "").replace("\xa0", "")
            return int(value)
        except Exception:
            return None

    def _norm_float(self, value: Any) -> Optional[float]:
        if value is None:
            return None
        try:
            return float(value)
        except Exception:
            return None

    def _norm_sale_intent(self, value: Any) -> Optional[int]:
        if value is None:
            return None
        if isinstance(value, bool):
            return 1 if value else 0
        if isinstance(value, (int, float)):
            return 1 if int(value) > 0 else 0
        if isinstance(value, str):
            s = value.strip().lower()
            if s in {"1", "true", "yes", "y"}:
                return 1
            if s in {"0", "false", "no", "n"}:
                return 0
        return None

    def _normalize_created_at(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Ensures:
        - created_at (ISO str)
        - created_at_ts (int)
        - created_at_source (source | ingested | normalized)
        """

        raw = payload.get("created_at")

        if isinstance(raw, datetime):
            dt = raw
            if not dt.tzinfo:
                dt = dt.replace(tzinfo=timezone.utc)

            payload["created_at"] = dt.isoformat()
            payload["created_at_ts"] = int(dt.timestamp())
            payload.setdefault("created_at_source", "source")
            return payload

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

        created_at_ts = self._norm_int(payload.get("created_at_ts"))
        if created_at_ts is not None and created_at_ts > 0:
            dt = datetime.fromtimestamp(created_at_ts, tz=timezone.utc)
            payload["created_at"] = dt.isoformat()
            payload["created_at_ts"] = created_at_ts
            payload.setdefault("created_at_source", "normalized")
            return payload

        now = datetime.now(tz=timezone.utc)
        payload["created_at"] = now.isoformat()
        payload["created_at_ts"] = int(now.timestamp())
        payload["created_at_source"] = "ingested"
        return payload

    def _normalize_payload_schema(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Canonical payload normalization only.
        No taxonomy recovery or business-logic extraction.
        """

        current_year = datetime.now(tz=timezone.utc).year

        brand = self._norm_str(payload.get("brand"))
        model = self._norm_str(payload.get("model"))
        fuel = self._norm_str(payload.get("fuel"))

        if fuel not in self.ALLOWED_FUELS:
            fuel = None

        price = self._norm_int(payload.get("price"))
        if price is not None and (price <= 0 or price < 10_000 or price > 200_000_000):
            price = None

        mileage = self._norm_int(payload.get("mileage"))
        if mileage is not None and (mileage < 0 or mileage > 500_000):
            mileage = None

        year = self._norm_int(payload.get("year"))
        if year is not None and (year < 1985 or year > current_year + 1):
            year = None

        sale_intent = self._norm_sale_intent(payload.get("sale_intent"))
        quality_score = self._norm_float(payload.get("quality_score"))
        if quality_score is not None:
            quality_score = max(0.0, min(1.0, quality_score))

        normalized: Dict[str, Any] = {
            # fixed canonical schema
            "raw_id": self._norm_int(payload.get("raw_id")),
            "normalized_id": self._norm_int(payload.get("normalized_id")),
            "source": self._norm_str(payload.get("source")),
            "source_url": payload.get("source_url") if isinstance(payload.get("source_url"), str) else None,

            "brand": brand,
            "model": model,
            "year": year,
            "mileage": mileage,
            "price": price,
            "fuel": fuel,
            "sale_intent": sale_intent,
            "quality_score": quality_score,
            "chunk_index": self._norm_int(payload.get("chunk_index")),
            "created_at_ts": self._norm_int(payload.get("created_at_ts")),
        }

        # keep compatible auxiliary fields if present
        normalized["doc_id"] = self._norm_int(payload.get("doc_id"))
        normalized["chunk_id"] = self._norm_int(payload.get("chunk_id"))
        normalized["title"] = payload.get("title") if isinstance(payload.get("title"), str) else None
        normalized["title_text"] = payload.get("title_text") if isinstance(payload.get("title_text"), str) else None
        normalized["content"] = payload.get("content") if isinstance(payload.get("content"), str) else None
        normalized["currency"] = payload.get("currency") if isinstance(payload.get("currency"), str) else None
        normalized["region"] = self._norm_str(payload.get("region"))
        normalized["paint_condition"] = (
            payload.get("paint_condition") if isinstance(payload.get("paint_condition"), str) else None
        )
        normalized["vector_type"] = payload.get("vector_type") if isinstance(payload.get("vector_type"), str) else None
        normalized["brand_model"] = f"{brand or ''} {model or ''}".strip() or None
        normalized["doc_quality"] = self._norm_int(payload.get("doc_quality"))
        normalized["created_at"] = payload.get("created_at") if isinstance(payload.get("created_at"), str) else None
        normalized["created_at_source"] = (
            payload.get("created_at_source") if isinstance(payload.get("created_at_source"), str) else None
        )

        return normalized

    def build_payload(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Centralized payload builder for upsert/retrieval compatibility.
        """
        normalized = self._normalize_payload_schema(payload)
        normalized = self._normalize_created_at(normalized)
        normalized["created_at_ts"] = self._norm_int(normalized.get("created_at_ts"))
        return normalized

    # =====================================================
    # UPSERT
    # =====================================================

    def upsert(self, points: List[PointStruct]):
        if not points:
            print("[QDRANT] no points to upsert", flush=True)
            return

        normalized_points: List[PointStruct] = []

        for p in points:
            payload = self.build_payload(p.payload or {})
            print(f"[DEBUG][UPSERT] brand={payload.get('brand')} model={payload.get('model')}", flush=True)

            normalized_points.append(
                PointStruct(
                    id=p.id,
                    vector=p.vector,
                    payload=payload,
                )
            )

        batch_size = 128
        total = len(normalized_points)

        for i in range(0, total, batch_size):
            batch = normalized_points[i:i + batch_size]
            self.client.upsert(
                collection_name=COLLECTION_NAME,
                points=batch,
            )

        if normalized_points:
            sample = normalized_points[0].payload
            print(f"[QDRANT] sample payload schema: {sample}", flush=True)

        print(f"[QDRANT] upserted points: {total}", flush=True)

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
        requested_limit = int(limit) if isinstance(limit, int) else 20
        if requested_limit <= 0:
            requested_limit = 20

        has_filter = query_filter is not None
        filter_summary = self._summarize_filter(query_filter)

        self._debug_log(
            f"search request limit={requested_limit} "
            f"filter_present={has_filter} "
            f"filter_summary={filter_summary}"
        )

        search_kwargs = {
            "collection_name": COLLECTION_NAME,
            "query": vector,
            "limit": requested_limit,
            "with_payload": True,
            "search_params": SearchParams(
                hnsw_ef=max(512, requested_limit * 6),
                exact=False,
            ),
        }

        if query_filter is not None:
            search_kwargs["query_filter"] = query_filter

        response = self.client.query_points(**search_kwargs)
        hits = response.points

        self._debug_log(f"vector hits returned={len(hits)}")

        if not hits and query_text:
            self._debug_log("vector search empty, trying text fallback")
            response = self.client.query_points(
                collection_name=COLLECTION_NAME,
                query=query_text,
                limit=requested_limit,
                with_payload=True,
            )
            hits = response.points
            self._debug_log(f"text fallback hits returned={len(hits)}")

        for p in hits:
            payload = p.payload or {}
            p.payload = self.build_payload(payload)

        self._debug_log(
            f"search complete limit={requested_limit} "
            f"filter_present={has_filter} "
            f"hits={len(hits)}"
        )

        return hits


QDRANT_HOST = os.getenv("QDRANT_HOST", "qdrant")
QDRANT_PORT = int(os.getenv("QDRANT_PORT", "6333"))

qdrant_client = QdrantClient(
    host=QDRANT_HOST,
    port=QDRANT_PORT,
)