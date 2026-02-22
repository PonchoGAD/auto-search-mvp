import os
from datetime import datetime, timezone
from typing import List, Optional
from uuid import uuid4

from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct

from pathlib import Path
import yaml
import re


COLLECTION_NAME = "auto_search_chunks"


# =====================================================
# BRAND + META EXTRACTION
# =====================================================

def _load_brands_config():
    base_dir = Path(__file__).resolve().parent.parent
    brands_path = base_dir / "config" / "brands.yaml"
    with open(brands_path, "r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    return data.get("brands", {})


BRANDS_CONFIG = _load_brands_config()


def detect_brand(text: str):
    t = (text or "").lower()

    for brand, cfg in BRANDS_CONFIG.items():
        for w in cfg.get("en", []) + cfg.get("ru", []):
            if re.search(rf"\b{re.escape(w.lower())}\b", t):
                return brand
        for a in cfg.get("aliases", []):
            if re.search(rf"\b{re.escape(a.lower())}\b", t):
                return brand
    return None


def extract_price(text: str):
    m = re.search(r"(\d[\d\s]{3,})\s*₽", text)
    if m:
        return int(m.group(1).replace(" ", ""))
    return None


def extract_year(text: str):
    m = re.search(r"\b(20\d{2}|19\d{2})\b", text)
    if m:
        return int(m.group(1))
    return None


def extract_mileage(text: str):
    m = re.search(r"(\d[\d\s]{2,})\s*(км|km)", text.lower())
    if m:
        return int(m.group(1).replace(" ", ""))
    return None


def is_catalog_url(url: str):
    u = (url or "").lower()

    if "avito.ru/all/" in u:
        return True

    if re.match(r"^https?://auto\.drom\.ru/[^/]+/?$", u):
        return True

    if u.endswith("/"):
        return True

    return False


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

        if is_catalog_url(document.source_url):
            return None

        text_blob = f"{document.title or ''}\n{document.content or ''}"

        brand = detect_brand(text_blob)
        price = extract_price(text_blob)
        year = extract_year(text_blob)
        mileage = extract_mileage(text_blob)

        payload = {
            "source": document.source,
            "source_url": document.source_url,
            "title": document.title,
            "content": document.content,

            "brand": brand,
            "price": price,
            "year": year,
            "mileage": mileage,

            "sale_intent": 0 if is_catalog_url(document.source_url) else 1,
        }

        if hasattr(document, "fuel"):
            payload["fuel"] = getattr(document, "fuel", None)

        if hasattr(document, "region"):
            payload["region"] = getattr(document, "region", None)

        if hasattr(document, "created_at"):
            payload["created_at"] = getattr(document, "created_at", None)

        if hasattr(document, "created_at_ts"):
            payload["created_at_ts"] = getattr(document, "created_at_ts", None)

        return PointStruct(
            id=str(uuid4()),
            vector=vector,
            payload=payload,
        )