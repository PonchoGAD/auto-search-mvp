#  apps\api\src\data_pipeline\index.py

import os
import hashlib
import re
from typing import List
from datetime import datetime, timezone
from pathlib import Path
import yaml

from qdrant_client.models import PointStruct

from db.models import RawDocument
from integrations.vector_db.qdrant import QdrantStore
from shared.embeddings.provider import embed_text

# =====================================================
# VECTOR CONFIG (FIXED)
# =====================================================

VECTOR_SIZE = 768


# =====================================================
# ⚠️ CRITICAL ADAPTER (DO NOT REMOVE)
# =====================================================

def deterministic_embedding(text: str) -> List[float]:
    """
    ✅ ADAPTER FOR SearchService.

    SearchService IMPORTS THIS FUNCTION.
    Therefore it MUST return the SAME embedding
    that was used during indexing.
    """
    return embed_text(text)


# =====================================================
# LOAD BRANDS WHITELIST (brands.yaml)
# =====================================================

def load_brands_whitelist() -> dict:
    try:
        base_dir = Path(__file__).resolve().parent.parent
        brands_path = base_dir / "config" / "brands.yaml"

        with open(brands_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            return data.get("brands", {})

    except Exception as e:
        print(f"[INDEX][WARN] failed to load brands.yaml: {e}")
        return {}


BRANDS_WHITELIST = load_brands_whitelist()
WHITELIST_SET = set(BRANDS_WHITELIST.keys())


# =====================================================
# 🆕 BRAND DETECTION
# =====================================================

def detect_brand(source_url, title, content):
    url = (source_url or "").lower()
    t = (title or "").lower()
    c = (content or "").lower()

    for brand in WHITELIST_SET:
        if not brand:
            continue

        b = brand.lower()

        # 1) source_url
        if b in url:
            return brand

        # 2) title
        if b in t:
            return brand

        # 3) content
        if b in c:
            return brand

    return None


# =====================================================
# 🆕 MODEL DETECTION
# =====================================================

def detect_model(source_url: str | None) -> str | None:
    if not source_url:
        return None

    url = source_url.lower()
    parts = [p for p in url.split("/") if p]

    brands = ["bmw", "audi", "mercedes", "toyota", "hyundai", "kia", "honda", "nissan"]

    for i, part in enumerate(parts):
        if part in brands:
            if i + 1 < len(parts):
                candidate = parts[i + 1]

                if not candidate:
                    return None

                if candidate.isdigit() and len(candidate) > 6:
                    return None

                if candidate.isdigit():
                    return None

                return candidate

    return None


# =====================================================
# INDEX RAW DOCUMENTS
# =====================================================

def index_raw_documents(raw_docs: List[RawDocument]) -> int:
    if not raw_docs:
        print("[INDEX][WARN] no raw documents to index")
        return 0

    store = QdrantStore()
    store.create_collection(VECTOR_SIZE)

    points: List[PointStruct] = []
    now = datetime.now(tz=timezone.utc)

    for doc in raw_docs:
        text = ((doc.title or "") + "\n" + (doc.content or "")).strip()
        if not text:
            continue

        vector = embed_text(text)
        if not vector:
            continue

        if len(vector) != VECTOR_SIZE:
            print(
                f"[INDEX][WARN] vector size mismatch doc={doc.id} "
                f"len={len(vector)} expected={VECTOR_SIZE}"
            )
            continue

        fetched_at = doc.fetched_at or now
        if fetched_at.tzinfo is None:
            fetched_at = fetched_at.replace(tzinfo=timezone.utc)

        # =====================================================
        # 🔥 SALE INTENT FILTER (SKIP CATALOGS)
        # =====================================================

        url = (doc.source_url or "").lower()

        if "/all/" in url:
            continue

        if url.endswith("/"):
            continue

        if not re.search(r"\d{6,}", url):
            continue

        brand = detect_brand(doc.source_url, doc.title, doc.content)
        model = detect_model(doc.source_url)

        payload = {
            "source": doc.source,
            "source_url": doc.source_url,
            "brand": brand,
            "model": model,
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