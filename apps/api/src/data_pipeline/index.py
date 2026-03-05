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
from services.ingest_quality import detect_brand as detect_brand_cfg


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
# FIELD EXTRACTION (MINIMAL BUT STRICT)
# =====================================================

def extract_fields_from_text(text: str) -> dict:
    lower = (text or "").lower()

    # year
    year = None
    m = re.search(r"\b(19\d{2}|20\d{2})\b", lower)
    if m:
        try:
            year = int(m.group(1))
        except Exception:
            year = None

    # mileage (km / тыс)
    mileage = None
    m = re.search(r"(\d[\d\s]{1,8})\s*(км|тыс)\b", lower)
    if m:
        try:
            num = int(m.group(1).replace(" ", ""))
            mileage = num * 1000 if m.group(2) == "тыс" else num
        except Exception:
            mileage = None

    # price (RUB only here)
    price = None
    currency = None
    m = re.search(r"(до|<=|<)?\s*(\d[\d\s]{1,10})\s*(₽|руб|р\.|р)\b", lower)
    if m:
        try:
            price = int(m.group(2).replace(" ", ""))
            currency = "RUB"
        except Exception:
            price = None
            currency = None

    # fuel
    fuel = None
    if "бенз" in lower:
        fuel = "petrol"
    elif "диз" in lower:
        fuel = "diesel"
    elif "гибрид" in lower:
        fuel = "hybrid"
    elif "электро" in lower or "электр" in lower:
        fuel = "electric"

    # paint_condition
    paint_condition = None
    if "без окрас" in lower or "не бит" in lower or "родная краска" in lower:
        paint_condition = "original"
    elif "крашен" in lower or "бит" in lower:
        paint_condition = "repainted"

    # sanity (hard)
    if isinstance(year, int):
        now_y = datetime.now(tz=timezone.utc).year
        if year < 1985 or year > now_y + 1:
            year = None

    if isinstance(price, int):
        if price < 10000 or price > 200000000:
            price = None
            currency = None

    if isinstance(mileage, int):
        if mileage < 0 or mileage > 1000000:
            mileage = None

    return {
        "year": year,
        "mileage": mileage,
        "price": price,
        "currency": currency,
        "fuel": fuel,
        "paint_condition": paint_condition,
    }


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

        vector = deterministic_embedding(text)
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

        # brand: unified detector (brands.yaml)
        brand_key, brand_conf = detect_brand_cfg(text)

        # fallback brand from url/title/content only if detector failed
        brand = brand_key or detect_brand(doc.source_url, doc.title, doc.content)

        model = detect_model(doc.source_url)

        fields = extract_fields_from_text(text)

        # normalize brand/model/fuel to lowercase (search_service expects lowercase strict matches)
        brand_norm = brand.lower().strip() if isinstance(brand, str) else None
        model_norm = model.lower().strip() if isinstance(model, str) else None

        fuel_norm = fields["fuel"].lower().strip() if isinstance(fields.get("fuel"), str) else None
        if fuel_norm not in {"petrol", "diesel", "hybrid", "electric"}:
            fuel_norm = None

        # Hard gate: if no structured signals, indexing this point harms precision
        if fields["price"] is None and fields["year"] is None and fields["mileage"] is None and brand_norm is None:
            continue

        payload = {
            "source": doc.source,
            "source_url": doc.source_url,
            "brand": brand_norm,
            "brand_confidence": float(brand_conf or 0.0),
            "model": model_norm,

            "price": fields["price"],
            "currency": fields["currency"],
            "mileage": fields["mileage"],
            "year": fields["year"],
            "fuel": fuel_norm,
            "region": None,
            "paint_condition": fields["paint_condition"],

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