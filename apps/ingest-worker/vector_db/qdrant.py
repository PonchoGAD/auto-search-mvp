import os
from datetime import datetime, timezone
from typing import List, Optional
from uuid import uuid4

from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct

from pathlib import Path
import yaml
import re


# =====================================================
# SAFE INT NORMALIZATION
# =====================================================

def safe_int(value: str | None):
    if not value:
        return None

    if isinstance(value, int):
        return value

    cleaned = str(value).replace("\xa0", "").replace(" ", "")
    cleaned = re.sub(r"[^\d]", "", cleaned)

    if not cleaned:
        return None

    try:
        return int(cleaned)
    except Exception:
        return None


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
WHITELIST_SET = set(BRANDS_CONFIG.keys())


def detect_brand(source_url, title, content):
    url = (source_url or "").lower()
    t = (title or "").lower()
    c = (content or "").lower()

    if "benzclub.ru" in url:
        return "mercedes"

    if "bmwclub" in url:
        return "bmw"

    if "toyotaclub" in url:
        return "toyota"

    for brand in WHITELIST_SET:
        if not brand:
            continue

        b = brand.lower()

        if b in url:
            return brand

        if b in t:
            return brand

        if b in c:
            return brand

    return None


def extract_price(text: str):
    text = text.lower().replace("\xa0", " ")

    patterns = [
        r"(\d[\d\s]\d)\s(₽|руб|р)",
        r"цена[:\s](\d[\d\s]\d)",
    ]

    for p in patterns:
        m = re.search(p, text)
        if m:
            return safe_int(m.group(1))

    return None


def extract_year(text: str):
    m = re.search(r"\b(20\d{2}|19\d{2})\b", text)
    if m:
        return int(m.group(1))
    return None


def extract_mileage(text: str):
    t = (text or "").lower().replace("\xa0", " ")

    m = re.search(
        r"(?:пробег[:\s])?([\d\s\xa0.,]{2,})\s(км|km)\b",
        t
    )
    if m:
        digits = re.sub(r"[^\d]", "", m.group(1))
        if digits:
            val = int(digits)
            if 1000 <= val <= 2_000_000:
                return val
    return None


def extract_fuel(text: str):
    t = (text or "").lower()

    if "гибрид" in t:
        return "hybrid"
    if "дизель" in t or "диз" in t:
        return "diesel"
    if "электро" in t or "электр" in t:
        return "electric"
    if "газ/бензин" in t or "гбо" in t:
        return "gas_petrol"
    if "бенз" in t:
        return "petrol"
    return None


def extract_paint_condition(text: str):
    t = (text or "").lower()

    if "без окрас" in t or "родная краска" in t:
        return "original"
    if "крашен" in t or "бит" in t:
        return "repainted"
    return None


def extract_city(text: str):
    cities = ["москва", "спб", "питер", "екатеринбург", "казань"]
    t = (text or "").lower()
    for c in cities:
        if c in t:
            return c
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
    # PAYLOAD NORMALIZATION
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

        if not chunk_text or len(chunk_text) < 30:
            return None

        text_blob = f"{document.title or ''}\n{document.content or ''}"

        brand = detect_brand(
            document.source_url,
            document.title,
            document.content,
        )
        if brand:
            brand = brand.lower().strip()

        # 🔥 META EXTRACTION (UPDATED)
        price = extract_price(text_blob)
        year = extract_year(text_blob)
        mileage = extract_mileage(text_blob)
        fuel = extract_fuel(text_blob)
        paint_condition = extract_paint_condition(text_blob)
        city = extract_city(text_blob)

        if price and price > 100_000_000:
            price = None

        if mileage and mileage > 2_000_000:
            mileage = None

        payload = {
            "source": document.source,
            "source_url": document.source_url,
            "title": document.title,
            "content": document.content,
            "brand": brand if brand else None,
            "price": price,
            "year": year,
            "mileage": mileage,
            "fuel": fuel,
            "paint_condition": paint_condition,
            "city": city,
            "region": None,
            "sale_intent": 1,
        }

        if hasattr(document, "created_at"):
            payload["created_at"] = getattr(document, "created_at", None)

        if hasattr(document, "created_at_ts"):
            payload["created_at_ts"] = getattr(document, "created_at_ts", None)

        print(
            f"[INDEX][META] brand={brand} price={price} mileage={mileage} fuel={fuel} city={city}",
            flush=True
        )

        return PointStruct(
            id=str(uuid4()),
            vector=vector,
            payload=payload,
        )