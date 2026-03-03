from typing import List, Dict, Any, Tuple
from pathlib import Path
from datetime import datetime, timezone
import yaml
from urllib.parse import urlparse
import os

from sentence_transformers import SentenceTransformer
from qdrant_client.models import Filter, FieldCondition, MatchValue

_model = None


def get_model():
    global _model
    if _model is None:
        print("[API][EMBED] loading model: intfloat/multilingual-e5-base")
        _model = SentenceTransformer("intfloat/multilingual-e5-base")
        print("[API][EMBED] model loaded")
    return _model


def embed_query(text: str):
    model = get_model()
    vec = model.encode(text).tolist()
    print(f"[API][DEBUG] vector_length={len(vec)}")
    return vec


from integrations.vector_db.qdrant import QdrantStore
from services.query_parser import StructuredQuery

from db.session import SessionLocal
from db.models import SearchHistory


# =========================
# LOAD BRANDS WHITELIST
# =========================
def load_brands() -> dict:
    try:
        base_dir = Path(__file__).resolve().parent.parent
        brands_path = base_dir / "config" / "brands.yaml"

        with open(brands_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            return data.get("brands", {})

    except Exception as e:
        print(f"[SEARCH][WARN] failed to load brands.yaml: {e}")
        return {}


BRANDS_WHITELIST = load_brands()
WHITELIST_SET = set(BRANDS_WHITELIST.keys())


# =========================
# SOURCE BOOSTS
# =========================
SOURCE_BOOSTS = {
    "benzclub.ru": 1.5,
    "bmwclub.ru": 1.5,
    "forum": 1.5,
    "telegram": 1.0,
    "auto.ru": 0.8,
    "drom.ru": 0.8,
    "avito.ru": 0.8,
    "marketplace": 0.8,
}

# =========================
# FAIRNESS CONFIG
# =========================
MAX_RESULTS_PER_SOURCE: int = 40
DOMAIN_PENALTY_K: float = 0.4

# =========================
# RECENCY CONFIG
# =========================
RECENCY_MAX_DAYS = 180
RECENCY_WEIGHT = 1.0

# =========================
# DEMO MODE
# =========================
DEMO_SEARCH_MODE = os.getenv("DEMO_SEARCH_MODE", "true").lower() == "true"
MIN_DEMO_SCORE = float(os.getenv("DEMO_MIN_SCORE", "0.0001"))


class SearchService:
    def __init__(self):
        self.store = QdrantStore()

    # =====================================================
    # MAIN SEARCH
    # =====================================================

    def search(
        self,
        structured: StructuredQuery,
        limit: int = None,
        top_k: int = None,
    ) -> List[Dict[str, Any]]:

        if limit is None:
            limit = int(os.getenv("SEARCH_LIMIT", "50"))

        if top_k is None:
            top_k = int(os.getenv("SEARCH_TOP_K", "120"))

        debug = {
            "skipped_by_price": 0,
            "skipped_by_price_null": 0,
            "skipped_by_mileage": 0,
            "skipped_by_mileage_null": 0,
            "skipped_by_url_duplicate": 0,
        }

        query_text = self._build_query_text(structured)
        query_vector = embed_query(query_text)

        # =========================
        # QDRANT MUST FILTER
        # =========================
        must_conditions = []

        if structured.brand:
            must_conditions.append(
                FieldCondition(
                    key="brand",
                    match=MatchValue(value=structured.brand.lower()),
                )
            )

        if structured.fuel:
            must_conditions.append(
                FieldCondition(
                    key="fuel",
                    match=MatchValue(value=structured.fuel.lower()),
                )
            )

        qdrant_filter = Filter(must=must_conditions) if must_conditions else None

        try:
            hits = self.store.search(
                vector=query_vector,
                limit=top_k,
                query_filter=qdrant_filter,
            )
        except Exception as e:
            print(f"[SEARCH][WARN] qdrant unavailable: {e}")
            return []

        if not hits:
            return []

        # =========================
        # DOC-LEVEL AGGREGATION
        # =========================
        doc_scores = {}
        doc_payloads = {}

        for hit in hits:
            payload = hit.payload or {}
            url = payload.get("source_url")
            if not url:
                continue

            if url not in doc_scores or hit.score > doc_scores[url]:
                doc_scores[url] = hit.score
                doc_payloads[url] = payload

        # =========================
        # FILTER + RANK
        # =========================
        scored_results = []

        for url, payload in doc_payloads.items():
            semantic_score = doc_scores[url]

            # STRICT PRICE FILTER
            if structured.price_max is not None:
                if payload.get("price") is None:
                    debug["skipped_by_price_null"] += 1
                    continue
                if payload["price"] > structured.price_max:
                    debug["skipped_by_price"] += 1
                    continue

            # STRICT MILEAGE
            if structured.mileage_max is not None:
                if payload.get("mileage") is None:
                    debug["skipped_by_mileage_null"] += 1
                    continue
                if payload["mileage"] > structured.mileage_max:
                    debug["skipped_by_mileage"] += 1
                    continue

            final_score = self._rank(semantic_score, payload)

            scored_results.append((final_score, payload))

        scored_results.sort(key=lambda x: x[0], reverse=True)

        results: List[Dict[str, Any]] = []
        seen_urls = set()

        for final_score, payload in scored_results:
            source_url = payload.get("source_url")

            if not source_url:
                continue

            if source_url in seen_urls:
                debug["skipped_by_url_duplicate"] += 1
                continue

            results.append(
                {
                    "brand": payload.get("brand"),
                    "model": payload.get("model"),
                    "year": payload.get("year"),
                    "mileage": payload.get("mileage"),
                    "price": payload.get("price"),
                    "currency": payload.get("currency", "RUB"),
                    "fuel": payload.get("fuel"),
                    "region": payload.get("region"),
                    "paint_condition": payload.get("paint_condition"),
                    "score": round(final_score, 6),
                    "why_match": f"semantic={round(final_score,4)}",
                    "source_url": source_url,
                    "source_name": payload.get("source"),
                }
            )

            seen_urls.add(source_url)

            if len(results) >= limit:
                break

        print(f"[DEBUG] filters={must_conditions}")
        print(f"[DEBUG] stats={debug}")

        try:
            session = SessionLocal()
            history = SearchHistory(
                raw_query=structured.raw_query,
                structured_query=structured.dict(),
                results_count=len(results),
                empty_result=len(results) == 0,
                source="search_api",
            )
            session.add(history)
            session.commit()
        except Exception as e:
            print(f"[SEARCH][WARN] history save failed: {e}")
        finally:
            try:
                session.close()
            except Exception:
                pass

        return results

    # =====================================================
    # RANKING
    # =====================================================

    def _rank(self, semantic_score, payload):
        recency = self._recency_score(payload)
        mileage_score = self._mileage_score(payload)
        price_score = self._price_score(payload)

        return (
            semantic_score * 0.6
            + recency * 0.15
            + mileage_score * 0.15
            + price_score * 0.10
        )

    def _recency_score(self, payload):
        ts = payload.get("created_at_ts")
        if not ts:
            return 0
        now = int(datetime.now(tz=timezone.utc).timestamp())
        age_days = (now - ts) / 86400
        return max(0, 1 - age_days / 180)

    def _mileage_score(self, payload):
        mileage = payload.get("mileage")
        if mileage is None:
            return 0
        return max(0, 1 - mileage / 300000)

    def _price_score(self, payload):
        price = payload.get("price")
        if price is None:
            return 0
        return max(0, 1 - price / 5000000)

    # =====================================================
    # HELPERS
    # =====================================================

    def _build_query_text(self, structured: StructuredQuery) -> str:
        parts: List[str] = []

        if structured.brand:
            parts.append(structured.brand)
        if structured.model:
            parts.append(structured.model)
        if structured.fuel:
            parts.append(structured.fuel)
        if structured.paint_condition:
            parts.append(structured.paint_condition)

        location = getattr(structured, "region", None) or getattr(structured, "city", None)
        if location:
            parts.append(location)

        if structured.keywords:
            parts.extend(structured.keywords)

        return " ".join(parts).strip()

    # =====================================================
    # SCORING (legacy)
    # =====================================================

    def _score_hit(
        self,
        vector_score: float,
        payload: Dict[str, Any],
        structured: StructuredQuery,
        source_rank: int,
        domain_rank: int,
    ) -> Tuple[float, List[str]]:

        reasons: List[str] = []

        semantic_score = vector_score
        reasons.append(f"semantic={round(semantic_score, 4)}")

        source = payload.get("source") or ""
        source_boost = SOURCE_BOOSTS.get(source, 1.0)

        payload_brand = payload.get("brand")
        brand_boost = 1.15 if payload_brand and payload_brand in WHITELIST_SET else 0.9

        sale_boost = 1.1 if str(payload.get("sale_intent")) == "1" else 0.85

        created_at_ts = payload.get("created_at_ts")
        recency_score = 0.0
        if isinstance(created_at_ts, (int, float)):
            now_ts = int(datetime.now(tz=timezone.utc).timestamp())
            age_days = max(0.0, (now_ts - int(created_at_ts)) / 86400)
            recency_score = max(0.0, 1.0 - age_days / RECENCY_MAX_DAYS)

        diversity_penalty = 1.0 / (1.0 + source_rank * 0.5)
        domain_penalty = 1.0 / (1.0 + domain_rank * DOMAIN_PENALTY_K)

        final_score = (
            (semantic_score + recency_score)
            * source_boost
            * brand_boost
            * sale_boost
            * diversity_penalty
            * domain_penalty
        )

        return final_score, reasons