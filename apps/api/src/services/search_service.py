from typing import List, Dict, Any, Tuple
from pathlib import Path
from datetime import datetime, timezone
import yaml
from urllib.parse import urlparse
import os

from integrations.vector_db.qdrant import QdrantStore
from data_pipeline.index import deterministic_embedding
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
MAX_RESULTS_PER_SOURCE: int = 3
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
        limit: int = 20,
        top_k: int = 50,
    ) -> List[Dict[str, Any]]:

        query_text = self._build_query_text(structured)
        query_vector = deterministic_embedding(query_text)

        try:
            hits = self.store.search(
                vector=query_vector,
                limit=top_k,
            )
        except Exception as e:
            print(f"[SEARCH][DEMO][WARN] qdrant unavailable: {e}")
            return []

        if not hits:
            print("[SEARCH][DEMO] hits=0")
            return []

        results: List[Dict[str, Any]] = []
        seen_urls = set()
        source_counter: Dict[str, int] = {}
        domain_counter: Dict[str, int] = {}

        for hit in hits:
            payload = hit.payload or {}
            source_url = payload.get("url")

            if not source_url or source_url in seen_urls:
                continue

            source_name = payload.get("source") or "unknown"
            source_counter.setdefault(source_name, 0)

            if source_counter[source_name] >= MAX_RESULTS_PER_SOURCE:
                continue

            domain = "unknown"
            try:
                parsed = urlparse(source_url)
                if parsed.netloc:
                    domain = parsed.netloc.lower()
            except Exception:
                pass

            domain_counter.setdefault(domain, 0)

            final_score, reasons = self._score_hit(
                vector_score=float(hit.score or 0.0),
                payload=payload,
                structured=structured,
                source_rank=source_counter[source_name],
                domain_rank=domain_counter[domain],
            )

            # 🔥 DEMO GUARANTEE
            if DEMO_SEARCH_MODE and final_score <= 0:
                final_score = MIN_DEMO_SCORE
                reasons.append("demo_fallback")

            if not DEMO_SEARCH_MODE and final_score <= 0:
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
                    "why_match": " + ".join(reasons),
                    "source_url": source_url,
                    "source_name": source_name,
                }
            )

            seen_urls.add(source_url)
            source_counter[source_name] += 1
            domain_counter[domain] += 1

            if len(results) >= limit:
                break

        results.sort(key=lambda r: r["score"], reverse=True)

        # -------------------------
        # SAVE SEARCH HISTORY
        # -------------------------
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
        if structured.region:
            parts.append(structured.region)
        if structured.keywords:
            parts.extend(structured.keywords)

        return " ".join(parts).strip()

    # =====================================================
    # SCORING
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
