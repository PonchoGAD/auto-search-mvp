from typing import List, Dict, Any, Tuple
from pathlib import Path
from datetime import datetime, timezone
import yaml
from urllib.parse import urlparse
import os

from qdrant_client.models import Filter, FieldCondition, MatchValue

from shared.embeddings.provider import embed_text

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
# FAIRNESS CONFIG
# =========================
MAX_RESULTS_PER_SOURCE: int = 40

# =========================
# RECENCY CONFIG
# =========================
RECENCY_MAX_DAYS = 180
RECENCY_WEIGHT = 1.0


class SearchService:
    def __init__(self):
        self.store = QdrantStore()

    # =====================================================
    # MAIN SEARCH (Production Search V2)
    # =====================================================

    def search(
        self,
        structured: StructuredQuery,
        limit: int = None,
        top_k: int = None,
    ) -> List[Dict[str, Any]]:

        if limit is None:
            limit = int(os.getenv("SEARCH_LIMIT", "50"))

        # V2 retrieval default: 200
        if top_k is None:
            top_k = int(os.getenv("SEARCH_TOP_K", "200"))

        env = (os.getenv("ENV", "") or os.getenv("APP_ENV", "") or "dev").lower()
        is_prod = env == "prod"

        brand_conf = float(getattr(structured, "brand_confidence", 0.0) or 0.0)
        brand_value = (structured.brand or "").strip().lower() if structured.brand else None
        fuel_value = (structured.fuel or "").strip().lower() if structured.fuel else None

        # -------------------------
        # DEBUG COUNTERS
        # -------------------------
        debug = {
            "applied_filters": [],
            "skipped_by_price": 0,
            "skipped_by_price_null": 0,
            "skipped_by_mileage": 0,
            "skipped_by_mileage_null": 0,
            "skipped_by_year": 0,
            "skipped_by_year_null": 0,
            "skipped_by_url_duplicate": 0,
            "fallback_triggered": False,
        }

        # -------------------------
        # EMBEDDING (provider-consistent)
        # -------------------------
        query_text = self._build_query_text(structured)
        query_vector = embed_text(query_text)

        # =====================================================
        # 1) QDRANT FILTER LAYER (brand strict by confidence + fuel strict)
        # =====================================================

        must_conditions: List[FieldCondition] = []

        strict_brand = bool(brand_value and brand_conf >= 0.9)
        if strict_brand:
            must_conditions.append(
                FieldCondition(
                    key="brand",
                    match=MatchValue(value=brand_value),
                )
            )
            debug["applied_filters"].append(f"brand=strict({brand_value})")

        # Fuel strict MUST always if provided (fuel not used in ranking)
        if fuel_value:
            must_conditions.append(
                FieldCondition(
                    key="fuel",
                    match=MatchValue(value=fuel_value),
                )
            )
            debug["applied_filters"].append(f"fuel=strict({fuel_value})")

        qdrant_filter = Filter(must=must_conditions) if must_conditions else None

        # =====================================================
        # 2) SEMANTIC RETRIEVAL (top_k)
        # =====================================================

        try:
            hits = self.store.search(
                vector=query_vector,
                limit=top_k,
                query_filter=qdrant_filter,
            )
        except Exception as e:
            print(f"[SEARCH][WARN] qdrant unavailable: {e}", flush=True)
            return []

        # =====================================================
        # 2.1) FALLBACK UX
        # strict brand filter gave 0 -> retry without brand, keep fuel + numeric post-filters
        # =====================================================

        if strict_brand and not hits:
            debug["fallback_triggered"] = True

            fallback_must: List[FieldCondition] = []
            if fuel_value:
                fallback_must.append(
                    FieldCondition(
                        key="fuel",
                        match=MatchValue(value=fuel_value),
                    )
                )

            fallback_filter = Filter(must=fallback_must) if fallback_must else None

            try:
                hits = self.store.search(
                    vector=query_vector,
                    limit=top_k,
                    query_filter=fallback_filter,
                )
            except Exception as e:
                print(f"[SEARCH][WARN] qdrant fallback unavailable: {e}", flush=True)
                return []

        if not hits:
            print(f"[DEBUG] filters={debug['applied_filters']}", flush=True)
            print(f"[DEBUG] stats={debug}", flush=True)
            return []

        # =====================================================
        # 3) DOC-LEVEL AGGREGATION (by source_url, keep best semantic)
        # =====================================================

        doc_scores: Dict[str, float] = {}
        doc_payloads: Dict[str, Dict[str, Any]] = {}

        for hit in hits:
            payload = hit.payload or {}
            url = payload.get("source_url")
            if not url:
                continue

            score = float(getattr(hit, "score", 0.0) or 0.0)

            if url not in doc_scores or score > doc_scores[url]:
                doc_scores[url] = score
                doc_payloads[url] = payload

        # =====================================================
        # 4) STRICT POST-FILTER (NULL SAFE) + 5) RANKING
        # =====================================================

        scored_results: List[Tuple[float, Dict[str, Any], List[str]]] = []

        for url, payload in doc_payloads.items():
            semantic = float(doc_scores.get(url, 0.0) or 0.0)

            # PROD safety: drop dev_seed in prod
            if is_prod and (payload.get("source") == "dev_seed"):
                continue

            # -------------------------
            # STRICT NUMERIC FILTERS (NULL SAFE)
            # -------------------------

            # price_max
            if structured.price_max is not None:
                if payload.get("price") is None:
                    debug["skipped_by_price_null"] += 1
                    continue
                try:
                    if payload["price"] > structured.price_max:
                        debug["skipped_by_price"] += 1
                        continue
                except Exception:
                    debug["skipped_by_price"] += 1
                    continue

            # mileage_max
            if structured.mileage_max is not None:
                if payload.get("mileage") is None:
                    debug["skipped_by_mileage_null"] += 1
                    continue
                try:
                    if payload["mileage"] > structured.mileage_max:
                        debug["skipped_by_mileage"] += 1
                        continue
                except Exception:
                    debug["skipped_by_mileage"] += 1
                    continue

            # year_min
            if structured.year_min is not None:
                if payload.get("year") is None:
                    debug["skipped_by_year_null"] += 1
                    continue
                try:
                    if payload["year"] < structured.year_min:
                        debug["skipped_by_year"] += 1
                        continue
                except Exception:
                    debug["skipped_by_year"] += 1
                    continue

            # -------------------------
            # RANKING LAYER (transparent)
            # final = semantic*0.7 + recency*0.15 + sale_bonus*0.1 + completeness*0.05
            # + brand_boost (only if confidence>=0.5 and exact match)
            # -------------------------

            recency = self._recency_score(payload)
            sale_bonus = self._sale_bonus(payload)
            completeness = self._completeness_score(payload)

            final_score = (
                semantic * 0.70
                + recency * 0.15
                + sale_bonus * 0.10
                + completeness * 0.05
            )

            brand_boost = 0.0
            if brand_value and brand_conf >= 0.5:
                if payload.get("brand") == brand_value:
                    brand_boost = 0.15

            final_score = final_score + brand_boost

            reasons = [
                f"semantic={round(semantic, 4)}",
                f"recency={round(recency, 4)}",
                f"sale={round(sale_bonus, 4)}",
                f"complete={round(completeness, 4)}",
                f"brand_boost={round(brand_boost, 4)}",
                f"final={round(final_score, 4)}",
            ]

            scored_results.append((final_score, payload, reasons))

        scored_results.sort(key=lambda x: x[0], reverse=True)

        # =====================================================
        # 6) RESPONSE BUILD (API unchanged)
        # =====================================================

        results: List[Dict[str, Any]] = []
        seen_urls = set()
        source_counter: Dict[str, int] = {}

        for final_score, payload, reasons in scored_results:
            source_url = payload.get("source_url")
            if not source_url:
                continue

            if source_url in seen_urls:
                debug["skipped_by_url_duplicate"] += 1
                continue

            source_name = payload.get("source") or "unknown"
            source_counter.setdefault(source_name, 0)
            if source_counter[source_name] >= MAX_RESULTS_PER_SOURCE:
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

            if len(results) >= limit:
                break

        print(f"[DEBUG] filters={debug['applied_filters']}", flush=True)
        print(f"[DEBUG] stats={debug}", flush=True)

        # =====================================================
        # HISTORY SAVE (unchanged)
        # =====================================================

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
            print(f"[SEARCH][WARN] history save failed: {e}", flush=True)
        finally:
            try:
                session.close()
            except Exception:
                pass

        return results

    # =====================================================
    # RANKING HELPERS (V2)
    # =====================================================

    def _recency_score(self, payload: Dict[str, Any]) -> float:
        ts = payload.get("created_at_ts")
        if not ts:
            return 0.0
        try:
            now_ts = int(datetime.now(tz=timezone.utc).timestamp())
            age_days = (now_ts - int(ts)) / 86400
            return max(0.0, 1.0 - age_days / float(RECENCY_MAX_DAYS))
        except Exception:
            return 0.0

    def _sale_bonus(self, payload: Dict[str, Any]) -> float:
        # normalized 0..1 bonus
        try:
            return 1.0 if str(payload.get("sale_intent")) == "1" else 0.0
        except Exception:
            return 0.0

    def _completeness_score(self, payload: Dict[str, Any]) -> float:
        keys = ["price", "mileage", "year", "brand", "model", "fuel"]
        present = 0
        for k in keys:
            if payload.get(k) is not None:
                present += 1
        return present / float(len(keys)) if keys else 0.0

    def _price_score(self, payload: Dict[str, Any], structured: StructuredQuery) -> float:
        """
        Price score fix (null-safe, no division by 0).

        If structured.price_max set:
            score = max(0, 1 - price/price_max)
        else:
            fallback = max(0, 1 - price/5_000_000)
        """
        price = payload.get("price")
        if price is None:
            return 0.0

        try:
            price_val = float(price)
        except Exception:
            return 0.0

        if structured.price_max is not None:
            try:
                denom = float(structured.price_max)
            except Exception:
                denom = 0.0

            if denom <= 0.0:
                return 0.0

            return max(0.0, 1.0 - (price_val / denom))

        denom = 5_000_000.0
        return max(0.0, 1.0 - (price_val / denom))

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
        source_boost = 1.0

        payload_brand = payload.get("brand")
        brand_boost = 1.0

        sale_boost = 1.0

        created_at_ts = payload.get("created_at_ts")
        recency_score = 0.0
        if isinstance(created_at_ts, (int, float)):
            now_ts = int(datetime.now(tz=timezone.utc).timestamp())
            age_days = max(0.0, (now_ts - int(created_at_ts)) / 86400)
            recency_score = max(0.0, 1.0 - age_days / RECENCY_MAX_DAYS)

        diversity_penalty = 1.0
        domain_penalty = 1.0

        final_score = (
            (semantic_score + recency_score)
            * source_boost
            * brand_boost
            * sale_boost
            * diversity_penalty
            * domain_penalty
        )

        return final_score, reasons