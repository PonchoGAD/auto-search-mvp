from typing import List, Dict, Any, Tuple
from pathlib import Path
from datetime import datetime, timezone
import yaml
from urllib.parse import urlparse
import os
import json

try:
    from qdrant_client.models import Filter, FieldCondition, MatchValue, IsNullCondition
except ImportError:
    from qdrant_client.models import Filter, FieldCondition, MatchValue
    IsNullCondition = None

from shared.embeddings.provider import embed_text
from sentence_transformers import CrossEncoder

from integrations.vector_db.qdrant import QdrantStore
from services.query_parser import StructuredQuery
from services.query_router import route_query
from services.car_intent_classifier import detect_car_intent
from services.query_expander import expand_query

from db.session import SessionLocal
from db.models import SearchHistory


# =====================================================
# CROSS ENCODER RERANKER
# =====================================================

_reranker = None


def get_reranker():
    global _reranker

    if _reranker is None:
        model_name = os.getenv(
            "RERANK_MODEL",
            "cross-encoder/ms-marco-MiniLM-L-6-v2",
        )

        print(f"[RERANK] loading model: {model_name}", flush=True)

        _reranker = CrossEncoder(model_name)

    return _reranker


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
MAX_RESULTS_PER_SOURCE: int = 20

# =========================
# RECENCY CONFIG
# =========================
RECENCY_MAX_DAYS = 180
RECENCY_WEIGHT = 1.2


print("[SEARCH] warming reranker", flush=True)
try:
    get_reranker()
except:
    pass


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

        intent = detect_car_intent(structured.raw_query)

        expanded_queries = expand_query(structured.raw_query)

        cache_key = f"search:{structured.raw_query}:{structured.brand}:{structured.model}:{structured.price_max}:{structured.mileage_max}:{structured.fuel}"

        try:
            from redis import Redis
            redis = Redis(
                host="redis",
                port=6379,
                socket_timeout=1,
                socket_connect_timeout=1
            )
            cached = redis.get(cache_key)

            if cached:
                return json.loads(cached)
        except:
            pass

        if limit is None:
            limit = int(os.getenv("SEARCH_LIMIT", "50"))

        if top_k is None:
            top_k = int(os.getenv("SEARCH_TOP_K", "120"))

        env = (os.getenv("ENV", "") or os.getenv("APP_ENV", "") or "dev").lower()
        is_prod = env == "prod"

        brand_conf = float(getattr(structured, "brand_confidence", 0.0) or 0.0)
        brand_value = (structured.brand or "").strip().lower() if structured.brand else None
        fuel_value = (structured.fuel or "").strip().lower() if structured.fuel else None
        model_value = (structured.model or "").strip().lower() if structured.model else None

        route = route_query(structured)

        strict_brand = bool(brand_value and brand_conf >= 0.9)
        allow_brandless_debug_fallback = (
            str(os.getenv("SEARCH_ALLOW_BRANDLESS_DEBUG_FALLBACK", "0")).strip().lower()
            in {"1", "true", "yes", "on"}
        )

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
            "model_fallback_triggered": False,
            "fuel_fallback_triggered": False,
            "brandless_debug_fallback_triggered": False,
            "search_stage": None,
        }

        query_text = self._build_query_text(structured)
        query_vector = embed_text(query_text)

        extra_vectors = []
        for q in expanded_queries:
            try:
                extra_vectors.append(embed_text(q))
            except Exception:
                pass

        def _build_filter(
            brand: str = None,
            model: str = None,
            fuel: str = None,
        ) -> Filter:

            must_conditions: List[FieldCondition] = []

            if brand:
                must_conditions.append(
                    FieldCondition(
                        key="brand",
                        match=MatchValue(value=brand),
                    )
                )

            if model:
                must_conditions.append(
                    FieldCondition(
                        key="model",
                        match=MatchValue(value=model),
                    )
                )

            if fuel:
                must_conditions.append(
                    FieldCondition(
                        key="fuel",
                        match=MatchValue(value=fuel),
                    )
                )

            if not must_conditions:
                return None

            return Filter(
                must=must_conditions,
            )

        def _search_vectors(query_filter: Filter) -> List[Any]:
            all_hits = []
            vectors = [query_vector] + extra_vectors

            for vec in vectors:
                try:
                    sub_hits = self.store.search(
                        vector=vec,
                        limit=top_k,
                        query_filter=query_filter,
                    )
                    all_hits.extend(sub_hits)
                except Exception:
                    pass

            return all_hits

        if route == "structured":
            primary_filter = _build_filter(
                brand=brand_value,
                model=model_value,
                fuel=fuel_value
            )

        elif route == "brand_only":
            primary_filter = _build_filter(
                brand=brand_value,
                model=model_value
            )

        else:
            primary_filter = _build_filter(
                brand=None,
                model=None,
                fuel=fuel_value
            )

        debug["applied_filters"].append(
            f"primary(brand={brand_value}, model={model_value}, fuel={fuel_value})"
        )
        debug["search_stage"] = "primary"

        try:
            hits = _search_vectors(primary_filter)
        except Exception as e:
            print(f"[SEARCH][WARN] qdrant unavailable: {e}", flush=True)
            return []

        if not hits and model_value:
            debug["fallback_triggered"] = True
            debug["model_fallback_triggered"] = True
            debug["search_stage"] = "fallback_without_model"

            fallback_filter = _build_filter(
                brand=brand_value,
                model=None,
                fuel=fuel_value,
            )
            debug["applied_filters"].append(
                f"fallback_without_model(brand={brand_value}, fuel={fuel_value})"
            )

            try:
                hits = _search_vectors(fallback_filter)
            except Exception as e:
                print(f"[SEARCH][WARN] qdrant fallback without model unavailable: {e}", flush=True)
                return []

        if not hits and fuel_value:
            debug["fallback_triggered"] = True
            debug["fuel_fallback_triggered"] = True
            debug["search_stage"] = "fallback_without_fuel"

            fallback_model = None if debug["model_fallback_triggered"] else model_value

            fallback_filter = _build_filter(
                brand=brand_value,
                model=fallback_model,
                fuel=None
            )
            debug["applied_filters"].append(
                f"fallback_without_fuel(brand={brand_value}, model={fallback_model})"
            )

            try:
                hits = _search_vectors(fallback_filter)
            except Exception as e:
                print(f"[SEARCH][WARN] qdrant fuel fallback unavailable: {e}", flush=True)
                return []

        if not hits and strict_brand and allow_brandless_debug_fallback:
            debug["fallback_triggered"] = True
            debug["brandless_debug_fallback_triggered"] = True
            debug["search_stage"] = "brandless_debug_fallback"

            brandless_filter = _build_filter(
                brand=None,
                model=None,
                fuel=fuel_value,
            )
            debug["applied_filters"].append(
                f"brandless_debug_fallback(fuel={fuel_value})"
            )

            try:
                hits = _search_vectors(brandless_filter)
            except Exception as e:
                print(f"[SEARCH][WARN] qdrant brandless debug fallback unavailable: {e}", flush=True)
                return []

        if not hits and not strict_brand and brand_value:
            debug["fallback_triggered"] = True
            debug["search_stage"] = "fuel_only_fallback"

            fuel_only_filter = _build_filter(
                brand=None,
                model=None,
                fuel=fuel_value,
            )
            debug["applied_filters"].append(
                f"fuel_only_fallback(fuel={fuel_value})"
            )

            try:
                hits = _search_vectors(fuel_only_filter)
            except Exception as e:
                print(f"[SEARCH][WARN] qdrant fuel-only fallback unavailable: {e}", flush=True)
                return []

        if not hits:

            print("[SEARCH] fallback triggered")

            try:

                hits = self.store.search(
                    vector=query_vector,
                    limit=top_k,
                    query_filter=None
                )

                debug["fallback_triggered"] = True
                debug["search_stage"] = "vector_fallback"

            except Exception as e:

                print(f"[SEARCH][ERROR] vector fallback failed: {e}")

                print(f"[DEBUG] filters={debug['applied_filters']}", flush=True)
                print(f"[DEBUG] stats={debug}", flush=True)

                return []

        seen_point_ids = set()
        unique_hits = []

        for hit in hits:

            pid = getattr(hit, "id", None)

            if pid and pid in seen_point_ids:
                continue

            seen_point_ids.add(pid)
            unique_hits.append(hit)

        hits = unique_hits

        doc_scores: Dict[str, float] = {}
        doc_payloads: Dict[str, Dict[str, Any]] = {}

        for hit in hits:
            payload = hit.payload or {}

            if payload.get("source") == "telegram":
                if payload.get("price") is None:
                    continue

            url = payload.get("source_url")
            if not url:
                continue

            if "avito.ru/all/avtomobili" in url:
                continue

            score = float(getattr(hit, "score", 0.0) or 0.0)

            if url not in doc_scores or score > doc_scores[url]:
                doc_scores[url] = max(score, doc_scores.get(url, 0))
                doc_payloads[url] = payload

        scored_results: List[Tuple[float, Dict[str, Any], List[str]]] = []

        for url, payload in doc_payloads.items():
            semantic = float(doc_scores.get(url, 0.0) or 0.0)

            if route in {"structured", "brand_only"} and brand_value:
                payload_brand = payload.get("brand")
                if payload_brand and payload_brand != brand_value:
                    continue

            if is_prod and (payload.get("source") == "dev_seed"):
                continue

            if structured.fuel is not None:

                payload_fuel = payload.get("fuel")

                if payload_fuel is None:
                    continue

                if str(payload_fuel).lower() != str(structured.fuel).lower():
                    continue

            if structured.price_max is not None:

                price_val = payload.get("price")

                if price_val is None:

                    debug["skipped_by_price_null"] += 1

                else:

                    try:
                        if price_val is not None and price_val > structured.price_max:
                            debug["skipped_by_price"] += 1
                            continue
                    except Exception:
                        debug["skipped_by_price"] += 1
                        continue

            if structured.mileage_max is not None:

                mileage_val = payload.get("mileage")

                if mileage_val is None:
                    debug["skipped_by_mileage_null"] += 1
                    continue

                try:

                    if mileage_val > structured.mileage_max:
                        debug["skipped_by_mileage"] += 1
                        continue

                except Exception:
                    debug["skipped_by_mileage"] += 1
                    continue

            if structured.year_min is not None:
                year_val = payload.get("year")
                if year_val is None:
                    debug["skipped_by_year_null"] += 1
                else:
                    try:
                        if year_val < structured.year_min:
                            debug["skipped_by_year"] += 1
                            continue
                    except Exception:
                        debug["skipped_by_year"] += 1
                        continue

            recency = self._recency_score(payload)
            sale_bonus = self._sale_bonus(payload)
            completeness = self._completeness_score(payload)
            price_score = self._price_score(payload, structured)

            mileage_score = 0.0

            if structured.mileage_max and payload.get("mileage"):

                if payload.get("mileage") is None:
                    final_score -= 0.05

                try:
                    m = payload["mileage"]
                    mileage_score = max(0.0, 1.0 - (m / structured.mileage_max))
                except:
                    pass

            text_score = self._text_score(payload, structured)

            if brand_value:
                final_score = (
                    semantic * 0.65
                    + text_score * 0.20
                    + recency * 0.15
                    + sale_bonus * 0.10
                    + completeness * 0.20
                )
            else:
                final_score = (
                    semantic * 0.60
                    + text_score * 0.20
                    + recency * 0.15
                    + sale_bonus * 0.05
                    + completeness * 0.15
                )

            if price_score > 0:
                final_score += price_score * 0.15

            final_score += mileage_score * 0.2

            brand_boost = 0.0

            payload_brand = payload.get("brand")

            if brand_value and payload_brand:

                if payload_brand == brand_value:
                    brand_boost = 0.25

                elif payload_brand.startswith(brand_value):
                    brand_boost = 0.15

            final_score = final_score + brand_boost

            if structured.fuel and payload.get("fuel"):

                if str(payload.get("fuel")).lower() != str(structured.fuel).lower():

                    final_score -= 0.6

            model_boost = 0.0

            payload_model = payload.get("model")

            if structured.model and payload_model:

                if payload_model == structured.model:
                    model_boost = 0.35

                elif structured.model in payload_model:
                    model_boost = 0.20

            final_score = final_score + model_boost

            vector_type = payload.get("vector_type")
            vector_boost = 0.0

            if vector_type == "title_boost":
                vector_boost = 0.12

            elif vector_type == "title":
                vector_boost = 0.09

            elif vector_type == "structured":
                vector_boost = 0.06

            final_score = final_score + vector_boost

            if payload.get("price") is None:
                final_score -= 0.10

            if payload.get("brand") and payload.get("model"):
                final_score += 0.12

            reasons = [
                f"semantic={round(semantic, 4)}",
                f"text={round(text_score,4)}",
                f"recency={round(recency, 4)}",
                f"sale={round(sale_bonus, 4)}",
                f"complete={round(completeness, 4)}",
                f"price_score={round(price_score, 4)}",
                f"mileage_score={round(mileage_score, 4)}",
                f"brand_boost={round(brand_boost, 4)}",
                f"model_boost={round(model_boost,4)}",
                f"vector_boost={round(vector_boost,4)}",
                f"final={round(final_score, 4)}",
            ]

            scored_results.append((final_score, payload, reasons))

        scored_results.sort(key=lambda x: x[0], reverse=True)

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

        try:
            session = SessionLocal()
            history = SearchHistory(
                raw_query=structured.raw_query,
                structured_query=structured.model_dump(),
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

        try:

            query_text = structured.raw_query

            results = self._rerank_results(
                query=query_text,
                results=results,
                top_k=min(len(results), 20),
            )

        except Exception as e:

            print(f"[RERANK][WARN] skipped: {e}", flush=True)

        try:
            from redis import Redis
            redis = Redis(
                host="redis",
                port=6379,
                socket_timeout=1,
                socket_connect_timeout=1
            )
            redis.set(
                cache_key,
                json.dumps(results, ensure_ascii=False),
                ex=60
            )
        except:
            pass

        return results

    def _text_score(self, payload: Dict[str, Any], structured: StructuredQuery) -> float:
        text_parts = []

        for key in ("brand", "model", "title", "title_text", "content"):
            value = payload.get(key)
            if value:
                if key == "content":
                    text_parts.append(str(value)[:600])
                else:
                    text_parts.append(str(value))

        text = " ".join(text_parts).lower()
        score = 0.0

        if structured.brand and structured.brand.lower() in text:
            score += 1.5

        if structured.model and structured.model.lower() in text:
            score += 1.5

        if structured.fuel and payload.get("fuel"):

            if structured.fuel.lower() == str(payload.get("fuel")).lower():
                score += 1.2
            else:
                score -= 0.5

        for kw in getattr(structured, "keywords", []) or []:
            if kw and kw.lower() in text:
                score += 0.25

        return min(score / 3.5, 1.0)

    def _rerank_results(
        self,
        query: str,
        results: List[Dict[str, Any]],
        top_k: int = 20,
    ) -> List[Dict[str, Any]]:

        if not results:
            return results

        reranker = get_reranker()

        pairs = []

        for r in results:
            text = ""

            if r.get("brand"):
                text += str(r["brand"]) + " "

            if r.get("model"):
                text += str(r["model"]) + " "

            if r.get("year"):
                text += str(r["year"]) + " "

            if r.get("fuel"):
                text += str(r["fuel"]) + " "

            if r.get("mileage"):
                text += f"{r['mileage']} km "

            pairs.append((query, text.strip()))

        max_rerank = min(len(results), 80)
        pairs = pairs[:max_rerank]
        results = results[:max_rerank]

        try:
            scores = reranker.predict(pairs)
        except Exception as e:
            print(f"[RERANK][WARN] failed: {e}", flush=True)
            return results

        for i, score in enumerate(scores):
            results[i]["rerank_score"] = float(score)

        results.sort(
            key=lambda x: x.get("rerank_score", 0),
            reverse=True,
        )

        return results[:min(top_k, 30)]

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

    def _build_query_text(self, structured: StructuredQuery) -> str:

        parts: List[str] = []

        if structured.brand:
            parts.append(structured.brand)
            parts.append(f"{structured.brand} car")

        if structured.model:
            parts.append(structured.model)
            parts.append(f"{structured.brand} {structured.model}")

        if structured.fuel:
            parts.append(structured.fuel)

        if structured.price_max:
            parts.append(f"price under {structured.price_max}")

        if structured.mileage_max:
            parts.append(f"mileage under {structured.mileage_max}")

        if structured.year_min:
            parts.append(f"year after {structured.year_min}")

        if structured.keywords:
            parts.extend(structured.keywords)

        return " ".join(parts).strip()

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


try:
    print("[SEARCH] warming up reranker...", flush=True)
    get_reranker()
except Exception as e:
    print(f"[SEARCH][WARN] reranker preload failed: {e}", flush=True)