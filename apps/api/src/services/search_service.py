from typing import List, Dict, Any, Tuple, Optional
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse
from collections import Counter
import yaml
import os
import json
import re

try:
    from qdrant_client.models import Filter, FieldCondition, MatchValue
except ImportError:
    from qdrant_client.models import Filter, FieldCondition, MatchValue

from shared.embeddings.provider import embed_text
from sentence_transformers import CrossEncoder

from integrations.vector_db.qdrant import QdrantStore
from domain.query_schema import StructuredQuery
from services.query_router import route_query
from services.car_intent_classifier import detect_car_intent
from services.query_expander import expand_query

from db.session import SessionLocal
from db.models import SearchHistory


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


def load_brands() -> dict:
    try:
        base_dir = Path(_file_).resolve().parent.parent
        brands_path = base_dir / "config" / "brands.yaml"

        with open(brands_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            return data.get("brands", {})

    except Exception as e:
        print(f"[SEARCH][WARN] failed to load brands.yaml: {e}", flush=True)
        return {}


def _normalize_token_text(value: str) -> str:
    value = (value or "").strip().lower()
    value = value.replace("\u00A0", " ").replace("\xa0", " ")
    value = re.sub(r"[-_/]+", " ", value)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _model_soft_match(payload_model: str, query_model: str) -> bool:
    pm = _normalize_token_text(payload_model)
    qm = _normalize_token_text(query_model)

    if not pm or not qm:
        return False

    if pm == qm:
        return True

    pm_tokens = pm.split()
    qm_tokens = qm.split()

    if pm_tokens == qm_tokens:
        return True

    if len(qm_tokens) == 1:
        token = qm_tokens[0]
        if token in pm_tokens:
            return True

        compact_pm = "".join(pm_tokens)
        compact_qm = "".join(qm_tokens)
        if compact_pm == compact_qm:
            return True

        return False

    compact_pm = "".join(pm_tokens)
    compact_qm = "".join(qm_tokens)

    if compact_pm == compact_qm:
        return True

    if all(t in pm_tokens for t in qm_tokens):
        return True

    return False


BRANDS_WHITELIST = load_brands()
WHITELIST_SET = set(BRANDS_WHITELIST.keys())

MAX_RESULTS_PER_SOURCE: int = 20
RECENCY_MAX_DAYS = 180

print("[SEARCH] warming reranker", flush=True)
try:
    get_reranker()
except Exception:
    pass


class SearchService:
    def _init_(self):
        self.store = QdrantStore()

    def _env_int(self, name: str, default: int) -> int:
        try:
            return int(os.getenv(name, str(default)))
        except Exception:
            return default

    def _env_float(self, name: str, default: float) -> float:
        try:
            return float(os.getenv(name, str(default)))
        except Exception:
            return default

    def _canonicalize_source_url(self, url: str) -> str:
        if not url:
            return ""

        try:
            parsed = urlparse(url.strip())
            scheme = (parsed.scheme or "https").lower()
            netloc = (parsed.netloc or "").lower()
            path = parsed.path.rstrip("/") or "/"

            return urlunparse((scheme, netloc, path, "", "", ""))
        except Exception:
            return (url or "").strip().lower()

    def _build_listing_fingerprint(self, payload: Dict[str, Any]) -> str:
        parsed = urlparse(payload.get("source_url") or "")
        domain = (parsed.netloc or "").lower()

        brand = _normalize_token_text(str(payload.get("brand") or ""))
        model = _normalize_token_text(str(payload.get("model") or ""))
        year = str(payload.get("year") or "")
        price = str(payload.get("price") or "")
        mileage = str(payload.get("mileage") or "")

        return "|".join([domain, brand, model, year, price, mileage])

    def _source_quality_score(self, payload: Dict[str, Any]) -> float:
        source = str(payload.get("source") or "unknown").strip().lower()

        priors = {
            "avito": self._env_float("SEARCH_SOURCE_PRIOR_AVITO", 0.90),
            "auto_ru": self._env_float("SEARCH_SOURCE_PRIOR_AUTO_RU", 0.95),
            "drom.ru": self._env_float("SEARCH_SOURCE_PRIOR_DROM", 0.90),
            "drom": self._env_float("SEARCH_SOURCE_PRIOR_DROM", 0.90),
            "telegram": self._env_float("SEARCH_SOURCE_PRIOR_TELEGRAM", 0.60),
            "unknown": self._env_float("SEARCH_SOURCE_PRIOR_UNKNOWN", 0.50),
        }

        return max(0.0, min(1.0, priors.get(source, priors["unknown"])))

    def _build_query_text(self, structured: StructuredQuery) -> str:
        parts: List[str] = []
        seen = set()

        def add(part: Optional[str]):
            p = _normalize_token_text(part or "")
            if not p or p in seen:
                return
            seen.add(p)
            parts.append(p)

        if structured.brand and structured.model:
            add(f"{structured.brand} {structured.model}")
        else:
            add(structured.brand)
            add(structured.model)

        add(structured.fuel)

        if structured.year_min:
            add(f"year from {structured.year_min}")

        if structured.price_max:
            add(f"price under {structured.price_max}")

        if structured.mileage_max:
            add(f"mileage under {structured.mileage_max}")

        for kw in getattr(structured, "keywords", []) or []:
            add(str(kw))

        if not parts:
            add(structured.raw_query or "")

        return " ".join(parts).strip()

    def _build_filter(
        self,
        brand: str = None,
        model: str = None,
        fuel: str = None,
    ) -> Optional[Filter]:
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

        return Filter(must=must_conditions)

    def _passes_hard_filters(
        self,
        payload: Dict[str, Any],
        structured: StructuredQuery,
        route: str,
    ) -> Tuple[bool, List[str]]:
        reasons: List[str] = []

        brand_value = _normalize_token_text(structured.brand or "")
        fuel_value = _normalize_token_text(structured.fuel or "")
        model_value = _normalize_token_text(structured.model or "")

        payload_brand = _normalize_token_text(str(payload.get("brand") or ""))
        payload_fuel = _normalize_token_text(str(payload.get("fuel") or ""))
        payload_model = _normalize_token_text(str(payload.get("model") or ""))

        if route in {"structured", "brand_only"} and brand_value:
            if payload_brand and payload_brand != brand_value:
                reasons.append("brand_mismatch")

        if fuel_value:
            if payload_fuel and payload_fuel != fuel_value:
                reasons.append("fuel_mismatch")

        if structured.price_max is not None:
            price_val = payload.get("price")
            if price_val is not None:
                try:
                    if float(price_val) > float(structured.price_max):
                        reasons.append("price_overflow")
                except Exception:
                    reasons.append("price_invalid")

        if structured.mileage_max is not None:
            mileage_val = payload.get("mileage")
            if mileage_val is not None:
                try:
                    if float(mileage_val) > float(structured.mileage_max):
                        reasons.append("mileage_overflow")
                except Exception:
                    reasons.append("mileage_invalid")

        if structured.year_min is not None:
            year_val = payload.get("year")
            if year_val is not None:
                try:
                    if int(year_val) < int(structured.year_min):
                        reasons.append("year_too_old")
                except Exception:
                    reasons.append("year_invalid")

        if model_value and payload_model:
            if not _model_soft_match(payload_model, model_value):
                reasons.append("model_mismatch")

        return len(reasons) == 0, reasons

    def _compute_soft_signals(
        self,
        payload: Dict[str, Any],
        structured: StructuredQuery,
        semantic_score: float,
        route: str,
    ) -> Dict[str, float]:
        signals: Dict[str, float] = {}

        brand_value = _normalize_token_text(structured.brand or "")
        fuel_value = _normalize_token_text(structured.fuel or "")
        model_value = _normalize_token_text(structured.model or "")

        payload_brand = _normalize_token_text(str(payload.get("brand") or ""))
        payload_fuel = _normalize_token_text(str(payload.get("fuel") or ""))
        payload_model = _normalize_token_text(str(payload.get("model") or ""))

        signals["semantic"] = max(0.0, min(1.0, float(semantic_score or 0.0)))
        signals["text_match"] = self._text_score(payload, structured)
        signals["freshness"] = self._recency_score(payload)
        signals["completeness"] = self._completeness_score(payload)
        signals["price_fit"] = self._price_score(payload, structured)
        signals["mileage_fit"] = self._mileage_score(payload, structured)
        signals["sale_intent"] = self._sale_bonus(payload)
        signals["source_quality"] = self._source_quality_score(payload)

        if brand_value:
            signals["brand_match"] = 1.0 if payload_brand == brand_value else 0.0
        else:
            signals["brand_match"] = 0.5

        if model_value:
            signals["model_match"] = 1.0 if _model_soft_match(payload_model, model_value) else 0.0
        else:
            signals["model_match"] = 0.5

        if fuel_value:
            if not payload_fuel:
                signals["fuel_match"] = 0.35
            else:
                signals["fuel_match"] = 1.0 if payload_fuel == fuel_value else 0.0
        else:
            signals["fuel_match"] = 0.5

        vector_type = str(payload.get("vector_type") or "").strip().lower()
        if vector_type == "title_boost":
            signals["representation_quality"] = 1.0
        elif vector_type == "title":
            signals["representation_quality"] = 0.85
        elif vector_type == "structured":
            signals["representation_quality"] = 0.70
        else:
            signals["representation_quality"] = 0.50

        return signals

    def _score_candidate(
        self,
        payload: Dict[str, Any],
        structured: StructuredQuery,
        semantic_score: float,
        route: str,
    ) -> Tuple[float, Dict[str, float]]:
        signals = self._compute_soft_signals(payload, structured, semantic_score, route)

        weights = {
            "semantic": self._env_float("SEARCH_W_SEMANTIC", 0.35),
            "text_match": self._env_float("SEARCH_W_TEXT", 0.15),
            "freshness": self._env_float("SEARCH_W_FRESHNESS", 0.10),
            "completeness": self._env_float("SEARCH_W_COMPLETENESS", 0.08),
            "price_fit": self._env_float("SEARCH_W_PRICE", 0.08),
            "mileage_fit": self._env_float("SEARCH_W_MILEAGE", 0.06),
            "fuel_match": self._env_float("SEARCH_W_FUEL", 0.06),
            "brand_match": self._env_float("SEARCH_W_BRAND", 0.15),
            "model_match": self._env_float("SEARCH_W_MODEL", 0.12),
            "source_quality": self._env_float("SEARCH_W_SOURCE", 0.03),
            "sale_intent": self._env_float("SEARCH_W_SALE", 0.02),
            "representation_quality": self._env_float("SEARCH_W_REPRESENTATION", 0.02),
        }

        final_score = 0.0
        for key, weight in weights.items():
            final_score += signals.get(key, 0.0) * weight

        return final_score, signals

    def _rerank_results(
        self,
        query: str,
        results: List[Dict[str, Any]],
        top_k: int = 20,
    ) -> List[Dict[str, Any]]:
        if not results:
            return results

        reranker = get_reranker()
        max_rerank = min(len(results), self._env_int("SEARCH_RERANK_MAX_CANDIDATES", 50))
        blend = self._env_float("SEARCH_RERANK_BLEND", 0.25)

        rerank_slice = results[:max_rerank]
        tail_slice = results[max_rerank:]

        pairs = []
        for r in rerank_slice:
            text = ""
            if r.get("brand"):
                text += f"{r['brand']} "
            if r.get("model"):
                text += f"{r['model']} "
            if r.get("year"):
                text += f"{r['year']} "
            if r.get("fuel"):
                text += f"{r['fuel']} "
            if r.get("mileage") is not None:
                text += f"{r['mileage']} km "
            if r.get("price") is not None:
                text += f"{r['price']} rub "
            pairs.append((query, text.strip()[:300]))

        try:
            rerank_scores = [float(x) for x in reranker.predict(pairs)]
        except Exception as e:
            print(f"[RERANK][WARN] failed: {e}", flush=True)
            return results[:top_k]

        if rerank_scores:
            min_score = min(rerank_scores)
            max_score = max(rerank_scores)
        else:
            min_score = 0.0
            max_score = 0.0

        denom = max_score - min_score

        for idx, row in enumerate(rerank_slice):
            original_score = float(row.get("score", 0.0) or 0.0)
            raw_rerank = rerank_scores[idx]

            if denom == 0:
                rerank_norm = 0.5
            else:
                rerank_norm = (raw_rerank - min_score) / denom

            final_blended_score = (original_score * (1.0 - blend)) + (rerank_norm * blend)

            row["original_score"] = original_score
            row["rerank_score"] = raw_rerank
            row["rerank_score_norm"] = round(rerank_norm, 6)
            row["final_blended_score"] = round(final_blended_score, 6)
            row["score"] = round(final_blended_score, 6)

        rerank_slice.sort(key=lambda x: x.get("final_blended_score", x.get("score", 0.0)), reverse=True)

        merged = rerank_slice + tail_slice
        return merged[:top_k]

    def search(
        self,
        structured: StructuredQuery,
        limit: int = None,
        top_k: int = None,
    ) -> List[Dict[str, Any]]:
        _ = detect_car_intent(structured.raw_query)

        if limit is None:
            limit = self._env_int("SEARCH_LIMIT", 50)

        if top_k is None:
            top_k = self._env_int("SEARCH_TOP_K", 120)

        min_candidates = self._env_int("SEARCH_MIN_CANDIDATES", 60)

        brand_conf = float(getattr(structured, "brand_confidence", 0.0) or 0.0)
        brand_value = _normalize_token_text(structured.brand or "")
        fuel_value = _normalize_token_text(structured.fuel or "")
        model_value = _normalize_token_text(structured.model or "")
        route = route_query(structured)

        cache_key = (
            f"search:{structured.raw_query}:"
            f"{brand_value}:{model_value}:{structured.price_max}:"
            f"{structured.mileage_max}:{fuel_value}:{structured.year_min}"
        )

        debug: Dict[str, Any] = {
            "query": {
                "raw_query": structured.raw_query,
                "brand": structured.brand,
                "model": structured.model,
                "fuel": structured.fuel,
                "price_max": structured.price_max,
                "mileage_max": structured.mileage_max,
                "year_min": structured.year_min,
            },
            "route": route,
            "retrieval_stages": [],
            "filtering": {
                "checked_candidates": 0,
                "discarded_candidates": 0,
                "discard_reasons_counter": {},
            },
            "scoring": {
                "semantic_avg": 0.0,
                "freshness_avg": 0.0,
                "completeness_avg": 0.0,
                "price_fit_avg": 0.0,
                "mileage_fit_avg": 0.0,
                "top_score_breakdowns": [],
            },
            "dedup": {
                "skipped_by_point_id_duplicate": 0,
                "skipped_by_canonical_url_duplicate": 0,
                "skipped_by_fingerprint_duplicate": 0,
            },
            "final": {
                "results_count": 0,
                "rerank_applied": False,
                "cache_hit": False,
                "cache_written": False,
            },
        }

        try:
            from redis import Redis
            redis = Redis(
                host="redis",
                port=6379,
                socket_timeout=1,
                socket_connect_timeout=1,
            )
            cached = redis.get(cache_key)
            if cached:
                debug["final"]["cache_hit"] = True
                return json.loads(cached)
        except Exception:
            pass

        query_text = self._build_query_text(structured)
        query_vector = embed_text(query_text)

        expanded_queries = expand_query(structured.raw_query or "")
        vectors = [query_vector]

        for q in expanded_queries:
            try:
                vec = embed_text(q)
                vectors.append(vec)
            except Exception:
                pass

        strict_brand = bool(brand_value and brand_conf >= 0.9)

        stages: List[Dict[str, Any]] = []

        primary_brand = brand_value if route in {"structured", "brand_only"} else None
        primary_model = model_value if route == "structured" else None
        primary_fuel = fuel_value if route == "structured" else fuel_value if route not in {"structured", "brand_only"} else None

        stages.append(
            {
                "stage_name": "strict_primary",
                "enabled": True,
                "filter": self._build_filter(
                    brand=primary_brand,
                    model=primary_model,
                    fuel=primary_fuel,
                ),
                "filter_summary": f"brand={primary_brand},model={primary_model},fuel={primary_fuel}",
            }
        )

        stages.append(
            {
                "stage_name": "no_model_fallback",
                "enabled": bool(model_value),
                "filter": self._build_filter(
                    brand=primary_brand,
                    model=None,
                    fuel=primary_fuel,
                ),
                "filter_summary": f"brand={primary_brand},model=None,fuel={primary_fuel}",
            }
        )

        stages.append(
            {
                "stage_name": "no_fuel_fallback",
                "enabled": bool(fuel_value),
                "filter": self._build_filter(
                    brand=primary_brand,
                    model=primary_model if route == "structured" else None,
                    fuel=None,
                ),
                "filter_summary": f"brand={primary_brand},model={primary_model if route == 'structured' else None},fuel=None",
            }
        )

        stages.append(
            {
                "stage_name": "weak_brand_fallback",
                "enabled": bool(not strict_brand),
                "filter": self._build_filter(
                    brand=None,
                    model=primary_model if route == "structured" and model_value else None,
                    fuel=fuel_value if fuel_value else None,
                ),
                "filter_summary": f"brand=None,model={primary_model if route == 'structured' and model_value else None},fuel={fuel_value if fuel_value else None}",
            }
        )

        stages.append(
            {
                "stage_name": "global_vector_fallback",
                "enabled": True,
                "filter": None,
                "filter_summary": "brand=None,model=None,fuel=None",
            }
        )

        all_hits = []
        seen_point_ids = set()

        def _search_stage(stage_filter: Optional[Filter]) -> List[Any]:
            stage_hits = []
            for vec in vectors:
                try:
                    sub_hits = self.store.search(
                        vector=vec,
                        limit=top_k,
                        query_filter=stage_filter,
                    )
                    stage_hits.extend(sub_hits)
                except Exception:
                    pass
            return stage_hits

        try:
            for stage in stages:
                if not stage["enabled"]:
                    continue

                raw_hits = _search_stage(stage["filter"])
                unique_added = 0

                for hit in raw_hits:
                    pid = getattr(hit, "id", None)
                    if pid is not None and pid in seen_point_ids:
                        debug["dedup"]["skipped_by_point_id_duplicate"] += 1
                        continue

                    if pid is not None:
                        seen_point_ids.add(pid)

                    all_hits.append(hit)
                    unique_added += 1

                debug["retrieval_stages"].append(
                    {
                        "stage_name": stage["stage_name"],
                        "filter_summary": stage["filter_summary"],
                        "raw_hits_count": len(raw_hits),
                        "unique_hits_added": unique_added,
                    }
                )

                if len(all_hits) >= min_candidates:
                    break

        except Exception as e:
            print(f"[SEARCH][WARN] qdrant unavailable: {e}", flush=True)
            return []

        doc_scores: Dict[str, float] = {}
        doc_payloads: Dict[str, Dict[str, Any]] = {}

        for hit in all_hits:
            payload = hit.payload or {}

            if payload.get("source") == "telegram" and payload.get("price") is None:
                continue

            source_url = payload.get("source_url")
            canonical_url = self._canonicalize_source_url(source_url) if source_url else ""
            fingerprint = self._build_listing_fingerprint(payload)

            doc_key = canonical_url or fingerprint
            if not doc_key:
                continue

            if "avito.ru/all/avtomobili" in (source_url or ""):
                continue

            score = float(getattr(hit, "score", 0.0) or 0.0)
            if doc_key not in doc_scores or score > doc_scores[doc_key]:
                doc_scores[doc_key] = score
                doc_payloads[doc_key] = payload

        scored_results: List[Tuple[float, Dict[str, Any], Dict[str, float], List[str]]] = []
        discard_counter = Counter()
        scoring_snapshots = []

        semantic_values = []
        freshness_values = []
        completeness_values = []
        price_fit_values = []
        mileage_fit_values = []

        for _, payload in doc_payloads.items():
            debug["filtering"]["checked_candidates"] += 1

            env_name = (os.getenv("ENV", "") or os.getenv("APP_ENV", "") or "dev").lower()
            is_prod = env_name == "prod"

            if is_prod and payload.get("source") == "dev_seed":
                discard_counter["dev_seed_prod"] += 1
                debug["filtering"]["discarded_candidates"] += 1
                continue

            source_url = payload.get("source_url")
            canonical_url = self._canonicalize_source_url(source_url) if source_url else ""
            semantic = float(doc_scores.get(canonical_url or self._build_listing_fingerprint(payload), 0.0) or 0.0)

            passed, discard_reasons = self._passes_hard_filters(payload, structured, route)
            if not passed:
                debug["filtering"]["discarded_candidates"] += 1
                for reason in discard_reasons:
                    discard_counter[reason] += 1
                continue

            final_score, signals = self._score_candidate(payload, structured, semantic, route)

            semantic_values.append(signals.get("semantic", 0.0))
            freshness_values.append(signals.get("freshness", 0.0))
            completeness_values.append(signals.get("completeness", 0.0))
            price_fit_values.append(signals.get("price_fit", 0.0))
            mileage_fit_values.append(signals.get("mileage_fit", 0.0))

            reasons_list = [
                f"semantic={round(signals.get('semantic', 0.0), 4)}",
                f"text_match={round(signals.get('text_match', 0.0), 4)}",
                f"freshness={round(signals.get('freshness', 0.0), 4)}",
                f"completeness={round(signals.get('completeness', 0.0), 4)}",
                f"price_fit={round(signals.get('price_fit', 0.0), 4)}",
                f"mileage_fit={round(signals.get('mileage_fit', 0.0), 4)}",
                f"fuel_match={round(signals.get('fuel_match', 0.0), 4)}",
                f"brand_match={round(signals.get('brand_match', 0.0), 4)}",
                f"model_match={round(signals.get('model_match', 0.0), 4)}",
                f"source_quality={round(signals.get('source_quality', 0.0), 4)}",
                f"sale_intent={round(signals.get('sale_intent', 0.0), 4)}",
                f"representation_quality={round(signals.get('representation_quality', 0.0), 4)}",
                f"final={round(final_score, 6)}",
            ]

            scored_results.append((final_score, payload, signals, reasons_list))

        debug["filtering"]["discard_reasons_counter"] = dict(discard_counter)

        def _avg(values: List[float]) -> float:
            if not values:
                return 0.0
            return round(sum(values) / len(values), 6)

        debug["scoring"]["semantic_avg"] = _avg(semantic_values)
        debug["scoring"]["freshness_avg"] = _avg(freshness_values)
        debug["scoring"]["completeness_avg"] = _avg(completeness_values)
        debug["scoring"]["price_fit_avg"] = _avg(price_fit_values)
        debug["scoring"]["mileage_fit_avg"] = _avg(mileage_fit_values)

        scored_results.sort(key=lambda x: x[0], reverse=True)

        results: List[Dict[str, Any]] = []
        seen_canonical_urls = set()
        seen_fingerprints = set()
        source_counter: Dict[str, int] = {}

        for final_score, payload, signals, reasons in scored_results:
            source_url = payload.get("source_url")
            if not source_url:
                continue

            canonical_url = self._canonicalize_source_url(source_url)
            fingerprint = self._build_listing_fingerprint(payload)

            if canonical_url and canonical_url in seen_canonical_urls:
                debug["dedup"]["skipped_by_canonical_url_duplicate"] += 1
                continue

            if fingerprint and fingerprint in seen_fingerprints:
                debug["dedup"]["skipped_by_fingerprint_duplicate"] += 1
                continue

            source_name = payload.get("source") or "unknown"
            source_counter.setdefault(source_name, 0)

            if source_counter[source_name] >= MAX_RESULTS_PER_SOURCE:
                continue

            row = {
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
                "score_breakdown": {k: round(v, 6) for k, v in signals.items()},
            }

            results.append(row)

            if len(scoring_snapshots) < 5:
                scoring_snapshots.append(
                    {
                        "url": source_url,
                        "brand": row.get("brand"),
                        "model": row.get("model"),
                        "score": row.get("score"),
                        "score_breakdown": row.get("score_breakdown"),
                    }
                )

            if canonical_url:
                seen_canonical_urls.add(canonical_url)
            if fingerprint:
                seen_fingerprints.add(fingerprint)

            source_counter[source_name] += 1

            if len(results) >= limit:
                break

        debug["scoring"]["top_score_breakdowns"] = scoring_snapshots

        try:
            results = self._rerank_results(
                query=structured.raw_query or query_text,
                results=results,
                top_k=min(len(results), limit),
            )
            debug["final"]["rerank_applied"] = len(results) > 0
        except Exception as e:
            print(f"[RERANK][WARN] skipped: {e}", flush=True)

        debug["final"]["results_count"] = len(results)

        verbose = str(os.getenv("SEARCH_DEBUG_VERBOSE", "0")).strip().lower() in {"1", "true", "yes", "on"}
        print(
            f"[SEARCH] query='{structured.raw_query}' route={route} stages={len(debug['retrieval_stages'])} results={len(results)}",
            flush=True,
        )
        if verbose:
            print(f"[SEARCH][DEBUG] {json.dumps(debug, ensure_ascii=False, default=str)}", flush=True)

        try:
            session = SessionLocal()
            history = SearchHistory(
                raw_query=structured.raw_query,
                structured_query=structured.model_dump() if hasattr(structured, "model_dump") else {},
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
            from redis import Redis
            redis = Redis(
                host="redis",
                port=6379,
                socket_timeout=1,
                socket_connect_timeout=1,
            )
            redis.set(
                cache_key,
                json.dumps(results, ensure_ascii=False),
                ex=60,
            )
            debug["final"]["cache_written"] = True
        except Exception:
            pass

        return results

    def _text_score(self, payload: Dict[str, Any], structured: StructuredQuery) -> float:
        text_parts: List[str] = []

        for key in ("brand", "model", "title", "title_text", "content"):
            value = payload.get(key)
            if value:
                if key == "content":
                    text_parts.append(str(value)[:600])
                else:
                    text_parts.append(str(value))

        text = _normalize_token_text(" ".join(text_parts))
        score = 0.0

        if structured.brand and _normalize_token_text(structured.brand) in text:
            score += 1.2

        if structured.model and _normalize_token_text(structured.model) in text:
            score += 1.2

        if structured.fuel:
            fuel_value = _normalize_token_text(structured.fuel)
            payload_fuel = _normalize_token_text(str(payload.get("fuel") or ""))
            if payload_fuel:
                if payload_fuel == fuel_value:
                    score += 0.8
                else:
                    score -= 0.3

        for kw in getattr(structured, "keywords", []) or []:
            kw_norm = _normalize_token_text(str(kw))
            if kw_norm and kw_norm in text:
                score += 0.15

        return max(0.0, min(score / 3.0, 1.0))

    def _recency_score(self, payload: Dict[str, Any]) -> float:
        ts = payload.get("created_at_ts")
        if not ts:
            return 0.0
        try:
            now_ts = int(datetime.now(tz=timezone.utc).timestamp())
            age_days = (now_ts - int(ts)) / 86400
            return max(0.0, min(1.0, 1.0 - age_days / float(RECENCY_MAX_DAYS)))
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
            return 0.15

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

            ratio = price_val / denom
            return max(0.0, min(1.0, 1.0 - ratio))

        denom = 5_000_000.0
        return max(0.0, min(1.0, 1.0 - (price_val / denom)))

    def _mileage_score(self, payload: Dict[str, Any], structured: StructuredQuery) -> float:
        mileage = payload.get("mileage")
        if mileage is None:
            return 0.15 if structured.mileage_max is not None else 0.5

        try:
            mileage_val = float(mileage)
        except Exception:
            return 0.0

        if structured.mileage_max is not None:
            try:
                denom = float(structured.mileage_max)
            except Exception:
                denom = 0.0

            if denom <= 0.0:
                return 0.0

            ratio = mileage_val / denom
            return max(0.0, min(1.0, 1.0 - ratio))

        denom = 250_000.0
        return max(0.0, min(1.0, 1.0 - (mileage_val / denom)))


try:
    print("[SEARCH] warming up reranker...", flush=True)
    get_reranker()
except Exception as e:
    print(f"[SEARCH][WARN] reranker preload failed: {e}", flush=True)