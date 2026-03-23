from typing import List, Dict, Any, Tuple, Optional
from pathlib import Path
from datetime import datetime, timezone
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode
from collections import Counter
import yaml
import os
import json
import re

from rank_bm25 import BM25Okapi

try:
    # 🔥 ИМПОРТИРУЕМ Range ДЛЯ ФИЛЬТРАЦИИ ПРОБЕГА, ГОДА И ЦЕНЫ
    from qdrant_client.models import Filter, FieldCondition, MatchValue, Range
except ImportError:
    from qdrant_client.models import Filter, FieldCondition, MatchValue, Range

from shared.embeddings.provider import embed_text
from sentence_transformers import CrossEncoder

from integrations.vector_db.qdrant import QdrantStore
from domain.query_schema import StructuredQuery
from services.query_router import route_query
from services.car_intent_classifier import detect_car_intent
from services.query_expander import expand_query
from services.taxonomy_service import taxonomy_service

from db.session import SessionLocal
from db.models import SearchHistory


def _normalize_model_strict(v: str) -> str:
    v = (v or "").lower()
    v = v.replace("-", "").replace("_", "").replace(" ", "")
    return v


def _bm25_score(query: str, docs: List[str]) -> float:
    if not docs:
        return 0.0

    query_norm = _normalize_token_text(query)
    if not query_norm:
        return 0.0

    prepared_docs =[_normalize_token_text(d) for d in docs if d]
    prepared_docs = [d for d in prepared_docs if d]
    if not prepared_docs:
        return 0.0

    tokenized_docs =[d.split() for d in prepared_docs]
    tokenized_query = query_norm.split()
    if not tokenized_query:
        return 0.0

    try:
        bm25 = BM25Okapi(tokenized_docs)
        raw_scores = bm25.get_scores(tokenized_query)
        if not len(raw_scores):
            return 0.0
        return max(0.0, min(1.0, sum(raw_scores) / (len(raw_scores) * 5.0)))
    except Exception:
        return 0.0


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
        base_dir = Path(__file__).resolve().parent.parent
        brands_path = base_dir / "config" / "brands.yaml"

        with open(brands_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        if isinstance(data, dict) and "brands" in data:
            brands = data.get("brands", {})
            return brands if isinstance(brands, dict) else {}

        return data if isinstance(data, dict) else {}

    except Exception as e:
        print(f"[SEARCH][WARN] failed to load brands.yaml: {e}", flush=True)
        return {}


def _normalize_token_text(value: str) -> str:
    value = (value or "").strip().lower()
    value = value.replace("\u00A0", " ").replace("\xa0", " ")
    value = value.replace("-", " ").replace("_", " ").replace("/", " ")
    value = re.sub(r"([a-zа-я])(\d)", r"\1 \2", value, flags=re.IGNORECASE)
    value = re.sub(r"(\d)([a-zа-я])", r"\1 \2", value, flags=re.IGNORECASE)
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def _compact_token_text(value: str) -> str:
    return _normalize_token_text(value).replace(" ", "")


def _model_soft_match(payload_model: str, query_model: str) -> bool:
    if re.search(r"\d", query_model):
        return _normalize_model_strict(payload_model) == _normalize_model_strict(query_model)

    pm_strict = _normalize_model_strict(payload_model)
    qm_strict = _normalize_model_strict(query_model)

    if pm_strict and qm_strict and pm_strict == qm_strict:
        return True

    pm = _normalize_token_text(payload_model)
    qm = _normalize_token_text(query_model)

    if not pm or not qm:
        return False

    if pm == qm:
        return True

    pm_tokens = pm.split()
    qm_tokens = qm.split()

    pm_compact = "".join(pm_tokens)
    qm_compact = "".join(qm_tokens)

    if pm_compact == qm_compact:
        return True

    if len(qm_tokens) == 1:
        q_single = qm_tokens[0]
        if q_single in pm_tokens:
            return True
        if q_single == pm_compact:
            return True
        return False

    if len(pm_tokens) == 1:
        p_single = pm_tokens[0]
        if p_single in qm_tokens:
            return True
        if p_single == qm_compact:
            return True

    if all(token in pm_tokens for token in qm_tokens):
        return True

    if qm_compact and (
        pm_compact == qm_compact
        or pm_compact.startswith(qm_compact)
        or pm_compact.endswith(qm_compact)
    ):
        return True

    return False


BRANDS_WHITELIST = load_brands()
WHITELIST_SET = set(BRANDS_WHITELIST.keys())

MAX_RESULTS_PER_SOURCE: int = 20
RECENCY_MAX_DAYS = 180

print("[SEARCH] warming reranker", flush=True)
try:
    get_reranker()
except Exception as e:
    print(f"[SEARCH][WARN] reranker warmup failed: {e}", flush=True)


class SearchService:
    def __init__(self):
        self.store = QdrantStore()
        self._last_debug: Dict[str, Any] = {}

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

    def _env_json_dict(self, name: str, default: Dict[str, Any]) -> Dict[str, Any]:
        raw = os.getenv(name)
        if not raw:
            return default
        try:
            parsed = json.loads(raw)
            return parsed if isinstance(parsed, dict) else default
        except Exception:
            return default

    def _canonicalize_source_url(self, url: str) -> str:
        if not url:
            return ""

        try:
            parsed = urlparse(url.strip())
            scheme = (parsed.scheme or "https").lower()
            netloc = (parsed.netloc or "").lower()
            path = (parsed.path or "/").rstrip("/") or "/"

            tracking_prefixes = (
                "utm_",
                "yclid",
                "gclid",
                "fbclid",
                "ref",
                "referrer",
            )

            filtered_query =[]
            for k, v in parse_qsl(parsed.query, keep_blank_values=True):
                key_norm = (k or "").strip().lower()
                if key_norm.startswith(tracking_prefixes):
                    continue
                filtered_query.append((k, v))

            query = urlencode(filtered_query, doseq=True)

            return urlunparse((scheme, netloc, path, "", query, ""))
        except Exception:
            return (url or "").strip().lower()

    def _build_listing_fingerprint(self, payload: Dict[str, Any]) -> str:
        source_url = payload.get("source_url") or ""
        canonical_url = self._canonicalize_source_url(source_url)
        parsed = urlparse(canonical_url)
        domain = (parsed.netloc or "").lower()
        path = (parsed.path or "").lower()

        brand = _normalize_token_text(str(payload.get("brand") or ""))
        model = _normalize_token_text(str(payload.get("model") or ""))
        year = str(payload.get("year") or "")
        price = str(payload.get("price") or "")
        mileage = str(payload.get("mileage") or "")

        return "|".join([domain, path, brand, model, year, price, mileage])

    def _source_quality_score(self, payload: Dict[str, Any]) -> float:
        source = str(payload.get("source") or "unknown").strip().lower()
        source_url = str(payload.get("source_url") or "").strip().lower()
        domain = (urlparse(source_url).netloc or "").lower()

        defaults = {
            "auto_ru": 0.95,
            "avito": 0.90,
            "avito.ru": 0.90,
            "drom": 0.90,
            "drom.ru": 0.90,
            "telegram": 0.60,
            "unknown": 0.50,
        }
        priors = self._env_json_dict("SEARCH_SOURCE_PRIORS", defaults)

        if source in priors:
            value = priors[source]
        elif domain in priors:
            value = priors[domain]
        elif "avito.ru" in domain:
            value = priors.get("avito.ru", priors.get("avito", priors.get("unknown", 0.50)))
        elif "drom.ru" in domain:
            value = priors.get("drom.ru", priors.get("drom", priors.get("unknown", 0.50)))
        else:
            value = priors.get("unknown", 0.50)

        try:
            return max(0.0, min(1.0, float(value)))
        except Exception:
            return 0.50

    def _build_query_text(self, structured: StructuredQuery) -> str:
        parts: List[str] = []
        seen = set()

        def add(part: Optional[str]):
            p = _normalize_token_text(part or "")
            if not p or p in seen:
                return
            seen.add(p)
            parts.append(p)

        if structured.brand:
            add(structured.brand)

        if structured.model:
            add(structured.model)

        if structured.fuel:
            add(structured.fuel)

        if structured.year_min:
            add(f"year from {structured.year_min}")

        if structured.price_max:
            add(f"price under {structured.price_max}")

        if structured.mileage_max:
            add(f"mileage under {structured.mileage_max}")

        for kw in getattr(structured, "keywords", []) or[]:
            add(str(kw))

        if not parts:
            add(structured.raw_query or "")

        return " ".join(parts).strip()

    def _passes_hard_filters(
        self,
        payload: Dict[str, Any],
        structured: StructuredQuery,
        route: str,
    ) -> Tuple[bool, List[str]]:
        reasons: List[str] =[]

        brand_value = _normalize_token_text(structured.brand or "")
        model_value = _normalize_model_strict(structured.model or "")

        payload_brand = _normalize_token_text(str(payload.get("brand") or ""))
        payload_model = str(payload.get("model") or "")

        source_name = str(payload.get("source") or "").strip().lower()

        if source_name == "telegram" and payload.get("price") is None:
            sale_intent = str(payload.get("sale_intent") or "0").strip()
            if sale_intent not in {"1", "true", "True"}:
                reasons.append("telegram_non_sale_no_price")

        if route in {"structured", "brand_only"} and brand_value:
            if payload_brand and payload_brand != brand_value:
                if getattr(structured, "brands", None):
                    if payload_brand not in structured.brands:
                        reasons.append("brand_mismatch")
                else:
                    reasons.append("brand_mismatch")

        if structured.price_max is not None:
            price_val = payload.get("price")
            if price_val is not None:
                try:
                    float(price_val)
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

        if route == "structured" and model_value:
            if not payload_model:
                reasons.append("model_missing")
            elif not _model_soft_match(payload_model, model_value):
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
        model_value = _normalize_model_strict(structured.model or "")

        payload_brand = _normalize_token_text(str(payload.get("brand") or ""))
        payload_fuel = _normalize_token_text(str(payload.get("fuel") or ""))
        payload_model = str(payload.get("model") or "")

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

        if model_value and not payload_model:
            signals["model_match"] = 0.0

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

        docs =[]
        for key in ("title", "title_text", "content"):
            if payload.get(key):
                docs.append(str(payload.get(key)))

        signals["bm25"] = _bm25_score(structured.raw_query or self._build_query_text(structured), docs)

        weights = {
            "semantic": self._env_float("SEARCH_W_SEMANTIC", 0.25),
            "text_match": self._env_float("SEARCH_W_TEXT", 0.20),
            "freshness": self._env_float("SEARCH_W_FRESHNESS", 0.10),
            "completeness": self._env_float("SEARCH_W_COMPLETENESS", 0.08),
            "price_fit": self._env_float("SEARCH_W_PRICE", 0.08),
            "mileage_fit": self._env_float("SEARCH_W_MILEAGE", 0.06),
            "fuel_match": self._env_float("SEARCH_W_FUEL", 0.12),
            "brand_match": self._env_float("SEARCH_W_BRAND", 0.15),
            "model_match": self._env_float("SEARCH_W_MODEL", 0.18),
            "source_quality": self._env_float("SEARCH_W_SOURCE", 0.03),
            "sale_intent": self._env_float("SEARCH_W_SALE", 0.02),
            "representation_quality": self._env_float("SEARCH_W_REPRESENTATION", 0.02),
            "bm25": self._env_float("SEARCH_W_BM25", 0.15),
        }

        final_score = 0.0
        for key, weight in weights.items():
            final_score += signals.get(key, 0.0) * weight

        # 🔥 ЖЕСТКИЕ ШТРАФЫ ДЛЯ FALLBACK РЕЖИМА (ЕСЛИ ФИЛЬТРЫ ПРОПУСТИЛИ МУСОР)
        if structured.fuel:
            p_fuel = payload.get("fuel")
            if not p_fuel:
                final_score *= 0.15  # Уничтожаем скор за отсутствие топлива
            elif p_fuel != structured.fuel:
                final_score *= 0.01  # Полностью гасим несовпадающее топливо

        if structured.mileage_max is not None:
            p_mil = payload.get("mileage")
            if p_mil is None:
                p_year = payload.get("year")
                try:
                    if not (p_year and int(p_year) >= 2024):
                        final_score *= 0.3 # Штрафуем за скрытый пробег у б/у авто
                except:
                    final_score *= 0.3
            elif p_mil > structured.mileage_max:
                final_score *= 0.01

        # Штраф за отсутствующий год, если мы ищем по году
        if structured.year_min is not None or getattr(structured, "year_max", None) is not None:
            if payload.get("year") is None:
                final_score *= 0.1  # Жестко штрафуем

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
        blend = self._env_float("SEARCH_RERANK_BLEND", 0.20)

        rerank_slice = results[:max_rerank]
        tail_slice = results[max_rerank:]

        pairs =[]
        for r in rerank_slice:
            text_parts =[]
            for key in ("brand", "model", "year", "fuel", "mileage", "price"):
                value = r.get(key)
                if value is not None:
                    text_parts.append(str(value))
            pairs.append((query, " ".join(text_parts)[:300]))

        try:
            rerank_scores = [float(x) for x in reranker.predict(pairs)]
        except Exception as e:
            print(f"[RERANK][WARN] failed: {e}", flush=True)
            return results[:top_k]

        min_score = min(rerank_scores) if rerank_scores else 0.0
        max_score = max(rerank_scores) if rerank_scores else 0.0
        denom = max_score - min_score

        for idx, row in enumerate(rerank_slice):
            base_score = float(row.get("score", 0.0) or 0.0)
            raw_rerank = rerank_scores[idx]

            rerank_norm = 0.5 if denom == 0 else (raw_rerank - min_score) / denom
            final_score = (base_score * (1.0 - blend)) + (rerank_norm * blend)

            row["base_score"] = round(base_score, 6)
            row["rerank_score"] = round(raw_rerank, 6)
            row["rerank_score_norm"] = round(rerank_norm, 6)
            row["score"] = round(final_score, 6)

            if "why_match" in row:
                row["why_match"] = re.sub(r"\s*\+\s*final=\d+(?:\.\d+)?", "", row["why_match"])
                row["why_match"] += f" + base_score={row['base_score']} + rerank={round(rerank_norm, 4)} + final={row['score']}"

        rerank_slice.sort(key=lambda x: x.get("score", 0.0), reverse=True)
        merged = rerank_slice + tail_slice
        return merged[:top_k]

    def search(
        self,
        structured: StructuredQuery,
        limit: int = None,
        top_k: int = None,
    ) -> List[Dict[str, Any]]:
        print("[SEARCH SERVICE LIVE CODE] entered", flush=True)
        _ = detect_car_intent(structured.raw_query)

        if limit is None:
            limit = self._env_int("SEARCH_LIMIT", 50)

        if top_k is None:
            top_k = self._env_int("SEARCH_TOP_K", 120)

        min_candidates = self._env_int("SEARCH_MIN_CANDIDATES", 120)

        brand_conf = float(getattr(structured, "brand_confidence", 0.0) or 0.0)

        canonical_brand = taxonomy_service.canonicalize_brand(structured.brand) if structured.brand else None
        canonical_model = (
            taxonomy_service.canonicalize_model(canonical_brand, structured.model)
            if canonical_brand and structured.model
            else structured.model
        )

        brand_filter_value = (canonical_brand or "").strip().lower()
        model_filter_value = (canonical_model or "").strip().lower()
        fuel_filter_value = (structured.fuel or "").strip().lower()

        brand_value = _normalize_token_text(canonical_brand or "")
        fuel_value = _normalize_token_text(structured.fuel or "")
        model_match_value = _normalize_model_strict(canonical_model or "")

        route = route_query(structured)

        if canonical_brand:
            structured.brand = canonical_brand

        if canonical_model:
            structured.model = canonical_model

        cache_key = (
            f"search:{structured.raw_query}:"
            f"{brand_value}:{model_match_value}:{structured.price_max}:"
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
            "retrieval_stages":[],
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
                "top_score_breakdowns":[],
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

        for q in expanded_queries[:2]:
            try:
                vectors.append(embed_text(q))
            except:
                pass

        # 🔥 СБОРКА ЖЕСТКИХ ФИЛЬТРОВ ДЛЯ БАЗЫ ДАННЫХ (QDRANT)
        must_conditions =[]

        brands_to_filter =[]
        if getattr(structured, "brands", None):
            brands_to_filter =[b for b in structured.brands if b in WHITELIST_SET]
        elif brand_filter_value and brand_filter_value in WHITELIST_SET:
            brands_to_filter = [brand_filter_value]

        if brands_to_filter:
            if len(brands_to_filter) == 1:
                must_conditions.append(
                    FieldCondition(key="brand", match=MatchValue(value=brands_to_filter[0]))
                )
            else:
                # OR condition для "бмв или ауди"
                should_brands =[FieldCondition(key="brand", match=MatchValue(value=b)) for b in brands_to_filter]
                must_conditions.append(Filter(should=should_brands))

        if fuel_filter_value:
            must_conditions.append(
                FieldCondition(key="fuel", match=MatchValue(value=fuel_filter_value))
            )

        if structured.mileage_max is not None:
            must_conditions.append(
                FieldCondition(key="mileage", range=Range(lte=int(structured.mileage_max)))
            )

        # Поддержка точного года и диапазона ("не старше 2023" vs "2023 года")
        year_range_args = {}
        if structured.year_min is not None:
            year_range_args["gte"] = int(structured.year_min)
        if getattr(structured, "year_max", None) is not None:
            year_range_args["lte"] = int(structured.year_max)
            
        if year_range_args:
            must_conditions.append(
                FieldCondition(key="year", range=Range(**year_range_args))
            )

        if structured.price_max is not None:
            must_conditions.append(
                FieldCondition(key="price", range=Range(lte=int(structured.price_max)))
            )

        query_filter = Filter(must=must_conditions) if must_conditions else None

        all_hits =[]
        try:
            for vec in vectors:
                # 🔥 ТЕПЕРЬ ПЕРЕДАЕМ НАШ ФИЛЬТР В ВЕКТОРНУЮ БАЗУ
                stage_hits = self.store.search(
                    vector=vec,
                    limit=500,
                    query_filter=query_filter, 
                    query_text=query_text,
                )
                all_hits.extend(stage_hits)
        except Exception as e:
            print(f"[SEARCH][WARN] qdrant unavailable: {e}", flush=True)
            return []

        print("[DEBUG SEARCH RETRIEVAL]", {
            "query": structured.raw_query,
            "vectors_used": len(vectors),
            "raw_hits_total": len(all_hits),
            "brand": structured.brand,
            "model": structured.model,
            "fuel": structured.fuel,
            "filters_applied": bool(query_filter)
        }, flush=True)

        self._last_debug["raw_hits_total"] = len(all_hits)
        self._last_debug["vectors_used"] = len(vectors)
        self._last_debug["query"] = structured.raw_query

        doc_scores: Dict[str, float] = {}
        doc_payloads: Dict[str, Dict[str, Any]] = {}

        for hit in all_hits:
            payload = hit.payload or {}

            source_url = payload.get("source_url")
            if not source_url:
                continue

            if "avito.ru/all/avtomobili" in source_url:
                continue

            canonical_url = self._canonicalize_source_url(source_url)
            fingerprint = self._build_listing_fingerprint(payload)
            doc_key = canonical_url or fingerprint
            if not doc_key:
                continue

            score = float(getattr(hit, "score", 0.0) or 0.0)

            if doc_key not in doc_scores or score > doc_scores[doc_key]:
                doc_scores[doc_key] = score
                doc_payloads[doc_key] = payload

        print("[DEBUG SEARCH DEDUP]", {
            "raw_hits_total": len(all_hits),
            "unique_docs": len(doc_payloads),
        }, flush=True)

        scored_results: List[Tuple[float, Dict[str, Any], Dict[str, float], List[str]]] = []
        discard_counter = Counter()

        semantic_values = []
        freshness_values =[]
        completeness_values = []
        price_fit_values = []
        mileage_fit_values =[]

        def _post_filter(payload, structured):
            # 🔥 ОТСЕВ МУСОРА: Если в тексте чанка больше 6 цен, это сетка "Похожие объявления" с Drom
            content_text = str(payload.get("content") or "")
            if content_text.count("₽") > 6 or content_text.count("руб") > 6:
                return False

            if getattr(structured, "brands", None):
                if payload.get("brand") not in structured.brands:
                    return False
            elif structured.brand:
                if payload.get("brand") != structured.brand:
                    return False

            if structured.model:
                if not payload.get("model"):
                    return False
                if not _model_soft_match(payload.get("model", ""), structured.model):
                    return False

            # 🔥 ЖЕСТКИЙ ФИЛЬТР ПО ТОПЛИВУ
            if structured.fuel:
                payload_fuel = payload.get("fuel")
                if not payload_fuel or payload_fuel != structured.fuel:
                    return False

            # 🔥 ЖЕСТКИЙ ФИЛЬТР ПО ПРОБЕГУ
            if structured.mileage_max is not None:
                m = payload.get("mileage")
                y = payload.get("year")
                try:
                    is_new = y and int(y) >= 2024
                except:
                    is_new = False
                
                if m is None:
                    if not is_new:
                        return False
                elif m > structured.mileage_max:
                    return False

            # 🔥 ЖЕСТКИЙ ФИЛЬТР ПО ГОДУ
            y = payload.get("year")
            if structured.year_min is not None:
                if y is None or int(y) < structured.year_min:
                    return False
            if getattr(structured, "year_max", None) is not None:
                if y is None or int(y) > structured.year_max:
                    return False

            return True

        post_filter_kept = 0

        for doc_key, payload in doc_payloads.items():
            debug["filtering"]["checked_candidates"] += 1

            env_name = (os.getenv("ENV", "") or os.getenv("APP_ENV", "") or "dev").lower()
            is_prod = env_name == "prod"

            if is_prod and payload.get("source") == "dev_seed":
                discard_counter["dev_seed_prod"] += 1
                debug["filtering"]["discarded_candidates"] += 1
                continue

            semantic = float(doc_scores.get(doc_key, 0.0) or 0.0)

            if not _post_filter(payload, structured):
                continue
            
            post_filter_kept += 1

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

            reasons_list =[
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

        if post_filter_kept == 0 and doc_payloads:
            print("[DEBUG SEARCH FALLBACK] post-filter killed everything, using top raw docs", flush=True)
            for doc_key, payload in list(doc_payloads.items())[:50]:
                
                # 🔥 СТРОГИЙ ЗАПРЕТ ПОДМЕНЫ МАРКИ И МОДЕЛИ В РЕЖИМЕ СПАСЕНИЯ (FALLBACK)
                if structured.brand and payload.get("brand"):
                    if payload.get("brand") != structured.brand:
                        continue
                if structured.model and payload.get("model"):
                    if not _model_soft_match(payload.get("model", ""), structured.model):
                        continue

                semantic = float(doc_scores.get(doc_key, 0.0) or 0.0)
                final_score, signals = self._score_candidate(payload, structured, semantic, route)
                reasons_list =[
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

        print("[DEBUG SEARCH FILTERING]", {
            "post_filter_kept": post_filter_kept,
            "scored_results": len(scored_results),
            "discard_reasons": dict(discard_counter),
        }, flush=True)

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

        results: List[Dict[str, Any]] =[]
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

            if canonical_url:
                seen_canonical_urls.add(canonical_url)
            if fingerprint:
                seen_fingerprints.add(fingerprint)

            source_counter[source_name] += 1

            if len(results) >= max(limit, self._env_int("SEARCH_RERANK_MAX_CANDIDATES", 50)):
                break

        try:
            results = self._rerank_results(
                query=structured.raw_query or query_text,
                results=results,
                top_k=min(len(results), limit),
            )

            if structured.brand:
                results =[
                    r for r in results
                    if (r.get("brand") or "").lower() == structured.brand.lower()
                ] or results

            if structured.fuel:
                boosted =[]
                for r in results:
                    payload_fuel = (r.get("fuel") or "").lower()
                    if structured.fuel == "electric":
                        if payload_fuel == "electric":
                            r["score"] *= 1.3
                        else:
                            r["score"] *= 0.3
                    else:
                        if payload_fuel == structured.fuel:
                            r["score"] *= 1.2
                        else:
                            r["score"] *= 0.85
                    boosted.append(r)
                results = sorted(boosted, key=lambda x: x["score"], reverse=True)

            if canonical_model:
                boosted =[]
                for r in results:
                    if r.get("model") and _model_soft_match(r.get("model", ""), canonical_model):
                        r["score"] *= 1.4
                    else:
                        r["score"] *= 0.6
                    boosted.append(r)
                results = sorted(boosted, key=lambda x: x["score"], reverse=True)

            debug["final"]["rerank_applied"] = len(results) > 0

        except Exception as e:
            print(f"[RERANK][WARN] skipped: {e}", flush=True)

        print("[DEBUG SEARCH FINAL]", {
            "results": len(results),
            "top_3":[
                {
                    "brand": r.get("brand"),
                    "model": r.get("model"),
                    "fuel": r.get("fuel"),
                    "price": r.get("price"),
                    "score": r.get("score"),
                }
                for r in results[:3]
            ]
        })

        debug["final"]["results_count"] = len(results)

        if not results:
            fallback =[]
            for doc_key, payload in list(doc_payloads.items())[:20]:
                
                # 🔥 СТРОГАЯ ПРОВЕРКА МАРКИ ДЛЯ FALLBACK
                if getattr(structured, "brands", None):
                    if payload.get("brand") not in structured.brands:
                        continue
                elif structured.brand:
                    if payload.get("brand") != structured.brand:
                        continue
                
                # 🔥 СТРОГАЯ ПРОВЕРКА МОДЕЛИ ДЛЯ FALLBACK
                if structured.model:
                    if not payload.get("model"):
                        continue
                    if not _model_soft_match(payload.get("model", ""), structured.model):
                        continue

                fallback.append({
                    "brand": payload.get("brand"),
                    "model": payload.get("model"),
                    "year": payload.get("year"),
                    "mileage": payload.get("mileage"),
                    "price": payload.get("price"),
                    "currency": payload.get("currency", "RUB"),
                    "fuel": payload.get("fuel"),
                    "region": payload.get("region"),
                    "paint_condition": payload.get("paint_condition"),
                    "score": 0.01,
                    "why_match": "Fallback: точных совпадений по жестким фильтрам не найдено",
                    "source_url": payload.get("source_url"),
                    "source_name": payload.get("source") or "unknown",
                    "score_breakdown": {"semantic": 0.01}
                })
            return fallback

        return results

    def _text_score(self, payload: Dict[str, Any], structured: StructuredQuery) -> float:
        text_parts: List[str] =[]

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
            score += 1.5

        if structured.model:
            model_norm = _normalize_token_text(structured.model)
            if model_norm in text or _compact_token_text(model_norm) in _compact_token_text(text):
                score += 1.5

        if structured.fuel:
            fuel_value = _normalize_token_text(structured.fuel)
            payload_fuel = _normalize_token_text(str(payload.get("fuel") or ""))
            if payload_fuel:
                if payload_fuel == fuel_value:
                    score += 0.8

        for kw in getattr(structured, "keywords", []) or[]:
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

            if ratio > 1.0:
                return 0.05

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

        post_filter_kept


        _score_candidate