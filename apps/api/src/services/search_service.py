# apps/api/src/services/search_service.py

from typing import List, Dict, Any, Tuple
import yaml

from integrations.vector_db.qdrant import QdrantStore
from data_pipeline.index import deterministic_embedding
from services.query_parser import StructuredQuery

from db.session import SessionLocal
from db.models import SearchHistory


# =========================
# LOAD BRANDS WHITELIST
# =========================

def load_brands() -> Dict[str, dict]:
    try:
        with open("brands.yaml", "r", encoding="utf-8") as f:
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
    # форумы (премиум)
    "benzclub.ru": 1.5,
    "bmwclub.ru": 1.5,
    "forum": 1.5,

    # нейтрально
    "telegram": 1.0,

    # маркетплейсы
    "auto.ru": 0.8,
    "drom.ru": 0.8,
    "avito.ru": 0.8,
    "marketplace": 0.8,
}


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
        """
        Поиск:
        - embedding запроса
        - vector search
        - explainable ranking (boosts / penalties)
        """

        # -------------------------
        # 1. Query → embedding
        # -------------------------
        query_text = self._build_query_text(structured)
        query_vector = deterministic_embedding(query_text)

        # -------------------------
        # 2. Vector search
        # -------------------------
        hits = self.store.search(
            vector=query_vector,
            limit=top_k,
        )

        results: List[Dict[str, Any]] = []
        seen_urls = set()

        # -------------------------
        # 3. Ranking + Dedup
        # -------------------------
        for hit in hits:
            payload = hit.payload or {}
            source_url = payload.get("url")

            if not source_url or source_url in seen_urls:
                continue

            final_score, reasons = self._score_hit(
                vector_score=float(hit.score),
                payload=payload,
                structured=structured,
            )

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
                    "score": round(final_score, 4),
                    "why_match": " + ".join(reasons) if reasons else "релевантно запросу",
                    "source_url": source_url,
                    "source_name": payload.get("source"),
                }
            )

            seen_urls.add(source_url)

            if len(results) >= limit:
                break

        # -------------------------
        # 4. Final sort
        # -------------------------
        results.sort(key=lambda r: r["score"], reverse=True)

        # -------------------------
        # 5. SAVE SEARCH HISTORY (RETENTION)
        # -------------------------
        try:
            session = SessionLocal()

            history = SearchHistory(
                raw_query=structured.raw_query if hasattr(structured, "raw_query") else "",
                structured_query=structured.dict(),
                results_count=len(results),
                empty_result=len(results) == 0,
                source="search_api",
            )

            session.add(history)
            session.commit()

        except Exception as e:
            # ❗️ НИКОГДА не ломаем поиск из-за аналитики
            print(f"[SEARCH][WARN] failed to save search history: {e}")

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
    ) -> Tuple[float, List[str]]:
        """
        Итоговый скоринг (объяснимый):

        final_score =
            vector_score
            * source_boost
            * brand_boost
            * sale_boost
            * structured_bonus
        """

        reasons: List[str] = []

        # -------------------------
        # Source boost
        # -------------------------
        source = payload.get("source") or ""
        source_boost = SOURCE_BOOSTS.get(source, 1.0)

        if source_boost != 1.0:
            reasons.append(f"source_boost={source_boost} ({source})")

        # -------------------------
        # Brand boost
        # -------------------------
        payload_brand = payload.get("brand")

        if payload_brand and payload_brand.lower() in WHITELIST_SET:
            brand_boost = 1.15
            reasons.append(f"brand_hit={payload_brand}")
        else:
            brand_boost = 0.9
            reasons.append("brand_outside_whitelist")

        # -------------------------
        # Sale intent boost
        # -------------------------
        sale_intent = payload.get("sale_intent")
        if str(sale_intent) == "1":
            sale_boost = 1.1
            reasons.append("sale_intent=true")
        else:
            sale_boost = 0.85
            reasons.append("sale_intent=false")

        # -------------------------
        # Structured query bonuses
        # -------------------------
        bonus = 1.0

        if structured.brand and payload_brand == structured.brand:
            bonus += 0.10
            reasons.append("exact_brand_match")

        if structured.model and payload.get("model") == structured.model:
            bonus += 0.10
            reasons.append("exact_model_match")

        if (
            structured.price_max
            and payload.get("price")
            and payload["price"] <= structured.price_max
        ):
            bonus += 0.05
            reasons.append("price_ok")

        if (
            structured.mileage_max
            and payload.get("mileage")
            and payload["mileage"] <= structured.mileage_max
        ):
            bonus += 0.05
            reasons.append("mileage_ok")

        # -------------------------
        # Final score
        # -------------------------
        final_score = (
            vector_score
            * source_boost
            * brand_boost
            * sale_boost
            * bonus
        )

        return final_score, reasons
