from typing import List, Dict, Any, Tuple
import yaml

from integrations.vector_db.qdrant import QdrantStore
from data_pipeline.index import deterministic_embedding
from services.query_parser import StructuredQuery


# =========================
# LOAD BRANDS WHITELIST
# =========================

def load_brands() -> Dict[str, list]:
    try:
        with open("brands.yaml", "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            return data.get("brands", {})
    except Exception as e:
        print(f"[SEARCH][WARN] failed to load brands.yaml: {e}")
        return {}


BRANDS_WHITELIST = load_brands()
WHITELIST_SET = set(BRANDS_WHITELIST.keys())


class SearchService:
    def __init__(self):
        self.store = QdrantStore()

    def search(
        self,
        structured: StructuredQuery,
        limit: int = 20,
        top_k: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Основной метод поиска:
        - детерминированный embedding запроса
        - поиск в Qdrant
        - ranking (boosts / penalties)
        - dedup по source_url
        """

        # -------------------------
        # 1. Structured → embedding
        # -------------------------
        query_text = self._build_query_text(structured)
        query_vector = deterministic_embedding(query_text)

        # -------------------------
        # 2. Поиск в Qdrant
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

            score, reasons = self._score_hit(
                base_score=float(hit.score),
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
                    "color": payload.get("color"),
                    "region": payload.get("region"),
                    "condition": payload.get("condition"),
                    "paint_condition": payload.get("paint_condition"),
                    "score": round(score, 4),
                    "why_match": ", ".join(reasons) if reasons else "релевантно запросу",
                    "source_url": source_url,
                    "source_name": payload.get("source"),
                }
            )

            seen_urls.add(source_url)

            if len(results) >= limit:
                break

        # -------------------------
        # 4. Финальная сортировка
        # -------------------------
        results.sort(key=lambda r: r["score"], reverse=True)
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

    def _score_hit(
        self,
        base_score: float,
        payload: Dict[str, Any],
        structured: StructuredQuery,
    ) -> Tuple[float, List[str]]:
        score = base_score
        reasons: List[str] = []

        payload_brand = payload.get("brand")

        # ---------- BRAND LOGIC ----------
        if structured.brand and payload_brand == structured.brand:
            score += 0.30
            reasons.append("точное совпадение марки")

        elif payload_brand in WHITELIST_SET:
            score += 0.08
            reasons.append("марка из whitelist")

        else:
            score -= 0.20
            reasons.append("марка вне whitelist")

        # ---------- OTHER BOOSTS ----------
        if structured.model and payload.get("model") == structured.model:
            score += 0.20
            reasons.append("совпадает модель")

        if structured.region and payload.get("region") == structured.region:
            score += 0.15
            reasons.append("подходит регион")

        if (
            structured.mileage_max
            and payload.get("mileage")
            and payload["mileage"] <= structured.mileage_max
        ):
            score += 0.10
            reasons.append("пробег в пределах запроса")

        if (
            structured.price_max
            and payload.get("price")
            and payload["price"] <= structured.price_max
        ):
            score += 0.10
            reasons.append("цена в пределах запроса")

        # ---------- PENALTIES ----------
        if (
            structured.mileage_max
            and payload.get("mileage")
            and payload["mileage"] > structured.mileage_max
        ):
            score -= 0.40
            reasons.append("превышен пробег")

        if (
            structured.price_max
            and payload.get("price")
            and payload["price"] > structured.price_max
        ):
            score -= 0.40
            reasons.append("превышена цена")

        return score, reasons
