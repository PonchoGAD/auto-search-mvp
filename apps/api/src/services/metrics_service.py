from __future__ import annotations

from typing import Any, Dict, List, Optional
from db.session import SessionLocal
from db.models import SearchEvent


def _safe_lower(x: Any) -> Optional[str]:
    return x.lower().strip() if isinstance(x, str) else None


def _precision_at_k(structured: Dict[str, Any], results: List[Dict[str, Any]], k: int = 10) -> Dict[str, Any]:
    top = results[:k] if results else []

    brand_q = _safe_lower(structured.get("brand"))
    model_q = _safe_lower(structured.get("model"))
    fuel_q = _safe_lower(structured.get("fuel"))

    price_max = structured.get("price_max")
    mileage_max = structured.get("mileage_max")
    year_min = structured.get("year_min")

    def ok_brand(r: Dict[str, Any]) -> bool:
        if not brand_q:
            return True
        return _safe_lower(r.get("brand")) == brand_q

    def ok_model(r: Dict[str, Any]) -> bool:
        if not model_q:
            return True
        return _safe_lower(r.get("model")) == model_q

    def ok_fuel(r: Dict[str, Any]) -> bool:
        if not fuel_q:
            return True
        return _safe_lower(r.get("fuel")) == fuel_q

    def ok_price(r: Dict[str, Any]) -> bool:
        if price_max is None:
            return True
        p = r.get("price")
        return isinstance(p, int) and p <= price_max

    def ok_mileage(r: Dict[str, Any]) -> bool:
        if mileage_max is None:
            return True
        m = r.get("mileage")
        return isinstance(m, int) and m <= mileage_max

    def ok_year(r: Dict[str, Any]) -> bool:
        if year_min is None:
            return True
        y = r.get("year")
        return isinstance(y, int) and y >= year_min

    def rate(fn) -> float:
        if not top:
            return 0.0
        return round(sum(1 for r in top if fn(r)) / float(len(top)), 3)

    metrics = {
        "k": k,
        "p_brand": rate(ok_brand),
        "p_model": rate(ok_model),
        "p_fuel": rate(ok_fuel),
        "p_price": rate(ok_price),
        "p_mileage": rate(ok_mileage),
        "p_year": rate(ok_year),
        "top_count": len(top),
    }

    # "all constraints satisfied" (если пользователь реально указал ограничения)
    constraints = []
    if brand_q:
        constraints.append(ok_brand)
    if model_q:
        constraints.append(ok_model)
    if fuel_q:
        constraints.append(ok_fuel)
    if price_max is not None:
        constraints.append(ok_price)
    if mileage_max is not None:
        constraints.append(ok_mileage)
    if year_min is not None:
        constraints.append(ok_year)

    if constraints and top:
        ok_all = 0
        for r in top:
            if all(fn(r) for fn in constraints):
                ok_all += 1
        metrics["p_all"] = round(ok_all / float(len(top)), 3)
    else:
        metrics["p_all"] = None

    return metrics


class MetricsService:
    def log_search(
        self,
        raw_query: str,
        structured_query: dict,
        results_count: int,
        latency_ms: int,
        results: Optional[List[Dict[str, Any]]] = None,  # ✅ backward compatible
    ):
        session = SessionLocal()

        # attach metrics into structured_query to avoid DB migrations
        try:
            if results is not None:
                structured_query = dict(structured_query or {})
                structured_query["_metrics"] = _precision_at_k(structured_query, results, k=10)
        except Exception:
            # never break prod due to metrics
            pass

        event = SearchEvent(
            raw_query=raw_query,
            structured_query=structured_query,
            results_count=results_count,
            latency_ms=latency_ms,
            empty_result=results_count == 0,
        )

        session.add(event)
        session.commit()
        session.close()