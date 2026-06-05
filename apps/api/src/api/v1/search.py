from fastapi import APIRouter, Query
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import time
import uuid

from services.query_parser import parse_query
from domain.query_schema import StructuredQuery
from services.search_service import SearchService
from services.metrics_service import MetricsService

try:
    from services.answer_builder import AnswerBuilder
except Exception:
    AnswerBuilder = None

router = APIRouter()

# =========================
# REQUEST
# =========================

class SearchRequest(BaseModel):
    query: str = Field(
        ...,
        example="BMW до 50 000 км, без окрасов, бензин",
    )
    include_answer: bool = False
    page: Optional[int] = Field(1, ge=1, description="Page number for pagination")
    limit: Optional[int] = Field(20, ge=1, le=50, description="Items per page to fetch (max 50)")

# =========================
# RESPONSE SCHEMAS
# =========================

class SearchResult(BaseModel):
    brand: Optional[str] = None
    model: Optional[str] = None
    year: Optional[int] = None
    mileage: Optional[int] = None
    price: Optional[int] = None
    currency: str = "RUB"

    fuel: Optional[str] = None
    color: Optional[str] = None
    region: Optional[str] = None
    city: Optional[str] = None
    condition: Optional[str] = None
    paint_condition: Optional[str] = None

    # 🔥 ТЕПЕРЬ ЭТИ ПОЛЯ ОПЦИОНАЛЬНЫ (API больше не упадет с 500 ошибкой)
    score: Optional[float] = 0.0
    why_match: Optional[str] = None
    source_url: str = Field(..., description="Every result item must contain source_url")
    source_name: Optional[str] = None
    score_breakdown: Optional[Dict[str, float]] = None

    # Дополнительные метаданные
    listing_id: str = Field(..., description="Every result item must contain listing_id")
    image_url: str = Field("", description="Every result item must contain image_url")
    photos: List[str] = []
    created_at: Optional[str] = None
    created_at_ts: Optional[int] = None


class SourceStat(BaseModel):
    name: str
    result_count: int

class PaginationInfo(BaseModel):
    total: int
    page: int
    limit: int
    pages: int

class DebugInfo(BaseModel):
    latency_ms: int
    vector_hits: int
    final_results: int
    query_language: str
    empty_result: bool
    request_id: Optional[str] = None
    parsed_query: Optional[Dict[str, Any]] = None
    filters_applied: Optional[Dict[str, Any]] = None

class SearchResponse(BaseModel):
    request_id: str
    structuredQuery: Dict[str, Any]
    results: List[SearchResult]
    sources: List[SourceStat]
    debug: DebugInfo
    pagination: Optional[PaginationInfo] = None
    answer: Optional[str] = None

# =========================
# ENDPOINT
# =========================

@router.post(
    "/search",
    response_model=SearchResponse,
    summary="Semantic auto search",
)
def search(
    request: SearchRequest,
    telegram_user_id: Optional[int] = Query(None, description="Optional telegram user ID for bot-api tracking"),
    request_id: Optional[str] = Query(None, description="Optional custom request ID"),
):
    started_at = time.time()
    req_id = request_id or str(uuid.uuid4())

    structured: Optional[StructuredQuery] = None
    results: List[dict] = []
    answer: Optional[str] = None

    vector_hits = 0

    try:
        # -------------------------
        # PARSE QUERY
        # -------------------------
        structured = parse_query(request.query)

        if structured:
            structured_payload = (
                structured.model_dump()
                if hasattr(structured, "model_dump")
                else structured.dict()
            )
        else:
            structured_payload = {}

        # Извлечение примененных фильтров для debug
        filters_applied = {
            k: v for k, v in structured_payload.items()
            if v is not None and v != [] and v != {}
        } if structured_payload else {}

        # -------------------------
        # SEARCH
        # -------------------------
        service = SearchService()

        try:
            results = service.search(structured)

            service_debug = getattr(service, "_last_debug", {}) or {}
            vector_hits = int(service_debug.get("raw_hits_total", 0))

            print(
                f"[SEARCH] query='{request.query}' results={len(results)} telegram_user={telegram_user_id}",
                flush=True,
            )
            print(f"[SEARCH][DEMO] hits={vector_hits}")

        except Exception as e:
            print(f"[SEARCH][DEMO][WARN] search skipped: {repr(e)}")
            results = []
            vector_hits = 0

        # -------------------------
        # OPTIONAL ANSWER
        # -------------------------
        if request.include_answer and AnswerBuilder and results:
            try:
                builder = AnswerBuilder()
                answer = builder.build(structured, results)
            except Exception:
                answer = None

    except Exception as e:
        latency_ms = int((time.time() - started_at) * 1000)
        # Логируем ошибку, не допуская падения всего API
        print(f"[SEARCH][ERROR] {repr(e)}")

        structured_payload = (
            structured.model_dump()
            if structured and hasattr(structured, "model_dump")
            else structured.dict()
            if structured
            else {}
        )

        return {
            "request_id": req_id,
            "structuredQuery": structured_payload,
            "results": [],
            "sources": [],
            "answer": None,
            "pagination": {
                "total": 0,
                "page": request.page or 1,
                "limit": request.limit or 20,
                "pages": 0,
            },
            "debug": {
                "latency_ms": latency_ms,
                "vector_hits": 0,
                "final_results": 0,
                "query_language": "ru",
                "empty_result": True,
                "request_id": req_id,
                "parsed_query": structured_payload,
                "filters_applied": {},
            },
        }

    # -------------------------
    # PAGINATION & MAPPING
    # -------------------------
    total_results = len(results)
    page = request.page or 1
    limit = request.limit or 20
    
    # Защитное ограничение лимитов страниц (LIMIT max = 50)
    limit = max(1, min(50, limit))
    page = max(1, page)

    start_idx = (page - 1) * limit
    end_idx = start_idx + limit

    paginated_results = results[start_idx:end_idx]
    pages_count = (total_results + limit - 1) // limit if total_results > 0 else 0

    mapped_results = []
    for r in paginated_results:
        # Каждый результат гарантированно содержит listing_id, image_url и source_url
        mapped_results.append({
            "brand": r.get("brand"),
            "model": r.get("model"),
            "year": r.get("year"),
            "mileage": r.get("mileage"),
            "price": r.get("price"),
            "currency": r.get("currency") or "RUB",
            "fuel": r.get("fuel"),
            "color": r.get("color"),
            "region": r.get("region"),
            "city": r.get("city"),
            "condition": r.get("condition"),
            "paint_condition": r.get("paint_condition"),
            "score": r.get("score") or 0.0,
            "why_match": r.get("why_match"),
            "source_url": r.get("source_url") or "",
            "source_name": r.get("source_name") or r.get("source") or "unknown",
            "score_breakdown": r.get("score_breakdown"),
            "listing_id": str(r.get("listing_id") or r.get("id") or ""),
            "image_url": r.get("image_url") or "",
            "photos": r.get("photos") if isinstance(r.get("photos"), list) else [],
            "created_at": r.get("created_at"),
            "created_at_ts": r.get("created_at_ts"),
        })

    # -------------------------
    # SOURCES STATS
    # -------------------------
    source_counter: Dict[str, int] = {}
    for r in results:
        name = r.get("source_name") or r.get("source") or "unknown"
        source_counter[name] = source_counter.get(name, 0) + 1

    sources = [
        {"name": name, "result_count": count}
        for name, count in source_counter.items()
    ]

    latency_ms = int((time.time() - started_at) * 1000)

    # -------------------------
    # METRICS
    # -------------------------
    try:
        metrics = MetricsService()
        metrics.log_search(
            raw_query=request.query,
            structured_query=structured_payload,
            results_count=len(mapped_results),
            latency_ms=latency_ms,
            results=mapped_results,
        )
    except Exception:
        pass

    return {
        "request_id": req_id,
        "structuredQuery": structured_payload,
        "results": mapped_results,
        "sources": sources,
        "answer": answer,
        "pagination": {
            "total": total_results,
            "page": page,
            "limit": limit,
            "pages": pages_count,
        },
        "debug": {
            "latency_ms": latency_ms,
            "vector_hits": vector_hits,
            "final_results": len(mapped_results),
            "query_language": "ru",
            "empty_result": total_results == 0,
            "request_id": req_id,
            "parsed_query": structured_payload,
            "filters_applied": filters_applied,
        },
    }