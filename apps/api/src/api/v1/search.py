from fastapi import APIRouter
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import time

from services.query_parser import parse_query, StructuredQuery
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
    condition: Optional[str] = None
    paint_condition: Optional[str] = None

    score: float
    why_match: str

    source_url: str
    source_name: Optional[str] = None


class SourceStat(BaseModel):
    name: str
    result_count: int


class DebugInfo(BaseModel):
    latency_ms: int
    vector_hits: int
    final_results: int
    query_language: str
    empty_result: bool


class SearchResponse(BaseModel):
    structuredQuery: Dict[str, Any]
    results: List[SearchResult]
    sources: List[SourceStat]
    debug: DebugInfo
    answer: Optional[str] = None


# =========================
# ENDPOINT
# =========================

@router.post(
    "/search",
    response_model=SearchResponse,
    summary="Semantic auto search",
)
def search(request: SearchRequest):
    started_at = time.time()

    structured: Optional[StructuredQuery] = None
    results: List[dict] = []
    answer: Optional[str] = None
    vector_hits = 0

    try:
        # -------------------------
        # PARSE QUERY
        # -------------------------
        structured = parse_query(request.query)

        # -------------------------
        # SEARCH (SAFE FOR DEMO)
        # -------------------------
        service = SearchService()

        try:
            results = service.search(structured)
            vector_hits = len(results)
            print(f"[SEARCH][DEMO] hits={vector_hits}")
        except Exception as e:
            # 🔥 КЛЮЧЕВОЕ ДЛЯ SMOKE DEMO
            # Qdrant пуст / коллекции нет / index не запускался
            print(f"[SEARCH][DEMO][WARN] search skipped: {e}")
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

        print(f"[SEARCH][ERROR] {e}")

        return {
            "structuredQuery": structured.model_dump() if structured else {},
            "results": [],
            "sources": [],
            "answer": None,
            "debug": {
                "latency_ms": latency_ms,
                "vector_hits": 0,
                "final_results": 0,
                "query_language": "ru",
                "empty_result": True,
            },
        }

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
    # METRICS (SAFE)
    # -------------------------
    try:
        metrics = MetricsService()
        metrics.log_search(
            raw_query=request.query,
            structured_query=structured.model_dump() if structured else {},
            results_count=len(results),
            latency_ms=latency_ms,
        )
    except Exception:
        pass

    return {
        "structuredQuery": structured.model_dump() if structured else {},
        "results": results,
        "sources": sources,
        "answer": answer,
        "debug": {
            "latency_ms": latency_ms,
            "vector_hits": vector_hits,
            "final_results": len(results),
            "query_language": "ru",
            "empty_result": len(results) == 0,
        },
    }
