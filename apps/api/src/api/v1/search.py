from fastapi import APIRouter
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import time

from db.models import SearchHistory
from db.session import SessionLocal

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
        example="BMW до 50 000 км, без окрасов, бензин"
    )
    include_answer: bool = False


# =========================
# RESPONSE SCHEMAS
# =========================

class SearchResult(BaseModel):
    brand: Optional[str]
    model: Optional[str]
    year: Optional[int]
    mileage: Optional[int]
    price: Optional[int]
    currency: str = "RUB"

    fuel: Optional[str]
    color: Optional[str]
    region: Optional[str]
    condition: Optional[str]
    paint_condition: Optional[str]

    score: float
    why_match: str

    source_url: str
    source_name: Optional[str]


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
    structuredQuery: Dict[str, Any]   # ✅ ВАЖНО
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
    summary="Semantic auto search"
)
def search(request: SearchRequest):
    started_at = time.time()

    structured: Optional[StructuredQuery] = None
    results: List[dict] = []
    answer = None

    try:
        structured = parse_query(request.query)

        service = SearchService()
        results = service.search(structured)

        if request.include_answer and AnswerBuilder:
            builder = AnswerBuilder()
            answer = builder.build(structured, results)

        # RETENTION
        try:
            session = SessionLocal()
            session.add(
                SearchHistory(
                    raw_query=request.query,
                    structured_query=structured.model_dump(),
                    results_count=len(results),
                    empty_result=len(results) == 0,
                )
            )
            session.commit()
        except Exception:
            pass
        finally:
            session.close()

    except Exception:
        latency_ms = int((time.time() - started_at) * 1000)
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

    source_counter = {}
    for r in results:
        name = r.get("source_name") or "unknown"
        source_counter[name] = source_counter.get(name, 0) + 1

    sources = [
        {"name": name, "result_count": count}
        for name, count in source_counter.items()
    ]

    latency_ms = int((time.time() - started_at) * 1000)

    try:
        metrics = MetricsService()
        metrics.log_search(
            raw_query=request.query,
            structured_query=structured.model_dump(),
            results_count=len(results),
            latency_ms=latency_ms,
        )
    except Exception:
        pass

    return {
        "structuredQuery": structured.model_dump(),
        "results": results,
        "sources": sources,
        "answer": answer,
        "debug": {
            "latency_ms": latency_ms,
            "vector_hits": len(results),
            "final_results": len(results),
            "query_language": "ru",
            "empty_result": len(results) == 0,
        },
    }
