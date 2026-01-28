from fastapi import APIRouter
from pydantic import BaseModel, Field
from typing import List, Optional, Dict, Any
import time

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
    structuredQuery: Dict[str, Any]  # ✅ ВАЖНО
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

    # NOTE:
    # - SearchHistory (retention) сейчас пишется внутри SearchService.search()
    # - Здесь НЕ дублируем запись, чтобы не было двойных логов и конфликтов таблиц

    try:
        structured = parse_query(request.query)

        service = SearchService()
        results = service.search(structured)

        if request.include_answer and AnswerBuilder:
            try:
                builder = AnswerBuilder()
                answer = builder.build(structured, results)
            except Exception:
                answer = None

    except Exception:
        latency_ms = int((time.time() - started_at) * 1000)
        # Важно: endpoint не должен падать
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
    # METRICS (не ломает поиск)
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
            # В MVP без доступа к raw hits считаем vector_hits как число финальных результатов.
            # (Если захочешь — расширим SearchService, чтобы он возвращал и hits_count отдельно.)
            "vector_hits": len(results),
            "final_results": len(results),
            "query_language": "ru",
            "empty_result": len(results) == 0,
        },
    }
