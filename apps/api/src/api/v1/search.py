from fastapi import APIRouter
from pydantic import BaseModel, Field
from typing import List, Optional
import time

from services.query_parser import parse_query, StructuredQuery
from services.search_service import SearchService
from services.metrics_service import MetricsService

# answer_builder может отсутствовать — импортим безопасно
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
    structuredQuery: StructuredQuery
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

    # значения по умолчанию — чтобы никогда не упасть
    structured = None
    results: List[dict] = []
    answer = None

    try:
        # 1️⃣ Parse query
        structured = parse_query(request.query)

        # 2️⃣ Search
        service = SearchService()
        results = service.search(structured)

        # 3️⃣ Optional answer
        if request.include_answer and AnswerBuilder:
            builder = AnswerBuilder()
            answer = builder.build(structured, results)

    except Exception as e:
        # ❗ Абсолютная гарантия JSON
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

    # 4️⃣ Sources aggregation
    source_counter = {}
    for r in results:
        name = r.get("source_name") or "unknown"
        source_counter[name] = source_counter.get(name, 0) + 1

    sources = [
        {"name": name, "result_count": count}
        for name, count in source_counter.items()
    ]

    # 5️⃣ Debug
    latency_ms = int((time.time() - started_at) * 1000)

    # 6️⃣ Metrics (НЕ ломает API)
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
