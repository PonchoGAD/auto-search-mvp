from db.session import SessionLocal
from db.models import SearchEvent


class MetricsService:
    def log_search(
        self,
        raw_query: str,
        structured_query: dict,
        results_count: int,
        latency_ms: int,
    ):
        session = SessionLocal()

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
