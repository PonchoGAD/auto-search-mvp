from __future__ import annotations

import logging
from time import perf_counter

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from src.clients.search_api import SearchApiClient
from src.db.session import get_db
from src.dependencies.auth import verify_internal_api_key
from src.repositories.search_history import SearchHistoryRepository
from src.repositories.users import UsersRepository
from src.schemas.search import ListingDetailsResponse, SearchRequest, SearchResponse
from src.services.search_gateway import SearchGatewayService


logger = logging.getLogger(__name__)
router = APIRouter(prefix="/search-proxy", tags=["search-proxy"])


@router.get("/health")
async def proxy_search_core_health() -> dict:
    client = SearchApiClient()

    try:
        health = await client.health()
        return {
            "status": "ok",
            "search_core": health,
        }
    except Exception as exc:
        logger.exception("search_core_health_failed error=%s", repr(exc))
        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={
                "message": "Search core недоступен.",
                "error": str(exc),
            },
        ) from exc


@router.post(
    "/search",
    response_model=SearchResponse,
    dependencies=[Depends(verify_internal_api_key)],
)
async def proxy_search(
    payload: SearchRequest,
    telegram_user_id: int | None = Query(default=None),
    db: Session = Depends(get_db),
) -> SearchResponse:
    started_at = perf_counter()

    logger.info(
        "search_proxy_start telegram_user_id=%s query=%s page=%s limit=%s",
        telegram_user_id,
        payload.query,
        payload.page,
        payload.limit,
    )

    try:
        search_service = SearchGatewayService()
        response = await search_service.search(payload)
    except Exception as exc:
        latency_ms = int((perf_counter() - started_at) * 1000)

        logger.exception(
            "search_proxy_failed telegram_user_id=%s query=%s latency_ms=%s error=%s",
            telegram_user_id,
            payload.query,
            latency_ms,
            repr(exc),
        )

        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={
                "message": "Search core недоступен. Проверь VPS, nginx, порт 80 и /api/v1/health.",
                "error": str(exc),
            },
        ) from exc

    latency_ms = int((perf_counter() - started_at) * 1000)

    response.debug.latency_ms = latency_ms
    response.debug.final_results = len(response.results)
    response.debug.empty_result = len(response.results) == 0

    if telegram_user_id is not None:
        try:
            user_repo = UsersRepository(db)
            user = user_repo.get_by_telegram_user_id(telegram_user_id)

            if user is not None:
                history_repo = SearchHistoryRepository(db)
                history_repo.create(
                    user_id=user.id,
                    raw_query=payload.query,
                    query_payload={
                        "query": payload.query,
                        "page": payload.page,
                        "limit": payload.limit,
                        "include_answer": payload.include_answer,
                        "structured_query": response.structured_query,
                    },
                    results_count=len(response.results),
                    latency_ms=latency_ms,
                    empty_result=len(response.results) == 0,
                )
        except Exception as exc:
            logger.exception(
                "search_history_write_failed telegram_user_id=%s query=%s error=%s",
                telegram_user_id,
                payload.query,
                repr(exc),
            )

    logger.info(
        "search_proxy_done telegram_user_id=%s query=%s results=%s latency_ms=%s",
        telegram_user_id,
        payload.query,
        len(response.results),
        latency_ms,
    )

    return response


@router.get(
    "/listings/{listing_id}",
    response_model=ListingDetailsResponse,
    dependencies=[Depends(verify_internal_api_key)],
)
async def proxy_listing_details(listing_id: str) -> ListingDetailsResponse:
    client = SearchApiClient()

    try:
        return await client.get_listing(listing_id)
    except Exception as exc:
        logger.exception(
            "search_proxy_listing_failed listing_id=%s error=%s",
            listing_id,
            repr(exc),
        )

        raise HTTPException(
            status_code=status.HTTP_502_BAD_GATEWAY,
            detail={
                "message": "Не удалось получить объявление из search core.",
                "listing_id": listing_id,
                "error": str(exc),
            },
        ) from exc