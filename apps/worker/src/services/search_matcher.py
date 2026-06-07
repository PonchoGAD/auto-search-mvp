from __future__ import annotations

from typing import Any

from src.clients.search_api import SearchApiClient
from src.config import settings
from src.logging import get_logger
from src.schemas.listing import ListingResult
from src.services.deduplication import (
    RedisDeduplicator,
    deduplicate_items,
    filter_new_items,
    pick_last_seen_listing_id,
)


logger = get_logger(__name__)


class SearchMatcherService:
    def __init__(self) -> None:
        self.search_client = SearchApiClient()

    async def run_saved_search(
        self,
        saved_search: dict[str, Any],
    ) -> dict[str, Any]:
        raw_query = str(saved_search.get("raw_query") or "").strip()
        saved_search_id = saved_search.get("id")
        last_seen_listing_id = str(saved_search.get("last_seen_listing_id") or "").strip() or None

        if not raw_query:
            logger.warning(
                "saved_search_empty_query saved_search_id=%s",
                saved_search_id,
            )

            return {
                "saved_search_id": saved_search_id,
                "user_id": saved_search.get("user_id"),
                "name": saved_search.get("name"),
                "raw_query": "",
                "results": [],
                "new_results": [],
                "total_results": 0,
                "new_count": 0,
                "last_seen_listing_id": last_seen_listing_id,
                "bootstrap_last_seen_listing_id": None,
                "selected_last_seen_listing_id": None,
                "is_first_run": not bool(last_seen_listing_id),
                "structured_query": {},
                "pagination": {},
                "debug": {},
                "error": "empty_query",
            }

        response = await self.search_client.search(
            query=raw_query,
            page=1,
            limit=settings.ALERTS_MATCH_LIMIT_PER_SEARCH,
            include_answer=False,
        )

        items = list(response.get("results") or [])
        items = deduplicate_items(items)

        if settings.REDIS_URL:
            redis_client = None
            try:
                import redis.asyncio as redis_module

                redis_client = redis_module.from_url(
                    settings.REDIS_URL,
                    socket_timeout=getattr(settings, "REDIS_TIMEOUT_SEC", 2.0),
                    decode_responses=True,
                )
                deduplicator = RedisDeduplicator(redis_client=redis_client)
                redis_filtered: list[dict[str, Any]] = []

                for item in items:
                    if not await deduplicator.check_and_register(item):
                        redis_filtered.append(item)

                items = redis_filtered
            except Exception as exc:
                logger.warning(
                    "saved_search_redis_deduplication_skipped error=%s",
                    repr(exc),
                )
            finally:
                if redis_client is not None:
                    await redis_client.close()

        total_results = len(items)
        bootstrap_last_seen_listing_id = pick_last_seen_listing_id(items)
        is_first_run = not bool(last_seen_listing_id)

        if not items:
            new_items: list[dict[str, Any]] = []
            selected_last_seen_listing_id = last_seen_listing_id
        elif is_first_run:
            new_items = []
            selected_last_seen_listing_id = bootstrap_last_seen_listing_id
        else:
            new_items = filter_new_items(
                items=items,
                last_seen_listing_id=last_seen_listing_id,
            )
            selected_last_seen_listing_id = pick_last_seen_listing_id(items) or last_seen_listing_id

        max_new_items = int(getattr(settings, "ALERTS_MAX_NEW_ITEMS_PER_SEARCH", 20))
        if max_new_items > 0:
            new_items = new_items[:max_new_items]

        validated_items: list[dict[str, Any]] = []
        for raw_item in items:
            try:
                listing = ListingResult.model_validate(raw_item).ensure_listing_id()
                validated_items.append(listing.model_dump())
            except Exception as exc:
                logger.warning(
                    "saved_search_item_validation_failed error=%s raw_item=%s",
                    repr(exc),
                    raw_item,
                )

        items = validated_items

        logger.info(
            "saved_search_match saved_search_id=%s user_id=%s raw_query=%s total=%s new=%s first_run=%s last_seen=%s bootstrap=%s selected_last_seen=%s",
            saved_search_id,
            saved_search.get("user_id"),
            raw_query,
            total_results,
            len(new_items),
            is_first_run,
            last_seen_listing_id,
            bootstrap_last_seen_listing_id,
            selected_last_seen_listing_id,
        )

        return {
            "saved_search_id": saved_search_id,
            "user_id": saved_search.get("user_id"),
            "name": saved_search.get("name"),
            "raw_query": raw_query,
            "results": items,
            "new_results": new_items,
            "total_results": total_results,
            "new_count": len(new_items),
            "last_seen_listing_id": last_seen_listing_id,
            "bootstrap_last_seen_listing_id": bootstrap_last_seen_listing_id,
            "selected_last_seen_listing_id": selected_last_seen_listing_id,
            "is_first_run": is_first_run,
            "structured_query": response.get("structuredQuery") or response.get("structured_query") or {},
            "pagination": response.get("pagination") or {},
            "debug": response.get("debug") or {},
        }