from __future__ import annotations

import logging

from src.clients.search_api import SearchApiClient
from src.schemas.listing import ListingResult
from src.schemas.search import SearchRequest, SearchResponse
from src.services.search_cache import SearchCache

logger = logging.getLogger(__name__)


class SearchGatewayService:
    def __init__(self) -> None:
        self.search_client = SearchApiClient()
        self.cache = SearchCache()

    async def search(self, payload: SearchRequest) -> SearchResponse:
        cache_key = self.cache.make_cache_key(
            query=payload.query,
            page=payload.page,
            limit=payload.limit,
            include_answer=payload.include_answer,
        )

        cached_response = await self.cache.get(cache_key)
        if cached_response is not None:
            logger.info(
                "search_gateway_cache_hit query=%s page=%s limit=%s",
                payload.query,
                payload.page,
                payload.limit,
            )
            return SearchResponse.model_validate(cached_response)

        raw_response = await self.search_client.search(payload)
        raw_items = list(raw_response.results or [])
        normalized_items: list[dict[str, object]] = []

        for raw_item in raw_items:
            try:
                listing = ListingResult.model_validate(raw_item)
                listing.ensure_listing_id()
                normalized_items.append(listing.model_dump())
            except Exception as exc:
                logger.warning(
                    "search_gateway_item_validation_failed error=%s raw_item=%s",
                    repr(exc),
                    raw_item,
                )

        response_payload = raw_response.model_dump()
        response_payload["results"] = normalized_items
        response = SearchResponse.model_validate(response_payload)
        await self.cache.set(cache_key, response.model_dump())
        return response
