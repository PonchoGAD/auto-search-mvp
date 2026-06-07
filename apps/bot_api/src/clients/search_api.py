from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from src.config import settings
from src.common.result_mapper import ResultMapper
from src.schemas.common import PaginationMeta
from src.schemas.search import (
    ListingDetailsResponse,
    SearchDebugInfo,
    SearchRequest,
    SearchResponse,
    SearchResultItem,
)


logger = logging.getLogger(__name__)


class SearchCoreUnavailableError(RuntimeError):
    pass


class SearchCoreClientError(RuntimeError):
    pass


class SearchApiClient:
    NON_RETRYABLE_STATUS_CODES = {400, 401, 403, 404, 422}

    def __init__(self) -> None:
        self.base_url = settings.SEARCH_API_BASE_URL.rstrip("/")
        self.search_prefix = settings.SEARCH_API_PREFIX.rstrip("/")
        self.timeout = settings.SEARCH_API_TIMEOUT_SEC
        self.api_key = settings.SEARCH_API_KEY
        self.retries = max(1, int(settings.SEARCH_API_RETRIES))
        self.retry_delay_sec = settings.SEARCH_API_RETRY_DELAY_SEC

    def _headers(self) -> dict[str, str]:
        headers = {"Content-Type": "application/json"}

        if self.api_key:
            headers["X-API-Key"] = self.api_key

        return headers

    def _url(self, path: str) -> str:
        return f"{self.base_url}{self.search_prefix}/{path.lstrip('/')}"

    def _safe_log_url(self, url: str) -> str:
        return url.split("?", 1)[0]

    async def _request(
        self,
        method: str,
        url: str,
        **kwargs: Any,
    ) -> httpx.Response:
        last_error: Exception | None = None
        safe_url = self._safe_log_url(url)

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for attempt in range(1, self.retries + 1):
                try:
                    response = await client.request(
                        method=method,
                        url=url,
                        headers=self._headers(),
                        **kwargs,
                    )

                    if response.status_code in self.NON_RETRYABLE_STATUS_CODES:
                        logger.warning(
                            "search_core_non_retryable_error method=%s url=%s status=%s body=%s",
                            method,
                            safe_url,
                            response.status_code,
                            response.text[:300],
                        )
                        response.raise_for_status()

                    if response.status_code >= 500:
                        raise SearchCoreUnavailableError(
                            f"search core server error {response.status_code}: {response.text[:300]}"
                        )

                    response.raise_for_status()
                    return response

                except httpx.HTTPStatusError as exc:
                    last_error = exc

                    status_code = exc.response.status_code
                    if status_code in self.NON_RETRYABLE_STATUS_CODES:
                        raise SearchCoreClientError(
                            f"search core client error {status_code}: {exc.response.text[:300]}"
                        ) from exc

                    logger.warning(
                        "search_core_http_status_failed method=%s url=%s attempt=%s/%s status=%s error=%s",
                        method,
                        safe_url,
                        attempt,
                        self.retries,
                        status_code,
                        repr(exc),
                    )

                    if attempt < self.retries:
                        await asyncio.sleep(self.retry_delay_sec)

                except (httpx.TimeoutException, httpx.NetworkError, SearchCoreUnavailableError) as exc:
                    last_error = exc

                    logger.warning(
                        "search_core_request_failed method=%s url=%s attempt=%s/%s error=%s",
                        method,
                        safe_url,
                        attempt,
                        self.retries,
                        repr(exc),
                    )

                    if attempt < self.retries:
                        await asyncio.sleep(self.retry_delay_sec)

                except Exception as exc:
                    last_error = exc

                    logger.warning(
                        "search_core_unexpected_request_failed method=%s url=%s attempt=%s/%s error=%s",
                        method,
                        safe_url,
                        attempt,
                        self.retries,
                        repr(exc),
                    )

                    if attempt < self.retries:
                        await asyncio.sleep(self.retry_delay_sec)

        raise SearchCoreUnavailableError(
            f"search core unavailable after {self.retries} attempts: {last_error}"
        )

    def _build_search_payload(self, request: SearchRequest) -> dict[str, Any]:
        return {
            "query": request.query,
            "page": request.page,
            "limit": request.limit,
            "include_answer": request.include_answer,
        }

    def _normalize_results(self, items: list[dict[str, Any]]) -> list[SearchResultItem]:
        normalized: list[SearchResultItem] = []

        for raw in items:
            try:
                mapped = ResultMapper.map_to_listing_result(raw)
                item = SearchResultItem(**mapped).ensure_listing_id()
                normalized.append(item)
            except Exception as exc:
                logger.warning(
                    "search_result_normalization_failed raw=%s error=%s",
                    getattr(raw, "get", lambda k, d=None: str(raw))("id", raw),
                    repr(exc),
                )

        return normalized

    def _build_response(
        self,
        raw_data: dict[str, Any],
        page: int,
        limit: int,
    ) -> SearchResponse:
        raw_results = raw_data.get("results") or []
        items = self._normalize_results(raw_results)

        debug_raw = raw_data.get("debug") or {}

        debug = SearchDebugInfo(
            latency_ms=int(debug_raw.get("latency_ms", 0) or 0),
            vector_hits=int(debug_raw.get("vector_hits", 0) or 0),
            final_results=int(debug_raw.get("final_results", len(items)) or len(items)),
            query_language=str(debug_raw.get("query_language", "ru") or "ru"),
            empty_result=bool(debug_raw.get("empty_result", len(items) == 0)),
        )

        total = raw_data.get("total")
        pagination_raw = raw_data.get("pagination") or {}

        pagination = PaginationMeta(
            page=int(pagination_raw.get("page") or page),
            limit=int(pagination_raw.get("limit") or limit),
            total=total if isinstance(total, int) else pagination_raw.get("total"),
            has_more=bool(pagination_raw.get("has_more", len(items) >= limit)),
        )

        return SearchResponse(
            structuredQuery=raw_data.get("structuredQuery") or raw_data.get("structured_query") or {},
            results=items,
            answer=raw_data.get("answer"),
            debug=debug,
            pagination=pagination,
        )

    async def health(self) -> dict[str, Any]:
        url = self._url("health")

        logger.info("search_core_health_start url=%s", self._safe_log_url(url))

        response = await self._request("GET", url)
        return response.json()

    async def search(self, request: SearchRequest) -> SearchResponse:
        url = self._url("search")
        payload = self._build_search_payload(request)

        logger.info(
            "search_core_search_start url=%s query=%s page=%s limit=%s",
            self._safe_log_url(url),
            request.query,
            request.page,
            request.limit,
        )

        response = await self._request("POST", url, json=payload)
        data = response.json()

        result = self._build_response(
            raw_data=data,
            page=request.page,
            limit=request.limit,
        )

        logger.info(
            "search_core_search_done query=%s results=%s empty=%s",
            request.query,
            len(result.results),
            result.debug.empty_result,
        )

        return result

    async def get_listing(self, listing_id: str) -> ListingDetailsResponse:
        url = self._url(f"listings/{listing_id}")

        logger.info(
            "search_core_listing_start url=%s listing_id=%s",
            self._safe_log_url(url),
            listing_id,
        )

        response = await self._request("GET", url)
        data = response.json()

        return ListingDetailsResponse(**data)

    async def raw_search(self, query: str, include_answer: bool = False) -> dict[str, Any]:
        url = self._url("search")

        payload = {
            "query": query,
            "include_answer": include_answer,
        }

        response = await self._request("POST", url, json=payload)
        return response.json()