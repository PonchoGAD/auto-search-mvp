from __future__ import annotations

import asyncio
import logging
from typing import Any

import httpx

from src.config import settings


logger = logging.getLogger(__name__)


class SearchApiClient:
    NON_RETRYABLE_STATUS_CODES = {400, 401, 403, 404, 422}

    def __init__(self) -> None:
        self.base_url = settings.search_api_url.rstrip("/")
        self.timeout = settings.SEARCH_API_TIMEOUT_SEC
        self.headers = settings.search_api_headers
        self.retries = max(1, int(getattr(settings, "SEARCH_API_RETRIES", 3)))
        self.retry_delay_sec = float(getattr(settings, "SEARCH_API_RETRY_DELAY_SEC", 1.0))

    def _safe_log_url(self, url: str) -> str:
        return url.split("?", 1)[0]

    async def _request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> httpx.Response:
        url = f"{self.base_url}/{path.lstrip('/')}"
        safe_url = self._safe_log_url(url)
        last_error: Exception | None = None

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for attempt in range(1, self.retries + 1):
                try:
                    response = await client.request(
                        method=method,
                        url=url,
                        headers=self.headers,
                        **kwargs,
                    )

                    if response.status_code in self.NON_RETRYABLE_STATUS_CODES:
                        logger.warning(
                            "worker_search_core_non_retryable_error method=%s url=%s status=%s body=%s",
                            method,
                            safe_url,
                            response.status_code,
                            response.text[:300],
                        )
                        response.raise_for_status()

                    if response.status_code >= 500:
                        raise RuntimeError(
                            f"search core server error {response.status_code}: {response.text[:300]}"
                        )

                    response.raise_for_status()
                    return response

                except httpx.HTTPStatusError as exc:
                    last_error = exc

                    status_code = exc.response.status_code
                    if status_code in self.NON_RETRYABLE_STATUS_CODES:
                        raise RuntimeError(
                            f"search core client error {status_code}: {exc.response.text[:300]}"
                        ) from exc

                    logger.warning(
                        "worker_search_core_http_status_failed method=%s url=%s attempt=%s/%s status=%s error=%s",
                        method,
                        safe_url,
                        attempt,
                        self.retries,
                        status_code,
                        repr(exc),
                    )

                    if attempt < self.retries:
                        await asyncio.sleep(self.retry_delay_sec)

                except (httpx.TimeoutException, httpx.NetworkError, RuntimeError) as exc:
                    last_error = exc

                    logger.warning(
                        "worker_search_core_request_failed method=%s url=%s attempt=%s/%s error=%s",
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
                        "worker_search_core_unexpected_request_failed method=%s url=%s attempt=%s/%s error=%s",
                        method,
                        safe_url,
                        attempt,
                        self.retries,
                        repr(exc),
                    )

                    if attempt < self.retries:
                        await asyncio.sleep(self.retry_delay_sec)

        raise RuntimeError(
            f"search core unavailable after {self.retries} attempts: {last_error}"
        )

    async def search(
        self,
        query: str,
        page: int = 1,
        limit: int = 10,
        include_answer: bool = False,
    ) -> dict[str, Any]:
        payload = {
            "query": query,
            "page": page,
            "limit": limit,
            "include_answer": include_answer,
        }

        response = await self._request("POST", "search", json=payload)
        return response.json()

    async def get_listing(self, listing_id: str) -> dict[str, Any]:
        response = await self._request("GET", f"listings/{listing_id}")
        return response.json()

    async def health(self) -> dict[str, Any]:
        response = await self._request("GET", "health")
        return response.json()