from __future__ import annotations

import asyncio
from typing import Any

import httpx

from src.config import settings
from src.logging import get_logger


logger = get_logger(__name__)


class BotApiClient:
    NON_RETRYABLE_STATUS_CODES = {400, 401, 403, 404, 409, 422}

    def __init__(self) -> None:
        self.base_url = settings.bot_api_url.rstrip("/")
        self.timeout = settings.BOT_API_TIMEOUT_SEC
        self.headers = settings.bot_api_headers
        self.retries = max(1, int(getattr(settings, "BOT_API_RETRIES", 3)))
        self.retry_delay_sec = float(getattr(settings, "BOT_API_RETRY_DELAY_SEC", 1.0))

    def _safe_url(self, url: str) -> str:
        return url.split("?", 1)[0]

    async def _request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> httpx.Response:
        url = f"{self.base_url}/{path.lstrip('/')}"
        safe_url = self._safe_url(url)
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
                            "bot_api_non_retryable_error method=%s url=%s status=%s body=%s",
                            method,
                            safe_url,
                            response.status_code,
                            response.text[:300],
                        )
                        response.raise_for_status()

                    if response.status_code >= 500:
                        raise RuntimeError(
                            f"bot_api server error {response.status_code}: {response.text[:300]}"
                        )

                    response.raise_for_status()
                    return response

                except httpx.HTTPStatusError as exc:
                    last_error = exc

                    if exc.response.status_code in self.NON_RETRYABLE_STATUS_CODES:
                        raise

                    logger.warning(
                        "bot_api_http_status_failed method=%s url=%s attempt=%s/%s status=%s error=%s",
                        method,
                        safe_url,
                        attempt,
                        self.retries,
                        exc.response.status_code,
                        repr(exc),
                    )

                    if attempt < self.retries:
                        await asyncio.sleep(self.retry_delay_sec)

                except (httpx.TimeoutException, httpx.NetworkError, RuntimeError) as exc:
                    last_error = exc

                    logger.warning(
                        "bot_api_request_failed method=%s url=%s attempt=%s/%s error=%s",
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
                        "bot_api_unexpected_request_failed method=%s url=%s attempt=%s/%s error=%s",
                        method,
                        safe_url,
                        attempt,
                        self.retries,
                        repr(exc),
                    )

                    if attempt < self.retries:
                        await asyncio.sleep(self.retry_delay_sec)

        raise RuntimeError(
            f"bot_api unavailable after {self.retries} attempts: {last_error}"
        )

    async def get_user_by_telegram_id(self, telegram_user_id: int) -> dict[str, Any]:
        response = await self._request(
            "GET",
            "/internal/users/by-telegram-id",
            params={"telegram_user_id": telegram_user_id},
        )
        return response.json()

    async def get_usage_limits(self, telegram_user_id: int) -> dict[str, Any]:
        response = await self._request(
            "GET",
            "/internal/usage-limits",
            params={"telegram_user_id": telegram_user_id},
        )
        return response.json()

    async def list_active_saved_searches(self) -> list[dict[str, Any]]:
        response = await self._request(
            "GET",
            "/internal/saved-searches/active",
        )
        data = response.json()
        return list(data or [])

    async def mark_saved_search_checked(
        self,
        saved_search_id: int,
        last_seen_listing_id: str | None = None,
    ) -> dict[str, Any]:
        payload = {
            "last_seen_listing_id": last_seen_listing_id,
        }

        response = await self._request(
            "POST",
            f"/internal/saved-searches/{saved_search_id}/mark-checked",
            json=payload,
        )
        return response.json()

    async def list_pending_notifications(self, limit: int = 100) -> list[dict[str, Any]]:
        response = await self._request(
            "GET",
            "/internal/notifications/pending",
            params={"limit": limit},
        )
        data = response.json()
        return list(data or [])

    async def create_notification(
        self,
        user_id: int,
        type: str,
        payload: dict[str, Any] | None = None,
        dedup_key: str | None = None,
        status: str = "pending",
    ) -> dict[str, Any]:
        body = {
            "user_id": user_id,
            "type": type,
            "payload": payload or {},
            "dedup_key": dedup_key,
            "status": status,
        }

        try:
            response = await self._request(
                "POST",
                "/internal/notifications",
                json=body,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 409:
                return {
                    "status": "duplicate",
                    "dedup_key": dedup_key,
                }
            raise

        data = response.json()
        message = str(data.get("message") or "")

        notification_id: int | None = None
        if "Notification created:" in message:
            try:
                notification_id = int(message.rsplit(":", 1)[1].strip())
            except Exception:
                notification_id = None

        if notification_id is not None:
            data["notification_id"] = notification_id

        return data

    async def mark_notification_sent(self, notification_id: int) -> dict[str, Any]:
        response = await self._request(
            "POST",
            f"/internal/notifications/{notification_id}/mark-sent",
        )
        return response.json()

    async def mark_notification_failed(
        self,
        notification_id: int,
        error_message: str | None = None,
    ) -> dict[str, Any]:
        payload = {
            "error_message": error_message,
        }

        response = await self._request(
            "POST",
            f"/internal/notifications/{notification_id}/mark-failed",
            json=payload,
        )
        return response.json()

    async def expire_overdue_subscriptions(self) -> dict[str, Any]:
        response = await self._request(
            "POST",
            "/internal/subscriptions/expire-overdue",
        )
        return response.json()

    async def create_search_history(
        self,
        telegram_user_id: int,
        raw_query: str,
        query_payload: dict[str, Any] | None = None,
        results_count: int = 0,
        latency_ms: int | None = None,
        empty_result: bool = False,
    ) -> dict[str, Any]:
        payload = {
            "telegram_user_id": telegram_user_id,
            "raw_query": raw_query,
            "query_payload": query_payload or {},
            "results_count": results_count,
            "latency_ms": latency_ms,
            "empty_result": empty_result,
        }

        response = await self._request(
            "POST",
            "/internal/search-history",
            json=payload,
        )
        return response.json()

    async def get_system_status(self) -> dict[str, Any]:
        response = await self._request(
            "GET",
            "/internal/admin/system-status",
        )
        return response.json()

    async def get_admin_user_stats(self) -> dict[str, Any]:
        response = await self._request("GET", "/internal/admin/user-stats")
        return response.json()

    async def get_admin_search_stats(self) -> dict[str, Any]:
        response = await self._request("GET", "/internal/admin/search-stats")
        return response.json()

    async def get_admin_favorites_stats(self) -> dict[str, Any]:
        response = await self._request("GET", "/internal/admin/favorites-stats")
        return response.json()

    async def get_admin_saved_searches_stats(self) -> dict[str, Any]:
        response = await self._request("GET", "/internal/admin/saved-searches-stats")
        return response.json()

    async def get_admin_revenue_stats(self) -> dict[str, Any]:
        response = await self._request("GET", "/internal/admin/revenue-stats")
        return response.json()

    async def get_admin_subscription_stats(self) -> dict[str, Any]:
        response = await self._request("GET", "/internal/admin/subscription-stats")
        return response.json()

    async def get_admin_payment_logs(self, limit: int = 50) -> list[dict[str, Any]]:
        response = await self._request(
            "GET",
            "/internal/admin/payment-logs",
            params={"limit": limit},
        )
        return list(response.json() or [])

    async def get_admin_notification_logs(self, limit: int = 50) -> list[dict[str, Any]]:
        response = await self._request(
            "GET",
            "/internal/admin/notification-logs",
            params={"limit": limit},
        )
        return list(response.json() or [])

    async def get_admin_error_logs(self, limit: int = 50) -> list[dict[str, Any]]:
        response = await self._request(
            "GET",
            "/internal/admin/error-logs",
            params={"limit": limit},
        )
        return list(response.json() or [])

    async def get_latest_searches(self, limit: int = 50) -> list[dict[str, Any]]:
        response = await self._request(
            "GET",
            "/internal/admin/latest-searches",
            params={"limit": limit},
        )
        return list(response.json() or [])

    async def get_latest_saved_searches(self, limit: int = 50) -> list[dict[str, Any]]:
        response = await self._request(
            "GET",
            "/internal/admin/latest-saved-searches",
            params={"limit": limit},
        )
        return list(response.json() or [])

    async def run_admin_alerts(self) -> dict[str, Any]:
        response = await self._request("POST", "/internal/admin/run-alerts")
        return response.json()