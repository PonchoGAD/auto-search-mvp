from __future__ import annotations

import asyncio
from typing import Any

import httpx

from src.config import settings
from src.logging import get_logger


logger = get_logger(__name__)


class TelegramBotClient:
    NON_RETRYABLE_STATUS_CODES = {400, 401, 403, 404}

    def __init__(self) -> None:
        self.token = settings.TELEGRAM_BOT_TOKEN
        self.timeout = settings.TELEGRAM_API_TIMEOUT_SEC
        self.base_url = f"https://api.telegram.org/bot{self.token}"
        self.retries = max(1, int(getattr(settings, "TELEGRAM_API_RETRIES", 3)))
        self.retry_delay_sec = float(getattr(settings, "TELEGRAM_API_RETRY_DELAY_SEC", 1.0))

    def _safe_url(self, method: str) -> str:
        return f"https://api.telegram.org/bot***/{method}"

    def _extract_retry_after(self, response: httpx.Response) -> float | None:
        try:
            data = response.json()
            retry_after = data.get("parameters", {}).get("retry_after")
            if retry_after is not None:
                return float(retry_after)
        except Exception:
            return None

        return None

    async def _post(
        self,
        method: str,
        payload: dict[str, Any],
    ) -> dict[str, Any]:
        url = f"{self.base_url}/{method}"
        safe_url = self._safe_url(method)
        last_error: Exception | None = None

        async with httpx.AsyncClient(timeout=self.timeout) as client:
            for attempt in range(1, self.retries + 1):
                try:
                    response = await client.post(url, json=payload)

                    if response.status_code in self.NON_RETRYABLE_STATUS_CODES:
                        logger.warning(
                            "telegram_non_retryable_error method=%s url=%s status=%s body=%s",
                            method,
                            safe_url,
                            response.status_code,
                            response.text[:300],
                        )
                        response.raise_for_status()

                    if response.status_code == 429:
                        retry_after = self._extract_retry_after(response)
                        delay = retry_after if retry_after is not None else self.retry_delay_sec

                        logger.warning(
                            "telegram_rate_limited method=%s url=%s attempt=%s/%s retry_after=%s",
                            method,
                            safe_url,
                            attempt,
                            self.retries,
                            delay,
                        )

                        if attempt < self.retries:
                            await asyncio.sleep(delay)
                            continue

                        response.raise_for_status()

                    if response.status_code >= 500:
                        raise RuntimeError(
                            f"telegram server error {response.status_code}: {response.text[:300]}"
                        )

                    response.raise_for_status()
                    return response.json()

                except httpx.HTTPStatusError as exc:
                    last_error = exc

                    if exc.response.status_code in self.NON_RETRYABLE_STATUS_CODES:
                        raise

                    logger.warning(
                        "telegram_http_status_failed method=%s url=%s attempt=%s/%s status=%s error=%s",
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
                        "telegram_request_failed method=%s url=%s attempt=%s/%s error=%s",
                        method,
                        safe_url,
                        attempt,
                        self.retries,
                        repr(exc),
                    )

                    if attempt < self.retries:
                        await asyncio.sleep(self.retry_delay_sec)

        raise RuntimeError(
            f"telegram api unavailable after {self.retries} attempts: {last_error}"
        )

    async def send_message(
        self,
        chat_id: int,
        text: str,
        disable_web_page_preview: bool = True,
    ) -> dict[str, Any]:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": disable_web_page_preview,
        }

        return await self._post("sendMessage", payload)

    async def send_message_with_markup(
        self,
        chat_id: int,
        text: str,
        reply_markup: dict[str, Any],
        disable_web_page_preview: bool = True,
    ) -> dict[str, Any]:
        payload = {
            "chat_id": chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": disable_web_page_preview,
            "reply_markup": reply_markup,
        }

        return await self._post("sendMessage", payload)