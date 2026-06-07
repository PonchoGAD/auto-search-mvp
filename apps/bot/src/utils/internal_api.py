from __future__ import annotations

from typing import Dict

from src.config import settings

INTERNAL_API_HEADER_NAME = "X-INTERNAL-KEY"
CONTENT_TYPE_HEADER_NAME = "Content-Type"
CONTENT_TYPE_JSON = "application/json"


def bot_api_headers() -> dict[str, str]:
    headers: dict[str, str] = {CONTENT_TYPE_HEADER_NAME: CONTENT_TYPE_JSON}
    internal_api_key = str(settings.INTERNAL_API_KEY or "").strip()

    if internal_api_key:
        headers[INTERNAL_API_HEADER_NAME] = internal_api_key

    return headers


def empty_headers() -> dict[str, str]:
    return {}
