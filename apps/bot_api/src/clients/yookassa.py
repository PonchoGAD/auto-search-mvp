from __future__ import annotations

import base64
from decimal import Decimal
from typing import Optional

import httpx


YOOKASSA_BASE_URL = "https://api.yookassa.ru/v3"


class YooKassaClient:
    """Sync HTTP client for YooKassa REST API v3."""

    def __init__(self, shop_id: str, secret_key: str, timeout: float = 15.0) -> None:
        self._shop_id = shop_id
        self._secret_key = secret_key
        self._timeout = timeout
        self._auth = (shop_id, secret_key)

    def create_payment(
        self,
        *,
        amount: Decimal,
        currency: str,
        description: str,
        idempotency_key: str,
        return_url: str,
        metadata: Optional[dict] = None,
    ) -> dict:
        """
        POST /payments — creates a redirect payment.
        Returns the full YooKassa payment object including confirmation.confirmation_url.
        """
        payload: dict = {
            "amount": {"value": f"{amount:.2f}", "currency": currency.upper()},
            "confirmation": {"type": "redirect", "return_url": return_url},
            "description": description[:128],
            "capture": True,
        }
        if metadata:
            payload["metadata"] = {str(k): str(v) for k, v in metadata.items()}

        with httpx.Client(timeout=self._timeout) as client:
            response = client.post(
                f"{YOOKASSA_BASE_URL}/payments",
                json=payload,
                auth=self._auth,
                headers={"Idempotency-Key": idempotency_key},
            )
            response.raise_for_status()
            return response.json()

    def verify_basic_auth(self, authorization_header: str) -> bool:
        """
        YooKassa sends webhook with Basic Auth header (shop_id:secret_key).
        Returns True if the header matches our credentials.
        """
        if not authorization_header.startswith("Basic "):
            return False
        try:
            decoded = base64.b64decode(authorization_header[6:]).decode("utf-8")
            shop_id, secret_key = decoded.split(":", 1)
            return shop_id == self._shop_id and secret_key == self._secret_key
        except Exception:
            return False
