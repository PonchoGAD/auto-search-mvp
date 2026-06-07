from __future__ import annotations

from datetime import datetime
from decimal import Decimal
from typing import Optional

from pydantic import Field

from src.schemas.common import BaseSchema


class PaymentCreateRequest(BaseSchema):
    telegram_user_id: int

    amount: Decimal = Field(gt=0)
    currency: str = Field(default="RUB", min_length=3, max_length=8)

    provider: str = Field(default="stub", min_length=1, max_length=32)
    description: Optional[str] = Field(default=None, max_length=512)

    plan: Optional[str] = Field(default=None, max_length=16)

    return_url: Optional[str] = None
    success_url: Optional[str] = None
    fail_url: Optional[str] = None

    payload: Optional[dict] = None
    idempotency_key: Optional[str] = Field(default=None, max_length=128)


class PaymentCreateResponse(BaseSchema):
    id: int
    provider: str
    status: str

    amount: Decimal
    currency: str

    external_payment_id: Optional[str] = None
    idempotency_key: Optional[str] = None

    payment_url: Optional[str] = None
    invoice_url: Optional[str] = None

    return_url: Optional[str] = None
    success_url: Optional[str] = None
    fail_url: Optional[str] = None

    created_at: datetime


class PaymentResponse(BaseSchema):
    id: int
    user_id: int

    provider: str
    status: str

    amount: Decimal
    currency: str

    external_payment_id: Optional[str] = None
    idempotency_key: Optional[str] = None

    payment_url: Optional[str] = None
    invoice_url: Optional[str] = None

    return_url: Optional[str] = None
    success_url: Optional[str] = None
    fail_url: Optional[str] = None

    description: Optional[str] = None

    payload: Optional[dict] = None
    paid_at: Optional[datetime] = None

    created_at: datetime
    updated_at: datetime


class PaymentStatusResponse(BaseSchema):
    id: int
    provider: str
    status: str

    amount: Decimal
    currency: str

    external_payment_id: Optional[str] = None
    idempotency_key: Optional[str] = None

    payment_url: Optional[str] = None
    invoice_url: Optional[str] = None

    paid_at: Optional[datetime] = None
    created_at: datetime
    updated_at: datetime


class PaymentWebhookRequest(BaseSchema):
    provider: str = Field(min_length=1, max_length=32)

    external_payment_id: str = Field(min_length=1, max_length=128)

    status: str = Field(min_length=1, max_length=16)

    amount: Optional[Decimal] = None
    currency: Optional[str] = Field(default=None, min_length=3, max_length=8)

    payload: Optional[dict] = None
    raw_payload: Optional[dict] = None
    metadata: Optional[dict] = None

    signature: Optional[str] = Field(default=None, max_length=512)
    event_id: Optional[str] = Field(default=None, max_length=128)

    webhook_type: Optional[str] = Field(default=None, max_length=64)
    event_type: Optional[str] = Field(default=None, max_length=64)

    paid_at: Optional[datetime] = None