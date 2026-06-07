from __future__ import annotations

from datetime import datetime
from typing import Any, Generic, Optional, TypeVar

from pydantic import BaseModel, ConfigDict, Field


T = TypeVar("T")


class BaseSchema(BaseModel):
    model_config = ConfigDict(
        from_attributes=True,
        populate_by_name=True,
        extra="ignore",
    )


class HealthResponse(BaseSchema):
    status: str = "ok"
    service: str
    version: str = "1.0.0"


class ErrorDetail(BaseSchema):
    code: str
    message: str
    details: Optional[dict[str, Any]] = None


class ErrorResponse(BaseSchema):
    error: ErrorDetail


class PaginationMeta(BaseSchema):
    page: int = Field(default=1, ge=1)
    limit: int = Field(default=10, ge=1)
    total: Optional[int] = Field(default=None, ge=0)
    has_more: bool = False


class PaginatedResponse(BaseSchema, Generic[T]):
    items: list[T]
    pagination: PaginationMeta


class MessageResponse(BaseSchema):
    message: str


class TimestampedResponse(BaseSchema):
    created_at: datetime
    updated_at: datetime