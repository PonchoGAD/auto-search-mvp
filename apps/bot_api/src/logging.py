from __future__ import annotations

import json
import logging
import re
import sys
from datetime import datetime, timezone
from typing import Any


SENSITIVE_KEYS = {
    "token",
    "api_key",
    "apikey",
    "key",
    "secret",
    "password",
    "passwd",
    "authorization",
    "internal_api_key",
    "x_internal_api_key",
    "x-internal-api-key",
    "webhook_secret",
    "database_url",
    "db_url",
    "dsn",
}


SENSITIVE_PATTERNS = (
    re.compile(r"(bot)[0-9]{8,}:[A-Za-z0-9_-]{20,}", re.IGNORECASE),
    re.compile(r"(Bearer\s+)[A-Za-z0-9._~+/=-]+", re.IGNORECASE),
    re.compile(r"(://[^:/\s]+:)[^@\s]+(@)", re.IGNORECASE),
    re.compile(r"([?&](?:token|api_key|key|secret|password)=)[^&\s]+", re.IGNORECASE),
)


RESERVED_LOG_RECORD_ATTRS = {
    "args",
    "asctime",
    "created",
    "exc_info",
    "exc_text",
    "filename",
    "funcName",
    "levelname",
    "levelno",
    "lineno",
    "module",
    "msecs",
    "message",
    "msg",
    "name",
    "pathname",
    "process",
    "processName",
    "relativeCreated",
    "stack_info",
    "thread",
    "threadName",
}


def _is_sensitive_key(key: str) -> bool:
    normalized = str(key or "").lower().replace("-", "_")
    return normalized in SENSITIVE_KEYS or any(part in normalized for part in SENSITIVE_KEYS)


def _mask_string(value: str) -> str:
    result = value

    for pattern in SENSITIVE_PATTERNS:
        if pattern.pattern.startswith("(://"):
            result = pattern.sub(r"\1***\2", result)
        elif "Bearer" in pattern.pattern:
            result = pattern.sub(r"\1***", result)
        elif "token|api_key" in pattern.pattern:
            result = pattern.sub(r"\1***", result)
        else:
            result = pattern.sub(r"\1***", result)

    return result


def _sanitize(value: Any, key: str | None = None) -> Any:
    if key and _is_sensitive_key(key):
        return "***"

    if isinstance(value, str):
        return _mask_string(value)

    if isinstance(value, dict):
        return {
            str(item_key): _sanitize(item_value, str(item_key))
            for item_key, item_value in value.items()
        }

    if isinstance(value, (list, tuple, set)):
        return [_sanitize(item) for item in value]

    return value


class JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": _sanitize(record.getMessage()),
        }

        request_id = getattr(record, "request_id", None)
        if request_id:
            payload["request_id"] = _sanitize(request_id)

        correlation_id = getattr(record, "correlation_id", None)
        if correlation_id:
            payload["correlation_id"] = _sanitize(correlation_id)

        for key, value in record.__dict__.items():
            if key in RESERVED_LOG_RECORD_ATTRS:
                continue

            if key in {"request_id", "correlation_id"}:
                continue

            if key.startswith("_"):
                continue

            payload[key] = _sanitize(value, key)

        if record.exc_info:
            payload["exc_info"] = _sanitize(self.formatException(record.exc_info))

        if record.stack_info:
            payload["stack_info"] = _sanitize(self.formatStack(record.stack_info))

        return json.dumps(payload, ensure_ascii=False, default=str)


def _resolve_level(level: int | str = logging.INFO) -> int:
    if isinstance(level, int):
        return level

    value = str(level).upper().strip()
    return getattr(logging, value, logging.INFO)


def setup_logging(level: int | str = logging.INFO) -> None:
    resolved_level = _resolve_level(level)
    root_logger = logging.getLogger()

    formatter = JsonFormatter()

    if root_logger.handlers:
        root_logger.setLevel(resolved_level)
        for handler in root_logger.handlers:
            handler.setLevel(resolved_level)
            handler.setFormatter(formatter)
        return

    handler = logging.StreamHandler(sys.stdout)
    handler.setLevel(resolved_level)
    handler.setFormatter(formatter)

    root_logger.setLevel(resolved_level)
    root_logger.addHandler(handler)

    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)
    logging.getLogger("uvicorn.access").setLevel(logging.INFO)


def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)