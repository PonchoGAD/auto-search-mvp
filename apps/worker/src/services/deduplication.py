from __future__ import annotations

import hashlib
import logging
from typing import Any
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit


logger = logging.getLogger(__name__)


class RedisDeduplicator:
    def __init__(self, redis_client: Any = None) -> None:
        self.redis = redis_client

    async def check_and_register(self, listing_data: dict) -> bool:
        try:
            if self.redis is None:
                raise RuntimeError("redis client is not configured")

            brand = str(listing_data.get("brand") or "").strip()
            model = str(listing_data.get("model") or "").strip()
            price = str(listing_data.get("price") or "").strip()
            year = str(listing_data.get("year") or "").strip()
            mileage = str(listing_data.get("mileage") or "").strip()

            hash_base = f"{brand}_{model}_{price}_{year}_{mileage}".lower().replace(" ", "")
            hash_key = hashlib.sha1(hash_base.encode("utf-8")).hexdigest()
            redis_key = f"dedup:{hash_key}"

            exists = await self.redis.exists(redis_key)
            if exists:
                return True

            await self.redis.set(redis_key, "1", ex=259200)
            return False

        except (ConnectionError, TimeoutError) as exc:
            logger.error("redis_deduplication_unavailable error=%s", repr(exc))
            return False
        except Exception as exc:
            logger.error("redis_deduplication_failed error=%s", repr(exc))
            return False


FINGERPRINT_FIELDS = (
    "listing_id",
    "source_url",
    "brand",
    "model",
    "year",
    "price",
    "mileage",
)


TRACKING_QUERY_PREFIXES = (
    "utm_",
)

TRACKING_QUERY_KEYS = {
    "yclid",
    "gclid",
    "fbclid",
    "from",
    "ref",
}


def _clean(value: Any) -> str:
    return str(value or "").strip().lower()


def normalize_source_url(value: Any) -> str:
    raw = str(value or "").strip()

    if not raw:
        return ""

    try:
        parts = urlsplit(raw)
        scheme = parts.scheme.lower()
        netloc = parts.netloc.lower()
        path = parts.path.rstrip("/")

        query_pairs = []

        for key, val in parse_qsl(parts.query, keep_blank_values=True):
            key_lower = key.lower()
            if key_lower in TRACKING_QUERY_KEYS:
                continue
            if any(key_lower.startswith(prefix) for prefix in TRACKING_QUERY_PREFIXES):
                continue
            query_pairs.append((key, val))

        query = urlencode(query_pairs, doseq=True)

        return urlunsplit((scheme, netloc, path, query, ""))
    except Exception:
        return raw.lower().rstrip("/")


def build_listing_fingerprint(item: dict[str, Any]) -> str:
    source_url = normalize_source_url(item.get("source_url") or item.get("url"))

    raw = "|".join(
        [
            _clean(item.get("listing_id") or item.get("id")),
            source_url,
            _clean(item.get("brand")),
            _clean(item.get("model")),
            _clean(item.get("year")),
            _clean(item.get("price")),
            _clean(item.get("mileage")),
        ]
    )
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def normalize_listing_id(item: dict[str, Any]) -> str:
    listing_id = str(item.get("listing_id") or item.get("id") or "").strip()

    if listing_id:
        item["listing_id"] = listing_id
        return listing_id

    source_url = normalize_source_url(item.get("source_url") or item.get("url"))
    if source_url:
        item["source_url"] = source_url
        listing_id = hashlib.sha1(source_url.encode("utf-8")).hexdigest()[:24]
        item["listing_id"] = listing_id
        return listing_id

    fingerprint = build_listing_fingerprint(item)
    item["listing_id"] = fingerprint[:24]
    return item["listing_id"]


def build_saved_search_dedup_key(
    saved_search_id: Any,
    listing_id: Any,
) -> str:
    raw = f"saved_search_alert:{_clean(saved_search_id)}:{_clean(listing_id)}"
    return hashlib.sha1(raw.encode("utf-8")).hexdigest()


def deduplicate_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen: set[str] = set()
    result: list[dict[str, Any]] = []

    for raw_item in items:
        item = dict(raw_item)
        listing_id = normalize_listing_id(item)
        source_url = normalize_source_url(item.get("source_url") or item.get("url"))

        if source_url:
            item["source_url"] = source_url

        fingerprint = source_url or listing_id or build_listing_fingerprint(item)

        if fingerprint in seen:
            continue

        seen.add(fingerprint)
        result.append(item)

    return result


def filter_new_items(
    items: list[dict[str, Any]],
    last_seen_listing_id: str | None = None,
) -> list[dict[str, Any]]:
    if not items:
        return []

    clean_last_seen = str(last_seen_listing_id or "").strip()

    if not clean_last_seen:
        return []

    result: list[dict[str, Any]] = []

    for raw_item in items:
        item = dict(raw_item)
        listing_id = normalize_listing_id(item)

        if listing_id == clean_last_seen:
            break

        result.append(item)

    return result


def pick_last_seen_listing_id(items: list[dict[str, Any]]) -> str | None:
    if not items:
        return None

    first = dict(items[0])
    return normalize_listing_id(first)


def listing_ids(items: list[dict[str, Any]]) -> list[str]:
    ids: list[str] = []

    for raw_item in items:
        item = dict(raw_item)
        listing_id = normalize_listing_id(item)

        if listing_id:
            ids.append(listing_id)

    return ids