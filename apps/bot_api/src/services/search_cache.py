from __future__ import annotations

import hashlib
import importlib
import json
from typing import Any

from src.config import settings


class SearchCache:
    def __init__(self) -> None:
        self.redis_url = settings.REDIS_URL
        self.timeout = settings.REDIS_TIMEOUT_SEC

    @staticmethod
    def make_cache_key(query: str, page: int, limit: int, include_answer: bool) -> str:
        raw = f"search:{query}|page={page}|limit={limit}|answer={include_answer}"
        return hashlib.sha1(raw.encode("utf-8")).hexdigest()

    async def _create_client(self) -> Any | None:
        if not self.redis_url:
            return None

        try:
            redis_module = importlib.import_module("redis.asyncio")
            return redis_module.from_url(
                self.redis_url,
                socket_timeout=self.timeout,
                decode_responses=True,
            )
        except ImportError:
            return None
        except Exception:
            return None

    async def get(self, cache_key: str) -> dict[str, Any] | None:
        client = await self._create_client()
        if client is None:
            return None

        try:
            raw = await client.get(cache_key)
            if not raw:
                return None
            return json.loads(raw)
        except Exception:
            return None
        finally:
            await client.close()

    async def set(self, cache_key: str, payload: dict[str, Any], ttl: int = 60) -> None:
        client = await self._create_client()
        if client is None:
            return None

        try:
            await client.set(cache_key, json.dumps(payload, default=str), ex=ttl)
        except Exception:
            return None
        finally:
            await client.close()
