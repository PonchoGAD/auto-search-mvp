import os
import random
import asyncio
from typing import List, Dict

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Message

# В зависимости от вашей структуры, импорт может быть из data_pipeline.telegram_filters или utils.telegram_filters
from utils.telegram_filters import is_valid_telegram_post


# =========================
# SAFE ENV HELPERS
# =========================

def _get_int_env(name: str, default: int = 0) -> int:
    value = os.getenv(name)
    if not value:
        return default
    try:
        return int(value)
    except ValueError:
        return default


# =========================
# HELPERS
# =========================

def _load_channels_from_env() -> List[str]:
    raw = os.getenv("TG_CHANNELS", "")
    out = []
    for c in raw.split(","):
        c = c.strip()
        if not c:
            continue
        if "t.me/" in c:
            c = c.split("t.me/")[-1].strip("/")
        c = "@" + c.lstrip("@")
        out.append(c)
    return out


def _load_channels_from_bot_api() -> List[str]:
    """Fetch active channels stored in bot-api DB via internal HTTP call."""
    bot_api_url = os.getenv("BOT_API_INTERNAL_URL", "").rstrip("/")
    internal_key = os.getenv("BOT_API_INTERNAL_KEY", "")
    if not bot_api_url or not internal_key:
        return []

    try:
        import urllib.request
        import json as _json
        req = urllib.request.Request(
            f"{bot_api_url}/api/v1/internal/admin/tg-channels",
            headers={"X-Internal-Api-Key": internal_key},
        )
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = _json.load(resp)
        channels = [f"@{ch['username']}" for ch in data if ch.get("is_active")]
        if channels:
            print(f"[TELEGRAM] loaded {len(channels)} channels from bot-api: {channels}", flush=True)
        return channels
    except Exception as e:
        print(f"[TELEGRAM][WARN] bot-api channel fetch failed ({e}), falling back to TG_CHANNELS env", flush=True)
        return []


def load_channels() -> List[str]:
    """
    Загружает список Telegram-каналов: сначала пробует bot-api DB,
    при недоступности — ENV переменную TG_CHANNELS.
    """
    api_channels = _load_channels_from_bot_api()
    if api_channels:
        return api_channels

    env_channels = _load_channels_from_env()
    if env_channels:
        print(f"[TELEGRAM] loaded {len(env_channels)} channels from TG_CHANNELS env", flush=True)
    else:
        print("[TELEGRAM][WARN] no channels found in bot-api or TG_CHANNELS env", flush=True)
    return env_channels


async def _fetch_from_channel(
    client: TelegramClient,
    channel: str,
    limit: int,
) -> List[Dict]:
    """
    Fetch + минимальный anti-noise на уровне источника.
    Бизнес-логика фильтрации остаётся в ingest_quality.
    """

    items: List[Dict] =[]

    total_messages = 0
    skipped_invalid = 0
    accepted = 0

    async for msg in client.iter_messages(channel, limit=limit):
        total_messages += 1

        if not isinstance(msg, Message):
            skipped_invalid += 1
            continue

        text = msg.text or msg.message or msg.raw_text

        if not text:
            skipped_invalid += 1
            continue

        text = text.strip()

        # Машины не продают в 2 словах
        if len(text) < 40:
            skipped_invalid += 1
            continue

        low = text.lower()

        # Быстрый отсев откровенного флуда
        if any(x in low for x in ["обсуждение", "комментарии", "подписывайтесь", "ссылка в профиле"]):
            skipped_invalid += 1
            continue

        # skip forwarded ads spam
        if getattr(msg, "fwd_from", None):
            skipped_invalid += 1
            continue

        # 🔥 ГЛАВНАЯ ПРОВЕРКА ЧЕРЕЗ ФИЛЬТР
        ok, reason = is_valid_telegram_post(text)

        if not ok:
            # Можно раскомментировать для отладки: 
            # print(f"[TG SKIP] {reason} -> {text[:50]}...")
            skipped_invalid += 1
            continue

        # limit message length
        if len(text) > 5000:
            text = text[:5000]

        # skip repost chains
        if text.count("http") > 5:
            skipped_invalid += 1
            continue

        # dedupe identical posts
        if any(x["content"] == text for x in items):
            skipped_invalid += 1
            continue

        source_url = f"https://t.me/{channel.lstrip('@')}/{msg.id}"

        title_line = text.split("\n")[0]

        if len(title_line) < 20:
            title_line = text[:120]

        items.append(
            {
                "source": "telegram",
                "source_url": source_url,
                "title": title_line.replace("\n", " ").strip(),
                "content": text,
                "created_at": msg.date.isoformat() if getattr(msg, "date", None) else None,
                "created_at_ts": int(msg.date.timestamp()) if getattr(msg, "date", None) else None,
                "created_at_source": "telegram",
            }
        )

        accepted += 1

    print(
        f"[TELEGRAM][{channel}] "
        f"total={total_messages}, "
        f"accepted={accepted}, "
        f"skipped={skipped_invalid}"
    )

    if accepted == 0:
        print(f"[TELEGRAM][WARN] channel {channel} returned 0 valid posts")

    return items


# =========================
# ASYNC PUBLIC API
# =========================

async def fetch_telegram(limit_per_channel: int | None = None) -> List[Dict]:
    """
    Асинхронный fetch Telegram.
    """

    if os.getenv("DISABLE_TELEGRAM", "false").lower() in ("1", "true", "yes"):
        print("[TELEGRAM] disabled via DISABLE_TELEGRAM env var")
        return []

    TG_API_ID = _get_int_env("TG_API_ID", 0)
    TG_API_HASH = os.getenv("TG_API_HASH", "")
    TG_SESSION_STRING = os.getenv("TG_SESSION_STRING", "")
    TG_FETCH_LIMIT = _get_int_env("TG_FETCH_LIMIT", 50)

    if not TG_API_ID or not TG_API_HASH or not TG_SESSION_STRING:
        print("[TELEGRAM][WARN] TG_API_ID / TG_API_HASH / TG_SESSION_STRING not set — skipping")
        return []

    # Telethon StringSession strings are base64-encoded, typically 350+ chars.
    # A short value means placeholder/invalid — would cause interactive auth prompt in container.
    if len(TG_SESSION_STRING) < 100:
        print(f"[TELEGRAM][WARN] TG_SESSION_STRING too short ({len(TG_SESSION_STRING)} chars) — skipping to avoid interactive prompt")
        return []

    channels = load_channels()
    if not channels:
        print("[TELEGRAM][WARN] no channels configured")
        return[]

    limit = limit_per_channel or TG_FETCH_LIMIT
    results: List[Dict] =[]

    async with TelegramClient(
        StringSession(TG_SESSION_STRING),
        TG_API_ID,
        TG_API_HASH,
    ) as client:

        tasks =[]

        for channel in channels:
            await asyncio.sleep(random.uniform(2.0, 4.5))
            tasks.append(_fetch_from_channel(client, channel, limit))

        results_nested = await asyncio.gather(*tasks, return_exceptions=True)

        for r in results_nested:
            if isinstance(r, list):
                results.extend(r)

    print(f"[TELEGRAM] total accepted from all channels: {len(results)}")

    return results


# =========================
# SYNC WRAPPER (SAFE)
# =========================

def fetch_telegram_sync(limit_per_channel: int | None = None) -> List[Dict]:
    return asyncio.run(fetch_telegram(limit_per_channel))