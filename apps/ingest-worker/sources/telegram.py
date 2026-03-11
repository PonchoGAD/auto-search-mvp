import os
import random
import asyncio
from typing import List, Dict

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Message

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

def load_channels() -> List[str]:
    """
    Загружает список Telegram-каналов из ENV.
    Пример:
    TG_CHANNELS=@cars_ru,@auto_moscow
    """

    raw = os.getenv("TG_CHANNELS", "")
    out = []

    for c in raw.split(","):
        c = c.strip()
        if not c:
            continue

        if "t.me/" in c:
            c = c.split("t.me/")[-1].strip("/")
            c = "@" + c

        if not c.startswith("@"):
            c = "@" + c

        out.append(c)

    return out


async def _fetch_from_channel(
    client: TelegramClient,
    channel: str,
    limit: int,
) -> List[Dict]:
    """
    Fetch + минимальный anti-noise на уровне источника.
    Бизнес-логика фильтрации остаётся в ingest_quality.
    """

    items: List[Dict] = []

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

        if len(text) < 40:
            skipped_invalid += 1
            continue

        low = text.lower()

        if "обсуждение" in low:
            skipped_invalid += 1
            continue

        if "комментарии" in low:
            skipped_invalid += 1
            continue

        if "подписывайтесь" in low:
            skipped_invalid += 1
            continue

        # skip forwarded ads spam
        if getattr(msg, "fwd_from", None):
            skipped_invalid += 1
            continue

        ok, reason = is_valid_telegram_post(text)

        if not ok:
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

    TG_API_ID = _get_int_env("TG_API_ID", 0)
    TG_API_HASH = os.getenv("TG_API_HASH", "")
    TG_SESSION_STRING = os.getenv("TG_SESSION_STRING", "")
    TG_FETCH_LIMIT = _get_int_env("TG_FETCH_LIMIT", 50)

    if not TG_API_ID or not TG_API_HASH or not TG_SESSION_STRING:
        print("[TELEGRAM][WARN] Telegram ENV variables are not properly set")
        return []

    channels = load_channels()
    if not channels:
        print("[TELEGRAM][WARN] no channels configured")
        return []

    limit = limit_per_channel or TG_FETCH_LIMIT
    results: List[Dict] = []

    async with TelegramClient(
        StringSession(TG_SESSION_STRING),
        TG_API_ID,
        TG_API_HASH,
    ) as client:

        tasks = []

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