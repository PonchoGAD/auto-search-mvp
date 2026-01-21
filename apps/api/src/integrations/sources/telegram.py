import os
from typing import List, Dict

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Message

from data_pipeline.telegram_filters import is_valid_telegram_post


# =========================
# ENV
# =========================

TG_API_ID = int(os.getenv("TG_API_ID", "0"))
TG_API_HASH = os.getenv("TG_API_HASH", "")
TG_SESSION_STRING = os.getenv("TG_SESSION_STRING", "")

TG_FETCH_LIMIT = int(os.getenv("TG_FETCH_LIMIT", "50"))
TG_CHANNELS_RAW = os.getenv("TG_CHANNELS", "")


# =========================
# HELPERS
# =========================

def load_channels() -> List[str]:
    """
    Загружает список Telegram-каналов из ENV.
    Пример:
    TG_CHANNELS=@cars_ru,@auto_moscow
    """
    return [
        c.strip()
        for c in TG_CHANNELS_RAW.split(",")
        if c.strip()
    ]


async def _fetch_from_channel(
    client: TelegramClient,
    channel: str,
    limit: int,
) -> List[Dict]:
    items: List[Dict] = []

    async for msg in client.iter_messages(channel, limit=limit):
        if not isinstance(msg, Message):
            continue

        if not msg.text:
            continue

        # 🔹 ФИЛЬТР КАЧЕСТВА
        if not is_valid_telegram_post(msg.text):
            continue

        source_url = f"https://t.me/{channel.lstrip('@')}/{msg.id}"

        items.append(
            {
                "source": "telegram",
                "source_url": source_url,
                "title": msg.text[:120].replace("\n", " ").strip(),
                "content": msg.text,
            }
        )

    return items


# =========================
# PUBLIC API
# =========================

def fetch_telegram(limit_per_channel: int | None = None) -> List[Dict]:
    """
    Entry point для ingestion Telegram.
    Возвращает список dict для RawDocument:
      {source, source_url, title, content}
    """

    if not TG_API_ID or not TG_API_HASH or not TG_SESSION_STRING:
        raise RuntimeError("Telegram ENV variables are not set")

    channels = load_channels()
    if not channels:
        print("[TELEGRAM][WARN] no channels configured")
        return []

    limit = limit_per_channel or TG_FETCH_LIMIT
    results: List[Dict] = []

    with TelegramClient(
        StringSession(TG_SESSION_STRING),
        TG_API_ID,
        TG_API_HASH,
    ) as client:
        for channel in channels:
            try:
                items = client.loop.run_until_complete(
                    _fetch_from_channel(client, channel, limit)
                )
                results.extend(items)
                print(f"[TELEGRAM] {channel}: fetched {len(items)}")
            except Exception as e:
                print(f"[TELEGRAM][ERROR] {channel}: {e}")

    return results
