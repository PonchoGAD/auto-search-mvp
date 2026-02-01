# apps/api/src/integrations/sources/telegram.py

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
    return [c.strip() for c in TG_CHANNELS_RAW.split(",") if c.strip()]


async def _fetch_from_channel(
    client: TelegramClient,
    channel: str,
    limit: int,
) -> List[Dict]:
    """
    Fetch + HARD anti-noise фильтр на уровне источника.
    Здесь мусор умирает окончательно.
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

        if not msg.text:
            skipped_invalid += 1
            continue

        text = msg.text.strip()

        # =========================
        # 🔒 HARD ANTI-NOISE FILTER
        # =========================
        if not is_valid_telegram_post(text):
            skipped_invalid += 1
            continue

        source_url = f"https://t.me/{channel.lstrip('@')}/{msg.id}"

        items.append(
            {
                "source": "telegram",
                "source_url": source_url,
                "title": text[:120].replace("\n", " ").strip(),
                "content": text,

                # 🔑 RECENCY — КАНОНИЧНЫЙ ФОРМАТ
                # ISO строка (безопасно для JSON / DB / Qdrant)
                "created_at": msg.date.isoformat() if getattr(msg, "date", None) else None,

                # Unix timestamp (удобно для скоринга / сортировки)
                "created_at_ts": int(msg.date.timestamp()) if getattr(msg, "date", None) else None,

                # Откуда взята дата
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

    return items


# =========================
# PUBLIC API
# =========================

def fetch_telegram(limit_per_channel: int | None = None) -> List[Dict]:
    """
    Entry point для ingestion Telegram.

    ГАРАНТИИ:
    - мусор не выходит из этого уровня
    - ingest получает только валидные объявления
    - created_at/created_at_ts всегда есть (если date есть у сообщения)
    - аналитика и recency не врут

    Возвращает список dict для RawDocument:
      {source, source_url, title, content, created_at, created_at_ts, created_at_source}
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
            except Exception as e:
                print(f"[TELEGRAM][ERROR] {channel}: {e}")

    print(f"[TELEGRAM] total accepted from all channels: {len(results)}")

    return results
