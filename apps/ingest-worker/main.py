import asyncio
import os
import time
import random
import requests

from sqlalchemy.exc import OperationalError

from db.session import engine, SessionLocal, Base
from db.models import RawDocument

from sources.auto_ru import fetch_auto_ru_serp
from sources.avito import fetch_avito_serp
from sources.drom import fetch_drom_ru


# =========================
# DB WAIT (CRITICAL)
# =========================

def wait_for_db(engine, retries: int = 10, delay: int = 3):
    """
    Ждём, пока PostgreSQL реально начнёт принимать соединения.
    Без этого ingest-worker будет падать при старте.
    """
    for i in range(retries):
        try:
            with engine.connect():
                print("[DB] connected")
                return
        except OperationalError:
            print(f"[DB] waiting... ({i + 1}/{retries})")
            time.sleep(delay)

    raise RuntimeError("DB not available after retries")


# 🔴 КЛЮЧЕВОЙ БЛОК
wait_for_db(engine)
Base.metadata.create_all(bind=engine)


# =========================
# SAVE
# =========================

def save_items(items):
    session = SessionLocal()
    saved = 0
    skipped = 0

    try:
        for item in items:

            # 🔥 Оставляем skip только если URL пустой
            if not item.get("source_url"):
                skipped += 1
                continue

            exists = (
                session.query(RawDocument)
                .filter_by(
                    source=item["source"],
                    source_url=item["source_url"],
                )
                .first()
            )

            if exists:
                skipped += 1
                continue

            session.add(
                RawDocument(
                    source=item["source"],
                    source_url=item["source_url"],
                    title=item.get("title"),
                    content=item.get("content"),
                )
            )
            saved += 1

        session.commit()
    finally:
        session.close()

    return saved, skipped


# =========================
# RUN
# =========================

async def run():
    auto_items = await fetch_auto_ru_serp(limit=300)
    avito_items = await fetch_avito_serp(limit=300)
    drom_items = fetch_drom_ru(limit=300)

    total = auto_items + avito_items + drom_items
    saved, skipped = save_items(total)

    print(
        f"[INGEST-WORKER] fetched={len(total)} "
        f"saved={saved} skipped={skipped}"
    )


# =========================
# PRODUCTION LOOP WITH BACKOFF
# =========================

SLEEP_BASE = 900  # 15 минут
MAX_BACKOFF = 3600  # максимум 1 час

backoff = SLEEP_BASE

if __name__ == "__main__":
    while True:
        try:
            print("[INGEST] cycle started")

            # сохраняем существующую async-логику
            asyncio.run(run())

            print(f"[INGEST] cycle completed — sleeping {SLEEP_BASE}s")
            time.sleep(SLEEP_BASE)

            backoff = SLEEP_BASE  # сбрасываем если успех

        except requests.exceptions.HTTPError as e:
            if "429" in str(e):
                print(f"[INGEST] 429 detected — backing off {backoff}s")
                time.sleep(backoff)
                backoff = min(backoff * 2, MAX_BACKOFF)
            else:
                print(f"[INGEST] error: {e}")
                time.sleep(600)

        except Exception as e:
            print(f"[INGEST] unexpected error: {e}")
            time.sleep(600)
