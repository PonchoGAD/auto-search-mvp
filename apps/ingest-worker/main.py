import asyncio
import time
import random

from sqlalchemy.exc import OperationalError
from sqlalchemy import text

from db.session import engine, SessionLocal, Base
from db.models import RawDocument

from sources.auto_ru import fetch_auto_ru_serp
from sources.avito import fetch_avito_serp
from sources.drom import fetch_drom_ru

# ✅ ВАЖНО: импортируем ASYNC telegram (НЕ sync wrapper)
from sources.telegram import fetch_telegram  # async def

# ✅ ДОБАВЛЕНО — форумы
from sources.benzclub import fetch_benzclub_listings
from sources.bmwclub import fetch_bmwclub_listings

SLEEP_BASE = 900   # 15 минут
MAX_BACKOFF = 3600 # 1 час


# =========================
# DB WAIT (CRITICAL)
# =========================
def wait_for_db(engine, retries: int = 30, delay: int = 2):
    for i in range(retries):
        try:
            with engine.connect():
                print("[DB] connected", flush=True)
                return
        except OperationalError:
            print(f"[DB] waiting... ({i + 1}/{retries})", flush=True)
            time.sleep(delay)
    raise RuntimeError("DB not available after retries")


wait_for_db(engine)
Base.metadata.create_all(bind=engine)


# =========================
# DB SCHEMA PATCH (MVP MIGRATION)
# =========================
def ensure_schema():
    session = SessionLocal()
    try:
        session.execute(text("""
            DO $$
            BEGIN
                IF NOT EXISTS (
                    SELECT 1
                    FROM information_schema.columns
                    WHERE table_name='raw_documents'
                    AND column_name='indexed'
                ) THEN
                    ALTER TABLE raw_documents
                    ADD COLUMN indexed BOOLEAN DEFAULT FALSE;
                END IF;
            END $$;
        """))
        session.commit()
        print("[DB] ensured raw_documents.indexed", flush=True)
    finally:
        session.close()


ensure_schema()


# =========================
# SAVE (HARDENED)
# =========================
def save_items(items):
    session = SessionLocal()
    saved = 0
    skipped = 0

    try:
        for item in items:
            url = item.get("source_url")
            content = item.get("content")

            if not url:
                skipped += 1
                print(f"[DB][SAVE] reason_skip=no_url item={item}", flush=True)
                continue

            if not content or len(content.strip()) < 10:
                skipped += 1
                print(f"[DB][SAVE] reason_skip=short_content url={url}", flush=True)
                continue

            exists = (
                session.query(RawDocument)
                .filter_by(source_url=url)
                .first()
            )
            if exists:
                skipped += 1
                print(f"[DB][SAVE] reason_skip=duplicate url={url}", flush=True)
                continue

            session.add(
                RawDocument(
                    source=item.get("source", "unknown"),
                    source_url=url,
                    title=item.get("title"),
                    content=content,
                )
            )
            saved += 1

        session.commit()
        print(f"[DB] saved={saved} skipped={skipped}", flush=True)
    finally:
        session.close()

    return saved, skipped


# =========================
# SAFE FETCH HELPERS (timeouts)
# =========================
async def safe_await(label: str, coro, timeout_s: int = 120):
    try:
        return await asyncio.wait_for(coro, timeout=timeout_s)
    except asyncio.TimeoutError:
        print(f"[INGEST][TIMEOUT] {label} > {timeout_s}s", flush=True)
        return []
    except Exception as e:
        print(f"[INGEST][ERROR] {label}: {e}", flush=True)
        return []


# =========================
# RUN
# =========================
async def run_cycle():
    print("[INGEST] run_cycle STARTED", flush=True)

    auto_items = await safe_await("auto_ru", fetch_auto_ru_serp(limit=50), timeout_s=180)
    await asyncio.sleep(random.uniform(0.5, 1.5))

    avito_items = await safe_await("avito", fetch_avito_serp(limit=50), timeout_s=180)
    await asyncio.sleep(random.uniform(0.5, 1.5))

    # drom sync — завернём в try, чтобы не валило цикл
    try:
        drom_items = fetch_drom_ru(limit=50) or []
    except Exception as e:
        print(f"[INGEST][ERROR] drom: {e}", flush=True)
        drom_items = []
    await asyncio.sleep(random.uniform(0.5, 1.5))

    # ✅ TELEGRAM — ASYNC await
    telegram_items = await safe_await("telegram", fetch_telegram(limit_per_channel=50), timeout_s=180)
    print(f"[INGEST] telegram fetched={len(telegram_items)}", flush=True)

    # ✅ ДОБАВЛЕНО — BENZCLUB
    try:
        benz_items = fetch_benzclub_listings(limit=30) or []
    except Exception as e:
        print(f"[INGEST][ERROR] benzclub: {e}", flush=True)
        benz_items = []

    # ✅ ДОБАВЛЕНО — BMWCLUB
    try:
        bmw_items = fetch_bmwclub_listings(limit=30) or []
    except Exception as e:
        print(f"[INGEST][ERROR] bmwclub: {e}", flush=True)
        bmw_items = []

    # =========================
    # TOTAL (РАСШИРЕНО)
    # =========================
    total = (
        (auto_items or [])
        + (avito_items or [])
        + (drom_items or [])
        + (telegram_items or [])
        + benz_items
        + bmw_items
    )

    print(f"[INGEST] total fetched={len(total)}", flush=True)

    saved, skipped = save_items(total)
    print(f"[INGEST] saved={saved} skipped={skipped}", flush=True)

    # ✅ ЛЕНИВЫЙ IMPORT индекса — после DB save
    if saved > 0:
        print("[INDEX] run_index CALLED", flush=True)
        from index import run_index
        run_index(limit=200)
    else:
        print("[INDEX] skipped (nothing saved)", flush=True)


# =========================
# PRODUCTION LOOP
# =========================
def main():
    backoff = SLEEP_BASE
    print("[INGEST WORKER] BOOTED", flush=True)

    while True:
        try:
            print("[INGEST] cycle started", flush=True)
            asyncio.run(run_cycle())
            print(f"[INGEST] cycle completed — sleeping {SLEEP_BASE}s", flush=True)
            time.sleep(SLEEP_BASE)
            backoff = SLEEP_BASE

        except Exception as e:
            print(f"[INGEST] unexpected error: {e}", flush=True)
            time.sleep(min(backoff, MAX_BACKOFF))
            backoff = min(backoff * 2, MAX_BACKOFF)


if __name__ == "__main__":
    main()