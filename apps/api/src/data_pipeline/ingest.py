import os
import asyncio
from typing import List, Dict

from db.session import SessionLocal
from db.models import RawDocument

# =========================
# SOURCES
# =========================

from integrations.sources.telegram import fetch_telegram

from integrations.sources.auto_ru_playwright import fetch_auto_ru_serp
from integrations.sources.drom_ru_playwright import fetch_drom_ru_serp


# =========================
# ENV FLAGS
# =========================
# Пример:
# INGEST_SOURCES=telegram,auto_ru,drom_ru
INGEST_SOURCES_RAW = os.getenv("INGEST_SOURCES", "telegram")

# лимиты для SERP
AUTO_RU_LIMIT = int(os.getenv("AUTO_RU_LIMIT", "30"))
DROM_RU_LIMIT = int(os.getenv("DROM_RU_LIMIT", "30"))


def _parse_sources() -> List[str]:
    return [
        s.strip().lower()
        for s in INGEST_SOURCES_RAW.split(",")
        if s.strip()
    ]


# =========================
# MAIN INGEST
# =========================

def run_ingest():
    """
    Ingest pipeline (configurable):

    Sources (via ENV INGEST_SOURCES):
      - telegram  -> fetch_telegram()
      - auto_ru   -> fetch_auto_ru_serp()
      - drom_ru   -> fetch_drom_ru_serp()

    All sources -> RawDocument
    """

    session = SessionLocal()
    sources = _parse_sources()

    all_items: List[Dict] = []

    try:
        # -------------------------
        # TELEGRAM
        # -------------------------
        if "telegram" in sources:
            try:
                tg_items = fetch_telegram()
                if not tg_items:
                    print("[INGEST][WARN] telegram returned 0 items")
                else:
                    all_items.extend(tg_items)
                    print(f"[INGEST][TELEGRAM] fetched: {len(tg_items)}")
            except Exception as e:
                print(f"[INGEST][ERROR] telegram failed: {e}")

        # -------------------------
        # AUTO.RU (Playwright SERP)
        # -------------------------
        if "auto_ru" in sources:
            try:
                auto_items = asyncio.run(
                    fetch_auto_ru_serp(limit=AUTO_RU_LIMIT)
                )
                if not auto_items:
                    print("[INGEST][WARN] auto.ru returned 0 items")
                else:
                    all_items.extend(auto_items)
                    print(f"[INGEST][AUTO.RU] fetched: {len(auto_items)}")
            except Exception as e:
                print(f"[INGEST][ERROR] auto.ru failed: {e}")

        # -------------------------
        # DROM.RU (Playwright SERP)
        # -------------------------
        if "drom_ru" in sources:
            try:
                drom_items = asyncio.run(
                    fetch_drom_ru_serp(limit=DROM_RU_LIMIT)
                )
                if not drom_items:
                    print("[INGEST][WARN] drom.ru returned 0 items")
                else:
                    all_items.extend(drom_items)
                    print(f"[INGEST][DROM.RU] fetched: {len(drom_items)}")
            except Exception as e:
                print(f"[INGEST][ERROR] drom.ru failed: {e}")

        if not all_items:
            print("[INGEST][WARN] no items fetched from any source")
            return

        # -------------------------
        # SAVE TO DB (RawDocument)
        # -------------------------
        saved = 0
        skipped = 0

        for item in all_items:
            exists = (
                session.query(RawDocument)
                .filter(RawDocument.source_url == item["source_url"])
                .first()
            )

            if exists:
                skipped += 1
                continue

            doc = RawDocument(
                source=item["source"],          # telegram / auto.ru / drom.ru
                source_url=item["source_url"],
                title=item.get("title"),
                content=item.get("content"),
            )
            session.add(doc)
            saved += 1

        session.commit()

        print(
            f"[INGEST] saved: {saved}, skipped (duplicates): {skipped}, total fetched: {len(all_items)}"
        )

    finally:
        session.close()
