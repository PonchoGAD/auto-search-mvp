# apps/api/src/data_pipeline/ingest.py

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
from integrations.sources.avito_playwright import fetch_avito_serp

# Форумы (HTTP, без Playwright)
from integrations.sources.benzclub import fetch_benzclub_listings
from integrations.sources.bmwclub import fetch_bmwclub_listings

# =========================
# QUALITY GATE (PROMPT 22)
# =========================

from services.ingest_quality import (
    is_sale_intent,
    detect_brand,
    build_meta_prefix,
    resolve_source_boost,
)

# =========================
# ENV CONFIG
# =========================
# Пример:
# INGEST_SOURCES=telegram,auto_ru,drom_ru,avito,benzclub,bmwclub

INGEST_SOURCES_RAW = os.getenv("INGEST_SOURCES", "telegram")

AUTO_RU_LIMIT = int(os.getenv("AUTO_RU_LIMIT", "30"))
DROM_RU_LIMIT = int(os.getenv("DROM_RU_LIMIT", "30"))
AVITO_LIMIT = int(os.getenv("AVITO_LIMIT", "30"))
BENZCLUB_LIMIT = int(os.getenv("BENZCLUB_LIMIT", "30"))
BMWCLUB_LIMIT = int(os.getenv("BMWCLUB_LIMIT", "30"))

# фильтр продаж
SALE_FILTER = os.getenv("SALE_FILTER", "true").lower() == "true"

# =========================
# HELPERS
# =========================

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
    Универсальный ingest pipeline + quality gate.

    Управляется через ENV:
      INGEST_SOURCES=telegram,auto_ru,drom_ru,avito,benzclub,bmwclub
      SALE_FILTER=true|false

    На ingestion:
      - фильтр intent "продажа"
      - определение бренда (brands.yaml)
      - source boost
      - meta-prefix без миграций БД
    """

    session = SessionLocal()
    sources = _parse_sources()

    all_items: List[Dict] = []

    skipped_not_sale = 0
    skipped_duplicates = 0

    try:
        # -------------------------
        # TELEGRAM
        # -------------------------
        if "telegram" in sources:
            try:
                items = fetch_telegram()
                if not items:
                    print("[INGEST][WARN] telegram returned 0 items")
                else:
                    all_items.extend(items)
                    print(f"[INGEST][TELEGRAM] fetched: {len(items)}")
            except Exception as e:
                print(f"[INGEST][ERROR] telegram failed: {e}")

        # -------------------------
        # AUTO.RU
        # -------------------------
        if "auto_ru" in sources:
            try:
                items = asyncio.run(fetch_auto_ru_serp(limit=AUTO_RU_LIMIT))
                if not items:
                    print("[INGEST][WARN] auto.ru returned 0 items")
                else:
                    all_items.extend(items)
                    print(f"[INGEST][AUTO.RU] fetched: {len(items)}")
            except Exception as e:
                print(f"[INGEST][ERROR] auto.ru failed: {e}")

        # -------------------------
        # DROM.RU
        # -------------------------
        if "drom_ru" in sources:
            try:
                items = asyncio.run(fetch_drom_ru_serp(limit=DROM_RU_LIMIT))
                if not items:
                    print("[INGEST][WARN] drom.ru returned 0 items")
                else:
                    all_items.extend(items)
                    print(f"[INGEST][DROM.RU] fetched: {len(items)}")
            except Exception as e:
                print(f"[INGEST][ERROR] drom.ru failed: {e}")

        # -------------------------
        # AVITO
        # -------------------------
        if "avito" in sources:
            try:
                items = asyncio.run(fetch_avito_serp(limit=AVITO_LIMIT))
                if not items:
                    print("[INGEST][WARN] avito returned 0 items")
                else:
                    all_items.extend(items)
                    print(f"[INGEST][AVITO] fetched: {len(items)}")
            except Exception as e:
                print(f"[INGEST][ERROR] avito failed: {e}")

        # -------------------------
        # BENZCLUB
        # -------------------------
        if "benzclub" in sources:
            try:
                items = fetch_benzclub_listings(limit=BENZCLUB_LIMIT)
                if not items:
                    print("[INGEST][WARN] benzclub returned 0 items")
                else:
                    all_items.extend(items)
                    print(f"[INGEST][BENZCLUB] fetched: {len(items)}")
            except Exception as e:
                print(f"[INGEST][ERROR] benzclub failed: {e}")

        # -------------------------
        # BMWCLUB
        # -------------------------
        if "bmwclub" in sources:
            try:
                items = fetch_bmwclub_listings(limit=BMWCLUB_LIMIT)
                if not items:
                    print("[INGEST][WARN] bmwclub returned 0 items")
                else:
                    all_items.extend(items)
                    print(f"[INGEST][BMWCLUB] fetched: {len(items)}")
            except Exception as e:
                print(f"[INGEST][ERROR] bmwclub failed: {e}")

        if not all_items:
            print("[INGEST][WARN] no items fetched from any source")
            return

        # -------------------------
        # QUALITY FILTER + SAVE
        # -------------------------
        saved = 0

        for item in all_items:
            source_url = item["source_url"]

            exists = (
                session.query(RawDocument)
                .filter(RawDocument.source_url == source_url)
                .first()
            )

            if exists:
                skipped_duplicates += 1
                continue

            title = item.get("title") or ""
            content = item.get("content") or ""
            text = f"{title} {content}".lower()

            # SALE FILTER
            sale_intent = is_sale_intent(text)

            if SALE_FILTER and not sale_intent:
                skipped_not_sale += 1
                continue

            # BRAND
            brand, brand_conf = detect_brand(text)

            # SOURCE BOOST
            source_boost = resolve_source_boost(item.get("source"))

            # META PREFIX (MVP)
            meta = build_meta_prefix(
                brand=brand,
                brand_confidence=brand_conf,
                sale_intent=sale_intent,
                source_boost=source_boost,
            )

            final_content = f"{meta}\n{content}"

            doc = RawDocument(
                source=item["source"],
                source_url=source_url,
                title=title,
                content=final_content,
            )

            session.add(doc)
            saved += 1

        session.commit()

        print(
            f"[INGEST] saved: {saved}, "
            f"skipped duplicates: {skipped_duplicates}, "
            f"skipped not-sale: {skipped_not_sale}, "
            f"total fetched: {len(all_items)}"
        )

    finally:
        session.close()
