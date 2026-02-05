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
# QUALITY / ANTI-NOISE
# =========================

from services.ingest_quality import (
    should_skip_doc,
    enrich_text_with_meta,
    SkipStats,
)

# =========================
# INDEXING (QDRANT)
# =========================

from data_pipeline.index import index_raw_documents

# =========================
# ENV CONFIG
# =========================

ENV = os.getenv("ENV", "local")
ENABLE_INGEST = os.getenv("ENABLE_INGEST", "false").lower() == "true"

DEMO_INGEST = os.getenv("DEMO_INGEST", "false").lower() == "true"
DEMO_INGEST_LIMIT = int(os.getenv("DEMO_INGEST_LIMIT", "30"))

INGEST_SOURCES_RAW = os.getenv("INGEST_SOURCES", "telegram")

AUTO_RU_LIMIT = int(os.getenv("AUTO_RU_LIMIT", "30"))
DROM_RU_LIMIT = int(os.getenv("DROM_RU_LIMIT", "30"))
AVITO_LIMIT = int(os.getenv("AVITO_LIMIT", "30"))
BENZCLUB_LIMIT = int(os.getenv("BENZCLUB_LIMIT", "30"))
BMWCLUB_LIMIT = int(os.getenv("BMWCLUB_LIMIT", "30"))


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

def run_ingest() -> Dict[str, int]:
    """
    MVP ingest pipeline.

    ⚠️ ЖЁСТКО ПОД МОДЕЛЬ RawDocument:
    id, source, source_url, title, content, fetched_at
    """

    if ENV == "prod" and not ENABLE_INGEST:
        print("[INGEST][BLOCKED] ingest disabled in prod")
        return {"saved": 0, "indexed": 0, "skipped": 0}

    session = SessionLocal()
    sources = _parse_sources()

    all_items: List[Dict] = []
    skipped_duplicates = 0
    stats = SkipStats()

    try:
        # -------------------------
        # TELEGRAM
        # -------------------------
        if "telegram" in sources:
            items = fetch_telegram()
            all_items.extend(items or [])
            print(f"[INGEST][TELEGRAM] fetched: {len(items or [])}")

        # -------------------------
        # AUTO.RU
        # -------------------------
        if "auto_ru" in sources:
            items = asyncio.run(fetch_auto_ru_serp(limit=AUTO_RU_LIMIT))
            all_items.extend(items or [])
            print(f"[INGEST][AUTO.RU] fetched: {len(items or [])}")

        # -------------------------
        # DROM.RU
        # -------------------------
        if "drom_ru" in sources:
            items = asyncio.run(fetch_drom_ru_serp(limit=DROM_RU_LIMIT))
            all_items.extend(items or [])
            print(f"[INGEST][DROM.RU] fetched: {len(items or [])}")

        # -------------------------
        # AVITO
        # -------------------------
        if "avito" in sources:
            items = asyncio.run(fetch_avito_serp(limit=AVITO_LIMIT))
            all_items.extend(items or [])
            print(f"[INGEST][AVITO] fetched: {len(items or [])}")

        # -------------------------
        # BENZCLUB
        # -------------------------
        if "benzclub" in sources:
            items = fetch_benzclub_listings(limit=BENZCLUB_LIMIT)
            all_items.extend(items or [])
            print(f"[INGEST][BENZCLUB] fetched: {len(items or [])}")

        # -------------------------
        # BMWCLUB
        # -------------------------
        if "bmwclub" in sources:
            items = fetch_bmwclub_listings(limit=BMWCLUB_LIMIT)
            all_items.extend(items or [])
            print(f"[INGEST][BMWCLUB] fetched: {len(items or [])}")

        if not all_items:
            print("[INGEST][WARN] no items fetched")
            return {"saved": 0, "indexed": 0, "skipped": 0}

        if DEMO_INGEST:
            all_items = all_items[:DEMO_INGEST_LIMIT]

        saved_docs: List[RawDocument] = []

        for item in all_items:
            stats.total += 1

            source_url = item.get("source_url")
            source = item.get("source") or ""

            if not source_url:
                stats.add(skip=True, reason="no_source_url")
                continue

            if session.query(RawDocument).filter_by(source_url=source_url).first():
                skipped_duplicates += 1
                stats.add(skip=True, reason="duplicate")
                continue

            title = item.get("title") or ""
            content = item.get("content") or ""
            raw_text = f"{title}\n{content}".strip()

            skip, skip_meta = should_skip_doc(raw_text, source)
            if skip:
                stats.add(skip=True, reason=skip_meta.get("reason", "unknown"))
                continue

            final_content, _ = enrich_text_with_meta(raw_text, source)

            doc = RawDocument(
                source=source,
                source_url=source_url,
                title=title,
                content=final_content,
            )

            session.add(doc)
            saved_docs.append(doc)
            stats.add(skip=False, reason="ok")

        session.commit()

        indexed = index_raw_documents(saved_docs)

        print(
            f"[INGEST] saved={len(saved_docs)}, "
            f"indexed={indexed}, "
            f"duplicates={skipped_duplicates}"
        )

        stats.log(prefix="[INGEST][ANTI_NOISE]")

        return {
            "saved": len(saved_docs),
            "indexed": indexed,
            "skipped": stats.skipped,
        }

    finally:
        session.close()
