#  apps\api\src\data_pipeline\ingest.py

import os
import re
import asyncio
from typing import List, Dict
from datetime import datetime, timezone

from db.session import SessionLocal
from db.models import RawDocument

# =========================
# SOURCES
# =========================

from integrations.sources.telegram import fetch_telegram

try:
    from integrations.sources.auto_ru import fetch_auto_ru_serp
except ImportError:
    fetch_auto_ru_serp = None

try:
    from integrations.sources.drom_ru import fetch_drom_ru_serp
except ImportError:
    fetch_drom_ru_serp = None

try:
    from integrations.sources.avito import fetch_avito_serp
except ImportError:
    fetch_avito_serp = None

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

from data_pipeline.normalize import run_normalize
from data_pipeline.chunk import run_chunk
from data_pipeline.index import run_index

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


def _ensure_event_loop() -> asyncio.AbstractEventLoop:
    try:
        return asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        return loop


def _run_coro(loop: asyncio.AbstractEventLoop, coro):
    return loop.run_until_complete(coro)


# =========================
# MAIN INGEST
# =========================

def run_ingest() -> Dict[str, int]:
    """
    Универсальный ingest pipeline + anti-noise gate.

    ГАРАНТИИ:
    - ingest ВЫКЛЮЧЕН в prod без ENABLE_INGEST=true
    - DEMO режим ограничивает объём данных
    - сохраняем только реальные поля RawDocument
    - после ingest идёт индексация в Qdrant
    """

    if ENV == "prod" and not ENABLE_INGEST:
        print("[INGEST][BLOCKED] ingest disabled in prod (ENABLE_INGEST=false)")
        return {"saved": 0, "indexed": 0, "skipped": 0}

    loop = _ensure_event_loop()

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
            try:
                items = _run_coro(loop, fetch_telegram(limit_per_channel=None))
                all_items.extend(items or [])
                print(f"[INGEST][TELEGRAM] fetched: {len(items or [])}")
            except Exception as e:
                print(f"[INGEST][ERROR] telegram failed: {e}")

        # -------------------------
        # AUTO.RU
        # -------------------------
        if "auto_ru" in sources and fetch_auto_ru_serp:
            try:
                items = _run_coro(loop, fetch_auto_ru_serp(limit=AUTO_RU_LIMIT))
                all_items.extend(items or [])
                print(f"[INGEST][AUTO.RU] fetched: {len(items or [])}")
            except Exception as e:
                print(f"[INGEST][ERROR] auto.ru failed: {e}")

        # -------------------------
        # DROM.RU
        # -------------------------
        if "drom_ru" in sources and fetch_drom_ru_serp:
            try:
                items = _run_coro(loop, fetch_drom_ru_serp(limit=DROM_RU_LIMIT))
                all_items.extend(items or [])
                print(f"[INGEST][ERROR] drom.ru failed: {e}")

        # -------------------------
        # AVITO
        # -------------------------
        if "avito" in sources and fetch_avito_serp:
            try:
                items = _run_coro(loop, fetch_avito_serp(limit=AVITO_LIMIT))
                all_items.extend(items or [])
                print(f"[INGEST][AVITO] fetched: {len(items or [])}")
            except Exception as e:
                print(f"[INGEST][ERROR] avito failed: {e}")

        # -------------------------
        # BENZCLUB
        # -------------------------
        if "benzclub" in sources:
            try:
                items = fetch_benzclub_listings(limit=BENZCLUB_LIMIT)
                all_items.extend(items or [])
                print(f"[INGEST][BENZCLUB] fetched: {len(items or [])}")
            except Exception as e:
                print(f"[INGEST][ERROR] benzclub failed: {e}")

        # -------------------------
        # BMWCLUB
        # -------------------------
        if "bmwclub" in sources:
            try:
                items = fetch_bmwclub_listings(limit=BMWCLUB_LIMIT)
                all_items.extend(items or [])
                print(f"[INGEST][BMWCLUB] fetched: {len(items or [])}")
            except Exception as e:
                print(f"[INGEST][ERROR] bmwclub failed: {e}")

        if not all_items:
            print("[INGEST][WARN] no items fetched")
            return {"saved": 0, "indexed": 0, "skipped": 0}

        print(f"[INGEST] total items before processing: {len(all_items)}")

        if DEMO_INGEST:
            all_items = all_items[:DEMO_INGEST_LIMIT]
            print(f"[INGEST][DEMO] limited to {len(all_items)} items")

        # -------------------------
        # ANTI-NOISE + SAVE
        # -------------------------
        saved_docs: List[RawDocument] = []

        for item in all_items:
            stats.total += 1

            source_url = item.get("source_url")
            source = item.get("source") or ""

            if not source_url:
                stats.add(skip=True, reason="no_source_url")
                print(f"[DB][SAVE] saved=0 skipped=1 reason_skip=no_source_url url={source_url}", flush=True)
                continue

            exists = (
                session.query(RawDocument)
                .filter(
                    RawDocument.source == source,
                    RawDocument.source_url == source_url,
                )
                .first()
            )
            if exists:
                skipped_duplicates += 1
                stats.add(skip=True, reason="duplicate")
                print(f"[DB][SAVE] saved=0 skipped=1 reason_skip=duplicate url={source_url}", flush=True)
                continue

            title = item.get("title") or ""
            content = item.get("content") or ""
            raw_text = f"{title}\n{content}".strip()

            if not raw_text or len(raw_text) < 10:
                stats.add(skip=True, reason="content_too_short")
                print(f"[DB][SAVE] saved=0 skipped=1 reason_skip=content_too_short url={source_url}", flush=True)
                continue

            skip, skip_meta = should_skip_doc(
                text=raw_text,
                source=source,
            )
            if skip:
                reason = skip_meta.get("reason", "unknown")
                stats.add(skip=True, reason=reason)
                print(f"[DB][SAVE] saved=0 skipped=1 reason_skip={reason} url={source_url}", flush=True)
                continue

            final_content, _meta = enrich_text_with_meta(
                raw_text=raw_text,
                source=source,
            )

            fuel_match = re.search(
                r"(бензин|дизель|гибрид|электро|газ|hybrid|diesel|petrol|electric)",
                raw_text.lower()
            )
            fuel = fuel_match.group(0) if fuel_match else None

            if fuel:
                final_content = f"{final_content}\n[FUEL: {fuel}]"

            doc = RawDocument(
                source=source,
                source_url=source_url,
                title=title,
                content=final_content,
                fetched_at=datetime.now(timezone.utc),
            )

            session.add(doc)
            saved_docs.append(doc)
            stats.add(skip=False, reason="ok")
            print(f"[DB][SAVE] saved=1 skipped=0 reason_skip=ok url={source_url}", flush=True)

        session.commit()

        # -------------------------
        # NORMALIZE -> CHUNK -> INDEX
        # -------------------------
        pipeline_limit = max(len(saved_docs), 500)

        run_normalize(limit=pipeline_limit, force_rebuild=False)
        run_chunk(limit=pipeline_limit, force_rebuild=False)
        indexed = run_index(limit=max(pipeline_limit * 3, 1000), force_rebuild=False)

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