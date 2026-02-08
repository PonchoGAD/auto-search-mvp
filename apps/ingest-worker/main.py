import asyncio
import os
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from sources.auto_ru import fetch_auto_ru_serp
from sources.avito import fetch_avito_serp
from sources.drom import fetch_drom_ru

from db.models import Base, RawDocument


# =========================
# DB
# =========================

DATABASE_URL = os.getenv("DATABASE_URL")
if not DATABASE_URL:
    raise RuntimeError("DATABASE_URL is not set")

engine = create_engine(DATABASE_URL)
Session = sessionmaker(bind=engine)

# 🔴 КРИТИЧЕСКИ ВАЖНО
# Создаём таблицы ДО любой записи
Base.metadata.create_all(bind=engine)


# =========================
# SAVE
# =========================

def save_items(items):
    session = Session()
    saved = 0
    skipped = 0

    for item in items:
        exists = session.query(RawDocument).filter_by(
            source=item["source"],
            source_url=item["source_url"],
        ).first()

        if exists:
            skipped += 1
            continue

        doc = RawDocument(
            source=item["source"],
            source_url=item["source_url"],
            title=item.get("title"),
            content=item.get("content"),
        )
        session.add(doc)
        saved += 1

    session.commit()
    session.close()
    return saved, skipped


# =========================
# MAIN
# =========================

async def run():
    auto_items = await fetch_auto_ru_serp(limit=30)
    avito_items = await fetch_avito_serp(limit=30)
    drom_items = fetch_drom_ru(limit=30)

    total = auto_items + avito_items + drom_items
    saved, skipped = save_items(total)

    print(
        f"[INGEST-WORKER] fetched={len(total)} "
        f"saved={saved} skipped={skipped}"
    )


if __name__ == "__main__":
    asyncio.run(run())
