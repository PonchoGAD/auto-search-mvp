import asyncio
import os
from sqlalchemy.orm import sessionmaker

from db.session import engine, SessionLocal, Base
from db.models import RawDocument

from sources.auto_ru import fetch_auto_ru_serp
from sources.avito import fetch_avito_serp
from sources.drom import fetch_drom_ru

# 🔴 КЛЮЧЕВО
Base.metadata.create_all(bind=engine)


def save_items(items):
    session = SessionLocal()
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

        session.add(
            RawDocument(
                source=item["source"],
                source_url=item["source_url"],
                title=item["title"],
                content=item["content"],
            )
        )
        saved += 1

    session.commit()
    session.close()
    return saved, skipped


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
