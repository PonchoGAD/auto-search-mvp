from db.session import SessionLocal
from db.models import RawDocument

from integrations.sources.telegram import fetch_telegram


def run_ingest():
    """
    Ingest pipeline:
    Telegram -> RawDocument

    Источник каналов:
    ENV TG_CHANNELS=@avito_auto,@cars_ru
    """

    session = SessionLocal()

    try:
        items = fetch_telegram()

        if not items:
            print("[INGEST][WARN] telegram returned 0 items")
            return

        saved = 0
        skipped = 0

        for item in items:
            exists = (
                session.query(RawDocument)
                .filter(RawDocument.source_url == item["source_url"])
                .first()
            )
            if exists:
                skipped += 1
                continue

            doc = RawDocument(
                source=item["source"],          # "telegram"
                source_url=item["source_url"],  # https://t.me/...
                title=item["title"],
                content=item["content"],
            )
            session.add(doc)
            saved += 1

        session.commit()

        print(
            f"[INGEST][TELEGRAM] saved: {saved}, skipped (duplicates): {skipped}"
        )

    finally:
        session.close()
