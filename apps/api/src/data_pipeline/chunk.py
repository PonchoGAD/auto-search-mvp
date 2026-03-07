#  apps\api\src\data_pipeline\chunk.py

from db.session import SessionLocal, engine
from db.models import Base, NormalizedDocument, DocumentChunk


def clean_text(text: str) -> str:
    if not text:
        return ""
    return " ".join(text.split())


def chunk_text_by_chars(text: str, size: int = 1500) -> list[str]:
    """
    Production chunker.

    - очищает текст
    - режет по символам
    - убирает слишком короткие куски
    """

    text = clean_text(text)

    if not text:
        return []

    chunks = [
        text[i:i + size]
        for i in range(0, len(text), size)
        if text[i:i + size]
    ]

    # фильтр слишком маленьких чанков
    filtered = [c for c in chunks if len(c) > 50]

    return filtered


def run_chunk(limit: int = 500, force_rebuild: bool = False):
    Base.metadata.create_all(bind=engine)
    session = SessionLocal()

    docs = (
        session.query(NormalizedDocument)
        .order_by(NormalizedDocument.id.desc())
        .limit(limit)
        .all()
    )

    if not docs:
        print("[CHUNK][WARN] no normalized documents found")
        session.close()
        return

    saved = 0

    for doc in docs:
        exists = (
            session.query(DocumentChunk)
            .filter_by(normalized_id=doc.id)
            .first()
        )

        if exists and not force_rebuild:
            continue

        if exists and force_rebuild:
            session.query(DocumentChunk).filter_by(normalized_id=doc.id).delete()
            session.flush()

        text = doc.normalized_text or ""

        if not text or len(text) < 30:
            continue

        chunks = chunk_text_by_chars(text)

        for idx, ch in enumerate(chunks):
            session.add(
                DocumentChunk(
                    normalized_id=doc.id,
                    chunk_index=idx,
                    chunk_text=ch,
                )
            )
            saved += 1

    session.commit()
    session.close()

    print(f"[CHUNK] chunks saved: {saved} from docs: {len(docs)}")