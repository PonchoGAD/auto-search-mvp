from db.session import SessionLocal, engine
from db.models import Base, NormalizedDocument, DocumentChunk


def chunk_text_by_chars(text: str, size: int = 1500) -> list[str]:
    return [text[i:i + size] for i in range(0, len(text), size) if text[i:i + size]]


def run_chunk(limit: int = 500):
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
        if exists:
            continue

        chunks = chunk_text_by_chars(doc.normalized_text or "")

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

    print(f"[CHUNK] saved: {saved}")
