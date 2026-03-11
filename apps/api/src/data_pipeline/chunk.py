#  apps\api\src\data_pipeline\chunk.py

import re

from db.session import SessionLocal, engine
from db.models import Base, NormalizedDocument, DocumentChunk


def clean_text(text: str) -> str:
    if not text:
        return ""
    return " ".join(text.split())


def chunk_text_by_chars(text: str, size: int = 1200, overlap: int = 200) -> list[str]:
    if not text:
        return []

    text = clean_text(text)
    if not text:
        return []

    if "форум" in text.lower():
        text = text[:800]

    if "каталог" in text.lower():
        text = text[:800]

    sentences = re.split(r'(?<=[.!?])\s+', text)

    chunks = []
    current = ""

    for sentence in sentences:
        sentence = sentence.strip()
        if not sentence:
            continue

        if len(current) + len(sentence) + 1 <= size:
            current = f"{current} {sentence}".strip()
        else:
            if current:
                chunks.append(current)

            tail = current[-overlap:] if current else ""
            current = f"{tail} {sentence}".strip()

    if current:
        chunks.append(current)

    return [c for c in chunks if len(c) >= 80]


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