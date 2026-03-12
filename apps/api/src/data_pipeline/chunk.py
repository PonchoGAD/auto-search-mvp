#  apps\api\src\data_pipeline\chunk.py

import re

from db.session import SessionLocal, engine
from db.models import Base, NormalizedDocument, DocumentChunk


def _clean_chunk_text(text: str) -> str:
    if not text:
        return ""

    text = text.replace("\u00A0", " ").replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _is_empty_chunk(text: str) -> bool:
    if not text:
        return True

    t = text.strip()

    if len(t) < 30:
        return True

    noise = [
        "подписывайтесь",
        "telegram",
        "t.me",
        "поделиться",
        "репост",
        "лайк",
    ]

    tl = t.lower()

    if any(x in tl for x in noise):
        return True

    return False


def clean_text(text: str) -> str:
    if not text:
        return ""
    return _clean_chunk_text(text)


def chunk_text_by_chars(text: str, size: int = 1200, overlap: int = 200) -> list[str]:
    if not text:
        return []

    text = _clean_chunk_text(text)
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
        sentence = _clean_chunk_text(sentence)
        if not sentence:
            continue

        if len(current) + len(sentence) + 1 <= size:
            current = f"{current} {sentence}".strip()
        else:
            if current:
                current = _clean_chunk_text(current)
                if not _is_empty_chunk(current):
                    if len(current) > 1200:
                        current = current[:1200]
                    chunks.append(current)

            tail = current[-overlap:] if current else ""
            current = _clean_chunk_text(f"{tail} {sentence}")

    if current:
        current = _clean_chunk_text(current)
        if not _is_empty_chunk(current):
            if len(current) > 1200:
                current = current[:1200]
            chunks.append(current)

    return [c for c in chunks if not _is_empty_chunk(c)]


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

        text = _clean_chunk_text(doc.normalized_text or "")

        if not text or len(text) < 30:
            continue

        chunks = chunk_text_by_chars(text)
        seen_chunks = set()

        for idx, chunk in enumerate(chunks):
            chunk = _clean_chunk_text(chunk)

            if _is_empty_chunk(chunk):
                continue

            if len(chunk) > 1200:
                chunk = chunk[:1200]

            if chunk in seen_chunks:
                continue

            seen_chunks.add(chunk)

            session.add(
                DocumentChunk(
                    normalized_id=doc.id,
                    chunk_index=idx,
                    chunk_text=chunk,
                )
            )
            saved += 1

    session.commit()
    session.close()

    print(f"[CHUNK] chunks saved: {saved} from docs: {len(docs)}")