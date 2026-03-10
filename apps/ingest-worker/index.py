from typing import List

from db.session import SessionLocal
from db.models import RawDocument

from vector_db.qdrant import QdrantStore
from data_pipeline.chunk import chunk_text
from shared.embeddings.provider import embed_text


BATCH_SIZE = 64  # upsert batching


def run_index(limit: int = 200):
    session = SessionLocal()
    store = QdrantStore()

    try:
        raw_new = session.query(RawDocument).filter(RawDocument.indexed == False).count()
        print(f"[INDEX] raw_new={raw_new}")

        docs: List[RawDocument] = (
            session.query(RawDocument)
            .filter(RawDocument.indexed == False)
            .limit(limit)
            .all()
        )

        if not docs:
            print("[INDEX] no new documents")
            return 0

        print(f"[INDEX] indexing {len(docs)} documents")

        # 🔥 1️⃣ Получаем размер вектора
        test_vector = embed_text("test")
        if not test_vector:
            print("[INDEX][ERROR] embed_text returned empty for test")
            return 0

        vector_size = len(test_vector)
        print(f"[INDEX] vector_size={vector_size}")

        if vector_size != 768:
            print(f"[INDEX][WARN] unexpected_vector_size={vector_size}")

        # 🔥 2️⃣ Создаём коллекцию если нет
        store.create_collection(vector_size)

        total_chunks = 0
        batch = []

        for doc in docs:
            # ограничиваем количество чанков (3–5)

            content = doc.content or ""

            if len(content) > 4000:
                content = content[:4000]

            chunks = chunk_text(content)[:8]

            for chunk in chunks:
                vector = embed_text(chunk)

                if not vector:
                    print(f"[INDEX][WARN] empty_vector doc_id={doc.id}")
                    continue

                if len(vector) != vector_size:
                    print(
                        f"[INDEX][WARN] bad_vector_size doc_id={doc.id} size={len(vector)}"
                    )
                    continue

                point = store.build_point(
                    document=doc,
                    chunk_text=chunk,
                    vector=vector,
                )

                if not point:
                    continue

                batch.append(point)
                total_chunks += 1

                # 🔥 batch upsert
                if len(batch) >= BATCH_SIZE:
                    store.upsert(batch)
                    print(f"[INDEX] upserted: {len(batch)}")
                    batch = []

            doc.indexed = True

        # финальный flush
        if batch:
            store.upsert(batch)
            print(f"[INDEX] upserted: {len(batch)}")

        session.commit()

        print(f"[INDEX] done, chunks={total_chunks}")

        return total_chunks

    finally:
        session.close()