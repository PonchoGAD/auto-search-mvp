from typing import List

from db.session import SessionLocal
from db.models import RawDocument

from vector_db.qdrant import QdrantStore
from data_pipeline.chunk import chunk_text
from data_pipeline.embed import embed_text


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
        vector_size = len(test_vector)

        # 🔥 2️⃣ Создаём коллекцию если нет
        store.create_collection(vector_size)

        total_chunks = 0

        for doc in docs:
            try:
                chunks = chunk_text(doc.content or "")
                points = []

                for chunk in chunks:
                    vector = embed_text(chunk)

                    point = store.build_point(
                        document=doc,
                        chunk_text=chunk,
                        vector=vector,
                    )

                    if point:
                        points.append(point)

                if points:
                    store.upsert(points)
                    total_chunks += len(points)

                doc.indexed = True
                session.commit()

                print(f"[INDEX] doc indexed ok url={doc.source_url}", flush=True)

            except Exception as e:
                session.rollback()
                print(
                    f"[INDEX][ERROR] doc failed url={doc.source_url} err={e}",
                    flush=True
                )
                continue

        print(f"[INDEX] done, chunks={total_chunks}")

        return total_chunks

    finally:
        session.close()