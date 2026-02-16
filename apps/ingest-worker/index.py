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
        docs: List[RawDocument] = (
            session.query(RawDocument)
            .filter(RawDocument.indexed == False)
            .limit(limit)
            .all()
        )

        if not docs:
            print("[INDEX] no new documents")
            return

        print(f"[INDEX] indexing {len(docs)} documents")

        collection_ready = False

        for doc in docs:
            text = (doc.content or "").strip()
            if not text:
                doc.indexed = True
                continue

            chunks = chunk_text(text)

            points = []

            for chunk in chunks:
                vector = embed_text(chunk)

                # 🔥 Гарантированное создание коллекции ДО первого upsert
                if not collection_ready:
                    store.create_collection(vector_size=len(vector))
                    collection_ready = True

                points.append(
                    store.build_point(
                        document=doc,
                        chunk_text=chunk,
                        vector=vector,
                    )
                )

            # upsert одним батчем по документу
            if points:
                store.upsert(points)

            doc.indexed = True

        session.commit()
        print("[INDEX] done")

    except Exception as e:
        session.rollback()
        print(f"[INDEX] error: {e}")
        raise

    finally:
        session.close()
