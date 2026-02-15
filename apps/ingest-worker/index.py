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

        for doc in docs:
            chunks = chunk_text(doc.content or "")

            for chunk in chunks:
                vector = embed_text(chunk)

                store.upsert([
                    store.build_point(
                        document=doc,
                        chunk_text=chunk,
                        vector=vector,
                    )
                ])

            doc.indexed = True

        session.commit()
        print("[INDEX] done")

    finally:
        session.close()
