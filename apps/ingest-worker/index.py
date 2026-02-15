from db.session import SessionLocal
from db.models import RawDocument

from integrations.vector_db.qdrant import QdrantStore


def run_index(limit: int = 200):
    session = SessionLocal()
    store = QdrantStore()

    try:
        docs = (
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
            store.add_document(doc)
            doc.indexed = True

        session.commit()
        print("[INDEX] done")

    finally:
        session.close()
