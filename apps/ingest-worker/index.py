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

        indexed_count = 0

        for doc in docs:
            try:
                store.add_document(doc)
                doc.indexed = True
                indexed_count += 1
            except Exception as e:
                print(f"[INDEX][ERROR] doc_id={doc.id} error={e}")

        session.commit()

        print(f"[INDEX] done indexed={indexed_count}")

    finally:
        session.close()
