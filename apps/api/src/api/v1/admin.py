from typing import Any, Dict, List

from fastapi import APIRouter

from db.session import SessionLocal
from db.models import RawDocument
from integrations.vector_db.qdrant import QdrantStore, COLLECTION_NAME


router = APIRouter(prefix="/api/v1/admin", tags=["admin"])


@router.get("/index-stats")
def get_index_stats() -> Dict[str, Any]:
    """
    Returns:
    - total points in Qdrant
    - total RawDocument count in DB
    - last 10 RawDocuments
    """

    # =========================
    # QDRANT STATS
    # =========================
    qdrant_store = QdrantStore()

    try:
        collection_info = qdrant_store.client.get_collection(
            collection_name=COLLECTION_NAME
        )
        qdrant_points = collection_info.points_count or 0
    except Exception:
        qdrant_points = 0

    # =========================
    # DATABASE STATS
    # =========================
    session = SessionLocal()

    try:
        db_count = session.query(RawDocument).count()

        last_docs: List[RawDocument] = (
            session.query(RawDocument)
            .order_by(RawDocument.id.desc())
            .limit(10)
            .all()
        )

        last_documents = [
            {
                "id": doc.id,
                "source": doc.source,
                "source_url": doc.source_url,
                "title": doc.title,
                "created_at": getattr(doc, "created_at", None),
            }
            for doc in last_docs
        ]

    finally:
        session.close()

    return {
        "qdrant_points": qdrant_points,
        "db_documents": db_count,
        "last_documents": last_documents,
    }
