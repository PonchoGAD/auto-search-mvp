from typing import Any, Dict, List

from fastapi import APIRouter
from qdrant_client import QdrantClient

from db.session import SessionLocal
from db.models import RawDocument
from integrations.vector_db.qdrant import QdrantStore

from core.settings import settings



router = APIRouter(prefix="/api/v1/admin", tags=["admin"])


# =====================================================
# MAIN INDEX STATS (QDRANT + DB)
# =====================================================

@router.get("/index-stats")
def get_index_stats() -> Dict[str, Any]:
    """
    Returns:
    - total points in Qdrant (main collection)
    - total RawDocument count in DB
    - last 10 RawDocuments
    """

    # =========================
    # QDRANT MAIN COLLECTION STATS
    # =========================
    qdrant_store = QdrantStore()

    try:
        collection_info = qdrant_store.client.get_collection(
            collection_name=settings.qdrant_collection
        )
        qdrant_points = collection_info.points_count or 0
        qdrant_vectors = collection_info.vectors_count or 0
        qdrant_status = collection_info.status
    except Exception:
        qdrant_points = 0
        qdrant_vectors = 0
        qdrant_status = "unknown"

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
        "qdrant_vectors": qdrant_vectors,
        "qdrant_status": qdrant_status,
        "db_documents": db_count,
        "last_documents": last_documents,
    }


# =====================================================
# FULL QDRANT COLLECTION STATS
# =====================================================

@router.get("/collections-stats")
def index_stats() -> Dict[str, Any]:
    """
    Returns stats for ALL Qdrant collections
    """

    client = QdrantClient(
        host=settings.qdrant_host,
        port=settings.qdrant_port,
        check_compatibility=False,
    )

    collections = client.get_collections().collections

    stats = {}

    for collection in collections:
        info = client.get_collection(collection.name)
        stats[collection.name] = {
            "points_count": info.points_count,
            "vectors_count": info.vectors_count,
            "status": info.status,
        }

    return {
        "collections": stats
    }
