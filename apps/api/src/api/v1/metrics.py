from fastapi import APIRouter
from db.session import SessionLocal
from db.models import RawDocument, NormalizedDocument
from integrations.vector_db.qdrant import QdrantStore
from core.settings import settings

router = APIRouter()
store = QdrantStore()
client = store.client


@router.get("/metrics")
def get_metrics():
    db = SessionLocal()

    total_raw = db.query(RawDocument).count()
    total_normalized = db.query(NormalizedDocument).count()

    try:
        collection_info = client.get_collection(
            collection_name=settings.qdrant_collection
        )
        total_chunks = collection_info.points_count
    except Exception:
        total_chunks = 0

    db.close()

    return {
        "total_raw_documents": total_raw,
        "total_normalized_documents": total_normalized,
        "total_chunks_in_qdrant": total_chunks,
    }