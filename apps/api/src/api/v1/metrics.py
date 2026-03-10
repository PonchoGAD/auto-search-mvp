from fastapi import APIRouter
from db.session import SessionLocal
from db.models import RawDocument, NormalizedDocument
from integrations.vector_db.qdrant import qdrant_client
from core.settings import settings

router = APIRouter()

@router.get("/metrics")
def get_metrics():
    db = SessionLocal()

    try:
        total_raw = db.query(RawDocument).count()
        total_normalized = db.query(NormalizedDocument).count()
    finally:
        db.close()

    try:
        collection_info = qdrant_client.get_collection(settings.QDRANT_COLLECTION)
        total_chunks = collection_info.points_count
    except Exception:
        total_chunks = 0

    return {
        "total_raw_documents": total_raw,
        "total_normalized_documents": total_normalized,
        "total_chunks_in_qdrant": total_chunks,
    }