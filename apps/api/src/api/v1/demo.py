# apps/api/src/api/v1/demo.py

from fastapi import APIRouter, HTTPException
import os

from core.settings import settings

from data_pipeline.index import index_raw_documents

from db.session import SessionLocal
from db.models import RawDocument


router = APIRouter(tags=["Demo"])


# =====================================================
# ENV GUARDS
# =====================================================

def _check_demo_allowed():
    """
    Жёсткая защита:
    - demo ingest НЕЛЬЗЯ запускать случайно
    - в prod — только с флагом ENABLE_INGEST=true
    """

    if settings.env == "prod":
        enabled = os.getenv("ENABLE_INGEST", "false").lower() == "true"
        if not enabled:
            raise HTTPException(
                status_code=403,
                detail="Demo ingest disabled in prod. Set ENABLE_INGEST=true",
            )


# =====================================================
# INGEST DISABLED ENDPOINT
# =====================================================

@router.post("/ingest")
def ingest_disabled():
    return {"status": "disabled", "reason": "ingest runs in ingest-worker"}


# =====================================================
# DEMO SEED ENDPOINT
# =====================================================

@router.post(
    "/demo/seed",
    summary="Seed demo data (index only)",
)
def demo_seed():
    """
    DEMO ENDPOINT.

    Делает:
    1) Загружает последние документы
    2) index -> Qdrant
    3) возвращает результат

    Возвращает:
    {
        "saved": int,
        "indexed": int
    }
    """

    _check_demo_allowed()

    # -------------------------
    # LOAD RECENT RAW DOCS
    # -------------------------
    session = SessionLocal()
    try:
        raw_docs = (
            session.query(RawDocument)
            .order_by(RawDocument.id.desc())
            .limit(50)  # 🔒 hard demo cap
            .all()
        )
    finally:
        session.close()

    if not raw_docs:
        return {
            "saved": 0,
            "indexed": 0,
            "warning": "no documents ingested",
        }

    # -------------------------
    # INDEX -> QDRANT
    # -------------------------
    try:
        indexed = index_raw_documents(raw_docs)
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"indexing failed: {e}",
        )

    return {
        "saved": len(raw_docs),
        "indexed": indexed,
    }