from sqlalchemy import (
    Column,
    Integer,
    String,
    Text,
    DateTime,
    Float,
    Boolean,
    JSON,
)
from sqlalchemy.sql import func
from datetime import datetime

from db.session import Base


# ======================================================
# RAW DOCUMENTS (сырые данные из источников)
# ======================================================

class RawDocument(Base):
    __tablename__ = "raw_documents"

    id = Column(Integer, primary_key=True)

    source = Column(String, index=True)
    source_url = Column(String, unique=True, index=True)

    title = Column(String)
    content = Column(Text)

    fetched_at = Column(DateTime, default=datetime.utcnow)


# ======================================================
# NORMALIZED DOCUMENTS (очищенные + извлечённые поля)
# ======================================================

class NormalizedDocument(Base):
    __tablename__ = "normalized_documents"

    id = Column(Integer, primary_key=True)

    raw_id = Column(Integer, index=True)  # без FK для MVP
    source = Column(String, index=True)
    source_url = Column(String, unique=True, index=True)

    title = Column(String)
    normalized_text = Column(Text)

    # извлечённые признаки (MVP)
    brand = Column(String, nullable=True)
    model = Column(String, nullable=True)
    year = Column(Integer, nullable=True)
    mileage = Column(Integer, nullable=True)
    price = Column(Integer, nullable=True)
    currency = Column(String, nullable=True)

    city = Column(String, nullable=True)
    region = Column(String, nullable=True)
    color = Column(String, nullable=True)
    fuel = Column(String, nullable=True)
    paint_condition = Column(String, nullable=True)
    condition = Column(String, nullable=True)

    fetched_at = Column(DateTime, default=datetime.utcnow)


# ======================================================
# DOCUMENT CHUNKS (чанки для векторного поиска)
# ======================================================

class DocumentChunk(Base):
    __tablename__ = "document_chunks"

    id = Column(Integer, primary_key=True)

    normalized_id = Column(Integer, index=True)  # без FK для MVP
    chunk_index = Column(Integer)

    chunk_text = Column(Text)
    quality_score = Column(Float, default=1.0)

    created_at = Column(DateTime, default=datetime.utcnow)


# ======================================================
# SEARCH METRICS (логирование запросов)
# ======================================================

class SearchEvent(Base):
    __tablename__ = "search_events"

    id = Column(Integer, primary_key=True)

    raw_query = Column(String, nullable=False)
    structured_query = Column(JSON, nullable=False)

    results_count = Column(Integer, nullable=False)
    latency_ms = Column(Integer, nullable=False)

    empty_result = Column(Boolean, default=False)

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )


class SearchHistory(Base):
    __tablename__ = "search_history"

    id = Column(Integer, primary_key=True)

    raw_query = Column(String, nullable=False)
    structured_query = Column(JSON, nullable=False)

    results_count = Column(Integer, nullable=False)
    empty_result = Column(Boolean, default=False)

    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
