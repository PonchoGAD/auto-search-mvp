from sqlalchemy import Column, Integer, String, Text, DateTime
from datetime import datetime

from .base import Base


class RawDocument(Base):
    __tablename__ = "raw_documents"

    id = Column(Integer, primary_key=True)
    source = Column(String, index=True)
    source_url = Column(String, unique=True, index=True)
    title = Column(String)
    content = Column(Text)
    fetched_at = Column(DateTime, default=datetime.utcnow)
