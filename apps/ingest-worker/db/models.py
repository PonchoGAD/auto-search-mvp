# apps/ingest-worker/db/models.py

from apps.shared.db.models import (
    DocumentChunk,
    NormalizedDocument,
    RawDocument,
    SearchEvent,
    SearchHistory,
)

__all__ = [
    "RawDocument",
    "NormalizedDocument",
    "DocumentChunk",
    "SearchEvent",
    "SearchHistory",
]