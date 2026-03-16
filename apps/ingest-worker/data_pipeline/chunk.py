# DEPRECATED
# Worker must not own a separate chunk pipeline.
# Use API pipeline: apps/api/src/data_pipeline/chunk.py

raise RuntimeError(
    "apps/ingest-worker/data_pipeline/chunk.py is deprecated. "
    "Use src.data_pipeline.chunk from API pipeline."
)