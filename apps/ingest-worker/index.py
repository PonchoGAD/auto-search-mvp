# DEPRECATED
# Worker must not own a separate index pipeline.
# Use API pipeline: apps/api/src/data_pipeline/index.py

raise RuntimeError(
    "apps/ingest-worker/index.py is deprecated. "
    "Use src.data_pipeline.index from API pipeline."
)