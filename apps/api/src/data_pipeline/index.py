# =====================================================
# ⚠️ DEPRECATED INDEX MODULE
# =====================================================
#
# Single Source of Truth = ingest-worker.
# All indexing logic has been moved to:
# apps/ingest-worker/
#
# This module is kept only to prevent import errors.
# It performs NO indexing.
# =====================================================

def run_index(*args, **kwargs):
    print("[INDEX][DEPRECATED] api/data_pipeline/index.py is disabled. Use ingest-worker instead.")
    return 0


def index_raw_documents(*args, **kwargs):
    print("[INDEX][DEPRECATED] index_raw_documents() disabled. Use ingest-worker.")
    return 0