# apps/api/src/integrations/vector_db/qdrant.py

from qdrant_client import QdrantClient
from qdrant_client.models import VectorParams, Distance, PointStruct
from typing import List


COLLECTION_NAME = "auto_search_chunks"


class QdrantStore:
    def __init__(self, host: str = "qdrant", port: int = 6333):
        self.client = QdrantClient(
            host=host,
            port=port,
            check_compatibility=False,  # важно для версии сервера 1.9.x
        )

    def create_collection(self, vector_size: int):
        collections = [
            c.name for c in self.client.get_collections().collections
        ]

        if COLLECTION_NAME in collections:
            return

        self.client.create_collection(
            collection_name=COLLECTION_NAME,
            vectors_config=VectorParams(
                size=vector_size,
                distance=Distance.COSINE,
            ),
        )

        print(f"[QDRANT] collection created: {COLLECTION_NAME}")

    def upsert(self, points: List[PointStruct]):
        if not points:
            print("[QDRANT] no points to upsert")
            return

        self.client.upsert(
            collection_name=COLLECTION_NAME,
            points=points,
        )

        print(f"[QDRANT] upserted points: {len(points)}")

    def search(self, vector: List[float], limit: int = 20):
        return self.client.search(
            collection_name=COLLECTION_NAME,
            query_vector=vector,
            limit=limit,
        )
