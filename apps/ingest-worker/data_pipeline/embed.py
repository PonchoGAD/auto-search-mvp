import os
from typing import List
from sentence_transformers import SentenceTransformer

_model = None


def get_model() -> SentenceTransformer:
    """
    Lazy model initialization.
    Модель загружается только при первом вызове embed_text().
    Это предотвращает блокировку старта ingest-worker.
    """
    global _model

    if _model is None:
        model_name = os.getenv(
            "EMBED_MODEL",
            "intfloat/multilingual-e5-base",
        )

        print(f"[EMBED] loading model: {model_name}")

        _model = SentenceTransformer(model_name)

        print("[EMBED] model loaded")

    return _model


def embed_text(text: str) -> List[float]:
    """
    Encode text into embedding vector.
    """
    if not text:
        return []

    model = get_model()

    vector = model.encode(
        f"passage: {text}",
        normalize_embeddings=True,
    ).tolist()

    return vector.tolist()
