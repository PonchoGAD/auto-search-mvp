import os
from typing import List
from sentence_transformers import SentenceTransformer

# =====================================================
# CONFIG
# =====================================================

DEFAULT_MODEL = "intfloat/multilingual-e5-base"
EXPECTED_VECTOR_SIZE = 768

_model: SentenceTransformer | None = None
_model_name: str | None = None


# =====================================================
# MODEL LOADER (lazy, singleton)
# =====================================================

def get_model() -> SentenceTransformer:
    global _model, _model_name

    model_name = os.getenv("EMBED_MODEL", DEFAULT_MODEL)

    if _model is None or _model_name != model_name:
        print(f"[EMBED] loading model: {model_name}", flush=True)

        device = os.getenv("EMBED_DEVICE", "cpu")

        _model = SentenceTransformer(
            model_name,
            device=device
        )
        _model_name = model_name

        print(f"[EMBED] device={device}", flush=True)
        print("[EMBED] model loaded", flush=True)

    return _model


# =====================================================
# SINGLE TEXT
# =====================================================

def embed_text(text: str) -> List[float]:
    if not text:
        return []

    text = text.strip()

    if len(text) > 2000:
        text = text[:2000]

    model = get_model()

    vector = model.encode(
        text,
        normalize_embeddings=True,
    )

    vector = vector.tolist()

    if len(vector) != EXPECTED_VECTOR_SIZE:
        raise RuntimeError(
            f"Embedding size mismatch: got {len(vector)}, "
            f"expected {EXPECTED_VECTOR_SIZE}"
        )

    return vector


# =====================================================
# BATCH
# =====================================================

def embed_texts(texts: List[str]) -> List[List[float]]:
    if not texts:
        return []

    model = get_model()

    vectors = model.encode(
        texts,
        normalize_embeddings=True,
        batch_size=int(os.getenv("EMBED_BATCH", "64")),
    )

    vectors = vectors.tolist()

    for v in vectors:
        if len(v) != EXPECTED_VECTOR_SIZE:
            raise RuntimeError(
                f"Embedding size mismatch: got {len(v)}, "
                f"expected {EXPECTED_VECTOR_SIZE}"
            )

    return vectors