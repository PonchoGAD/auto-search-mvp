# apps/api/src/integrations/embeddings/embedder.py
from typing import List
from sentence_transformers import SentenceTransformer

_model = None

def get_model():
    global _model
    if _model is None:
        print("[API][EMBED] loading model: intfloat/multilingual-e5-base", flush=True)
        _model = SentenceTransformer("intfloat/multilingual-e5-base")
        print("[API][EMBED] model loaded", flush=True)
    return _model

def embed_query(text: str) -> List[float]:
    model = get_model()
    vec = model.encode(f"query: {text}")
    return vec.tolist() if hasattr(vec, "tolist") else list(vec)

def embed_passage(text: str) -> List[float]:
    model = get_model()
    vec = model.encode(f"passage: {text}")
    return vec.tolist() if hasattr(vec, "tolist") else list(vec)

