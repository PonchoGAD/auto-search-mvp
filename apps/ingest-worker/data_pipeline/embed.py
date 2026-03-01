import os
from typing import List
from sentence_transformers import SentenceTransformer

_model = None

def get_model():
    global _model
    if _model is None:
        print("[EMBED] loading model: intfloat/multilingual-e5-base", flush=True)
        _model = SentenceTransformer("intfloat/multilingual-e5-base")
        print("[EMBED] model loaded", flush=True)
    return _model

def _to_list(vec):
    # vec может быть numpy.ndarray или list
    if hasattr(vec, "tolist"):
        return vec.tolist()
    return list(vec)

def embed_text(text: str):
    model = get_model()
    vec = model.encode(text)
    return _to_list(vec)