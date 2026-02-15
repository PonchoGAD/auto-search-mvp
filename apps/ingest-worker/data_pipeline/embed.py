from sentence_transformers import SentenceTransformer

model = SentenceTransformer("intfloat/multilingual-e5-base")


def embed_text(text: str):
    return model.encode(text).tolist()
