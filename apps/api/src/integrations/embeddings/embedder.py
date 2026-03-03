from typing import List
import hashlib
import random


class Embedder:
    """
    MVP deterministic embedder.

    ВАЖНО:
    VECTOR_SIZE должен совпадать с размером в Qdrant коллекции.
    Сейчас коллекция = 768.
    """

    VECTOR_SIZE = 768  # синхронизировано с Qdrant

    def embed(self, texts: List[str]) -> List[List[float]]:
        """
        Превращает список текстов в стабильные псевдо-векторы.
        Один и тот же текст -> один и тот же вектор.
        """
        vectors = []

        for text in texts:
            if not text:
                text = ""

            seed = int(hashlib.md5(text.encode()).hexdigest(), 16)
            random.seed(seed)

            vectors.append(
                [random.random() for _ in range(self.VECTOR_SIZE)]
            )

        return vectors


# =====================================================
# SINGLETON INSTANCE
# =====================================================

_embedder = Embedder()


# =====================================================
# PUBLIC API FUNCTIONS
# =====================================================

def embed_query(text: str) -> List[float]:
    """
    Используется SearchService.
    Возвращает один вектор.
    """
    return _embedder.embed([text])[0]


def embed_texts(texts: List[str]) -> List[List[float]]:
    """
    Используется при индексировании.
    """
    return _embedder.embed(texts)