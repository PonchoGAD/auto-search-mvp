from typing import List
import hashlib
import random

class Embedder:
    VECTOR_SIZE = 384  # фиксируем размер

    def embed(self, texts: List[str]) -> List[List[float]]:
        """
        MVP-заглушка:
        превращаем текст в стабильный псевдо-вектор
        """
        vectors = []
        for text in texts:
            seed = int(hashlib.md5(text.encode()).hexdigest(), 16)
            random.seed(seed)
            vectors.append([random.random() for _ in range(self.VECTOR_SIZE)])
        return vectors
