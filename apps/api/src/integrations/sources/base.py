# apps/api/src/integrations/sources/base.py

from abc import ABC, abstractmethod

class BaseSource(ABC):

    @abstractmethod
    def fetch(self) -> list[dict]:
        """
        Должен вернуть список словарей:
        {
          "url": str,
          "title": str,
          "content": str
        }
        """
        pass
