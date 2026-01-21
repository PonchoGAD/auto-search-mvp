# apps/api/src/services/query_parser.py

from typing import List, Optional
from pydantic import BaseModel, Field, ValidationError
import re

class StructuredQuery(BaseModel):
    brand: Optional[str] = None
    model: Optional[str] = None
    price_max: Optional[int] = None
    mileage_max: Optional[int] = None
    fuel: Optional[str] = None
    paint_condition: Optional[str] = None
    city: Optional[str] = None

    keywords: List[str] = Field(default_factory=list)
    exclusions: List[str] = Field(default_factory=list)

    class Config:
        extra = "forbid"

def parse_query(raw_text: str) -> StructuredQuery:
    """
    Главная точка входа.
    Никогда не бросает исключения наружу.
    Всегда возвращает StructuredQuery.
    """
    raw_text = raw_text.strip()

    # 1. Пытаемся через LLM
    try:
        llm_result = _parse_with_llm(raw_text)
        return StructuredQuery(**llm_result)
    except Exception:
        # 2. Fallback
        return _parse_with_fallback(raw_text)

def _parse_with_llm(raw_text: str) -> dict:
    """
    Здесь ТОЛЬКО вызов LLM.
    Если LLM:
    - недоступен
    - вернул невалидный JSON
    - вернул лишние поля
    → исключение
    """

    # ⚠️ Заглушка — реальный LLM подключим позже
    raise RuntimeError("LLM not implemented yet")
