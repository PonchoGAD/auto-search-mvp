# apps/api/src/services/query_parser.py

from typing import List, Optional
from pydantic import BaseModel, Field
import re


# =========================
# SCHEMA
# =========================

class StructuredQuery(BaseModel):
    # 🔑 RAW QUERY (для retention / analytics)
    raw_query: Optional[str] = None

    # Основные поля
    brand: Optional[str] = None
    model: Optional[str] = None
    price_max: Optional[int] = None
    mileage_max: Optional[int] = None
    fuel: Optional[str] = None
    paint_condition: Optional[str] = None
    city: Optional[str] = None

    # Дополнительно
    keywords: List[str] = Field(default_factory=list)
    exclusions: List[str] = Field(default_factory=list)

    class Config:
        extra = "forbid"


# =========================
# MAIN ENTRY
# =========================

def parse_query(raw_text: str) -> StructuredQuery:
    """
    Главная точка входа.
    Никогда не бросает исключения наружу.
    Всегда возвращает StructuredQuery.
    """
    raw_text = (raw_text or "").strip()

    if not raw_text:
        return StructuredQuery(raw_query=raw_text)

    # 1️⃣ Пытаемся через LLM (позже)
    try:
        llm_result = _parse_with_llm(raw_text)
        sq = StructuredQuery(**llm_result)
        sq.raw_query = raw_text
        return sq

    except Exception:
        # 2️⃣ Надёжный fallback
        return _parse_with_fallback(raw_text)


# =========================
# LLM PLACEHOLDER
# =========================

def _parse_with_llm(raw_text: str) -> dict:
    """
    Заглушка под будущий LLM.
    Любая ошибка → fallback.
    """
    raise RuntimeError("LLM not implemented yet")


# =========================
# FALLBACK PARSER (RULE-BASED)
# =========================

def _parse_with_fallback(raw_text: str) -> StructuredQuery:
    text = raw_text.lower()

    result = StructuredQuery(raw_query=raw_text)

    # -------------------------
    # BRAND (simple, expandable)
    # -------------------------
    BRAND_MAP = {
        "bmw": ["bmw", "бмв"],
        "audi": ["audi", "ауди"],
        "mercedes": ["mercedes", "mercedes-benz", "мерседес", "мерс"],
        "toyota": ["toyota", "тойота"],
        "lexus": ["lexus", "лексус"],
        "volkswagen": ["volkswagen", "vw", "фольксваген"],
    }

    for brand, aliases in BRAND_MAP.items():
        for a in aliases:
            if a in text:
                result.brand = brand
                break
        if result.brand:
            break

    # -------------------------
    # PRICE (max)
    # -------------------------
    m = re.search(
        r"(до|<=|<)?\s*(\d[\d\s]{2,10})\s*(₽|руб|р\.|тыс|к|\$|€)",
        text,
    )
    if m:
        price = int(m.group(2).replace(" ", ""))
        if m.group(3) in ["тыс", "к"]:
            price *= 1000
        result.price_max = price

    # -------------------------
    # MILEAGE (max)
    # -------------------------
    m = re.search(r"до\s*(\d[\d\s]{1,8})\s*(км|тыс)", text)
    if m:
        mileage = int(m.group(1).replace(" ", ""))
        if m.group(2) == "тыс":
            mileage *= 1000
        result.mileage_max = mileage

    # -------------------------
    # FUEL
    # -------------------------
    if "бенз" in text:
        result.fuel = "petrol"
    elif "диз" in text:
        result.fuel = "diesel"
    elif "гибрид" in text:
        result.fuel = "hybrid"
    elif "электро" in text:
        result.fuel = "electric"

    # -------------------------
    # PAINT CONDITION
    # -------------------------
    if "без окрас" in text or "не бит" in text:
        result.paint_condition = "original"
    elif "крашен" in text or "бит" in text:
        result.paint_condition = "repainted"

    # -------------------------
    # CITY (мягко, MVP)
    # -------------------------
    m = re.search(
        r"\b(москва|спб|питер|екатеринбург|казань|новосибирск)\b",
        text,
    )
    if m:
        result.city = m.group(1)

    # -------------------------
    # KEYWORDS / EXCLUSIONS
    # -------------------------
    tokens = re.findall(r"[a-zа-я0-9]+", text)

    STOP_TOKENS = {
        "до", "без", "и", "или", "не",
        "бит", "крашен",
        "км", "тыс", "руб", "р", "₽",
    }

    for t in tokens:
        if t.startswith("не") and len(t) > 2:
            result.exclusions.append(t[1:])
        elif t not in STOP_TOKENS:
            result.keywords.append(t)

    return result
