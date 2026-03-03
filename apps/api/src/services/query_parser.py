from typing import List, Optional, Dict, Tuple
from pydantic import BaseModel, Field
import re
import yaml
from pathlib import Path
from datetime import datetime


# =========================
# LOAD BRANDS (SINGLE SOURCE OF TRUTH)
# =========================

def load_brands() -> Dict[str, Dict[str, List[str]]]:
    """
    Загружает brands.yaml один раз.
    Формат:
    {
      "bmw": {
        "en": [...],
        "ru": [...],
        "aliases": [...]
      }
    }
    """
    try:
        base_dir = Path(__file__).resolve().parent.parent
        brands_path = base_dir / "config" / "brands.yaml"

        with open(brands_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            return data.get("brands", {})

    except Exception as e:
        print(f"[QUERY][WARN] failed to load brands.yaml: {e}")
        return {}


BRANDS_CONFIG = load_brands()


# =========================
# SCHEMA
# =========================

class StructuredQuery(BaseModel):
    # 🔑 RAW QUERY (для retention / analytics)
    raw_query: Optional[str] = None

    # Основные поля
    brand: Optional[str] = None
    brand_confidence: float = 0.0
    model: Optional[str] = None
    price_max: Optional[int] = None
    mileage_max: Optional[int] = None
    year_min: Optional[int] = None  # 🆕
    fuel: Optional[str] = None
    paint_condition: Optional[str] = None
    city: Optional[str] = None
    region: Optional[str] = None

    # Дополнительно
    keywords: List[str] = Field(default_factory=list)
    exclusions: List[str] = Field(default_factory=list)

    class Config:
        extra = "forbid"


# =========================
# MAIN ENTRY
# =========================

def parse_query(raw_text: str) -> StructuredQuery:
    raw_text = (raw_text or "").strip()

    if not raw_text:
        return StructuredQuery(raw_query=raw_text)

    try:
        llm_result = _parse_with_llm(raw_text)
        sq = StructuredQuery(**llm_result)
        sq.raw_query = raw_text
        return sq

    except Exception:
        return _parse_with_fallback(raw_text)


# =========================
# LLM PLACEHOLDER
# =========================

def _parse_with_llm(raw_text: str) -> dict:
    raise RuntimeError("LLM not implemented yet")


# =========================
# FALLBACK PARSER (RULE-BASED)
# =========================

def _parse_with_fallback(raw_text: str) -> StructuredQuery:
    text = raw_text.lower()
    result = StructuredQuery(raw_query=raw_text)

    current_year = datetime.utcnow().year

    # -------------------------
    # BRAND (yaml-driven)
    # -------------------------
    brand, confidence = _extract_brand(text)
    if brand:
        result.brand = brand.lower()  # гарант canonical lowercase
        result.brand_confidence = confidence

    # -------------------------
    # PRICE (max)
    # -------------------------
    price_patterns = [
        r"(до|<=|<)?\s*(\d+[\d\s]*)\s*(млн|миллион|m)",
        r"(до|<=|<)?\s*(\d+[\d\s]*)\s*(тыс|к)",
        r"(до|<=|<)?\s*(\d+[\d\s]*)\s*(₽|руб|р\.|\$|€)",
    ]

    for p in price_patterns:
        m = re.search(p, text)
        if m:
            value = int(m.group(2).replace(" ", ""))
            unit = m.group(3)

            if unit in ["млн", "миллион", "m"]:
                value *= 1_000_000
            elif unit in ["тыс", "к"]:
                value *= 1_000

            result.price_max = value
            break

    # -------------------------
    # MILEAGE (max)
    # -------------------------
    m = re.search(r"до\s*(\d+[\d\s]*)\s*(км|тыс)", text)
    if m:
        mileage = int(m.group(1).replace(" ", ""))
        if m.group(2) == "тыс":
            mileage *= 1_000
        result.mileage_max = mileage

    # -------------------------
    # YEAR_MIN (новое)
    # -------------------------

    # от 2018 / с 2020 / после 2017
    m = re.search(r"(от|с|после)\s*(20\d{2}|19\d{2})", text)
    if m:
        result.year_min = int(m.group(2))

    # не старше 10 лет / младше 7 лет / за последние 5 лет
    m = re.search(r"(не\s+старше|младше|за\s+последние)\s*(\d+)\s*лет", text)
    if m:
        years = int(m.group(2))
        result.year_min = current_year - years

    # -------------------------
    # FUEL (strict lowercase normalization)
    # -------------------------
    if "бенз" in text:
        result.fuel = "petrol"
    elif "диз" in text:
        result.fuel = "diesel"
    elif "гибрид" in text:
        result.fuel = "hybrid"
    elif "электро" in text or "электр" in text:
        result.fuel = "electric"

    if result.fuel:
        result.fuel = result.fuel.lower()

    # -------------------------
    # PAINT CONDITION
    # -------------------------
    if "без окрас" in text or "не бит" in text or "родная краска" in text:
        result.paint_condition = "original"
    elif "крашен" in text or "бит" in text:
        result.paint_condition = "repainted"

    # -------------------------
    # CITY
    # -------------------------
    m = re.search(
        r"\b(москва|спб|питер|екатеринбург|казань|новосибирск|алматы|астана)\b",
        text,
    )
    if m:
        result.city = m.group(1)

    # -------------------------
    # RECENCY INTENT
    # -------------------------
    if any(w in text for w in ["свеж", "нов", "последн"]):
        result.keywords.append("recent")

    # -------------------------
    # KEYWORDS / EXCLUSIONS (hardened)
    # -------------------------
    tokens = re.findall(r"[a-zа-я0-9]+", text)

    STOP_TOKENS = {
        "до", "без", "и", "или", "не",
        "бит", "крашен",
        "км", "тыс", "руб", "р", "₽",
        "от", "с", "после", "старше", "младше",
        "лет", "год", "года",
    }

    brand_synonyms = set()
    if result.brand and result.brand in BRANDS_CONFIG:
        cfg = BRANDS_CONFIG[result.brand]
        for w in cfg.get("en", []) + cfg.get("ru", []) + cfg.get("aliases", []):
            brand_synonyms.add(w.lower())

    for t in tokens:
        if t.isdigit():
            continue

        if result.brand and t == result.brand:
            continue

        if t in brand_synonyms:
            continue

        if t.startswith("не") and len(t) > 2:
            result.exclusions.append(t[1:])
        elif t not in STOP_TOKENS and t not in result.keywords:
            result.keywords.append(t)

    return result


# =========================
# BRAND EXTRACTION LOGIC
# =========================

def _extract_brand(text: str) -> Tuple[Optional[str], float]:
    """
    Возвращает:
    - canonical brand
    - confidence:
        exact → 1.0
        alias → 0.8
        fuzzy → 0.6
    """
    for brand, cfg in BRANDS_CONFIG.items():
        for w in cfg.get("en", []) + cfg.get("ru", []):
            if re.search(rf"\b{re.escape(w.lower())}\b", text):
                return brand.lower(), 1.0

        for a in cfg.get("aliases", []):
            if re.search(rf"\b{re.escape(a.lower())}\b", text):
                return brand.lower(), 0.8

        for w in cfg.get("en", []) + cfg.get("ru", []):
            if w.lower() in text:
                return brand.lower(), 0.6

    return None, 0.0