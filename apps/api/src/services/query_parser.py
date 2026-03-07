from typing import List, Optional, Dict, Tuple
from pydantic import BaseModel, Field
import re
import yaml
from services.brand_detector import detect_brand
from pathlib import Path
from datetime import datetime
from domain.query_schema import StructuredQuery
from services.query_normalizer import normalize_query


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
# BUILD BRAND INDEX (FAST LOOKUP)
# =========================

BRAND_TOKEN_INDEX = {}

for brand, cfg in BRANDS_CONFIG.items():

    tokens = set()

    tokens.update(cfg.get("en", []))
    tokens.update(cfg.get("ru", []))
    tokens.update(cfg.get("aliases", []))

    for t in tokens:
        BRAND_TOKEN_INDEX[t.lower()] = brand.lower()


# =========================
# LOAD MODELS CONFIG
# =========================

def load_models() -> Dict[str, Dict[str, List[str]]]:
    try:
        base_dir = Path(__file__).resolve().parent.parent
        models_path = base_dir / "config" / "models.yaml"

        with open(models_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        return data
    except Exception as e:
        print(f"[QUERY][WARN] failed to load models.yaml: {e}")
        return {}


MODELS_CONFIG = load_models()


MODEL_EXPANSION = {
    "bmw": {
        "3": ["3 series", "f30", "320", "328", "330"],
        "5": ["5 series", "f10", "g30", "520", "530"],
        "x5": ["f15", "g05"],
    },
    "toyota": {
        "camry": ["xv70", "xv50"],
        "land cruiser": ["lc200", "lc300"],
    },
    "mercedes": {
        "c": ["c200", "c300", "w205"],
        "e": ["e200", "e300", "w213"],
    }
}


# =========================
# MAIN ENTRY
# =========================

def parse_query(raw_text: str) -> StructuredQuery:
    raw_text = (raw_text or "").strip()
    normalized_raw_text = normalize_query(raw_text)

    if not raw_text:
        return StructuredQuery(raw_query=raw_text)

    try:
        llm_result = _parse_with_llm(normalized_raw_text)
        sq = StructuredQuery(**llm_result)
        sq.raw_query = raw_text
        return sq

    except Exception:
        return _parse_with_fallback(normalized_raw_text)


# =========================
# LLM PLACEHOLDER
# =========================

def _parse_with_llm(raw_text: str) -> dict:
    raise RuntimeError("LLM not implemented yet")


def expand_query_keywords(query: StructuredQuery):

    if not query.brand:
        return

    brand = query.brand.lower()
    brand_models = MODELS_CONFIG.get(brand, {})

    expanded = []

    if query.model:
        model_key = query.model.lower()
        if model_key in brand_models:
            expanded.extend(brand_models[model_key])

    raw_lower = (query.raw_query or "").lower()
    for model_key, values in brand_models.items():
        if model_key in raw_lower:
            expanded.extend(values)

    for v in expanded:
        if v not in query.keywords:
            query.keywords.append(v)


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
    brand, confidence = detect_brand(title=text, text=text)

    if brand:
        confidence = 1.0
        result.brand = brand.lower()
        result.brand_confidence = confidence

    # -------------------------
    # MODEL (basic extraction)
    # -------------------------

    if result.brand:

        MODEL_PATTERNS = {
            "bmw": ["x5", "x6", "x3", "m5", "m3"],
            "toyota": ["camry", "corolla", "land cruiser", "rav4"],
            "mercedes": ["c200", "e200", "e300", "gle", "gls"],
            "volkswagen": ["tiguan", "touareg", "polo", "passat"],
        }

        brand_models = MODEL_PATTERNS.get(result.brand, [])

        for m in brand_models:
            if m in text:
                result.model = m
                break

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

    m = re.search(r"(от|с|после)\s*(20\d{2}|19\d{2})", text)
    if m:
        result.year_min = int(m.group(2))

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
        "бенз", "бензин", "дизель", "диз", "гибрид", "электро",
        "млн", "миллион", "к", "тысяч", "тысяс", "пробег"
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

        if result.model and t == result.model:
            continue

        if t in brand_synonyms:
            continue

        if t.startswith("не") and len(t) > 2:
            result.exclusions.append(t[1:])
        elif t not in STOP_TOKENS and t not in result.keywords:
            result.keywords.append(t)

    expand_query_keywords(result)

    return result