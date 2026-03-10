from typing import List, Optional, Dict, Tuple
from pydantic import BaseModel, Field
import re
import yaml
from services.brand_detector import detect_brand
from services.model_resolver import resolve_model
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


def _parse_price_value(num_str: str, unit: str | None) -> Optional[int]:
    if not num_str:
        return None

    raw = str(num_str).strip().replace(" ", "").replace(",", ".")
    if not raw:
        return None

    try:
        value = float(raw)
    except Exception:
        return None

    unit_norm = (unit or "").strip().lower()

    if unit_norm in {"млн", "миллион", "m"}:
        value *= 1_000_000
    elif unit_norm in {"тыс", "к", "k"}:
        value *= 1_000
    elif unit_norm in {"₽", "руб", "р", "р."}:
        pass
    elif unit is None:
        pass
    else:
        return None

    try:
        int_value = int(value)
    except Exception:
        return None

    if 10_000 <= int_value <= 200_000_000:
        return int_value

    return None


def _parse_mileage_value(num_str: str, unit: str | None) -> Optional[int]:
    if not num_str:
        return None

    raw = str(num_str).strip().replace(" ", "").replace(",", ".")
    if not raw:
        return None

    try:
        value = float(raw)
    except Exception:
        return None

    unit_norm = (unit or "").strip().lower()

    if unit_norm in {"тыс", "т.км", "k"}:
        value *= 1_000
    elif unit_norm in {"км", ""}:
        pass
    else:
        return None

    try:
        int_value = int(value)
    except Exception:
        return None

    if 0 <= int_value <= 500_000:
        return int_value

    return None


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
        result.brand = brand.lower()
        result.brand_confidence = confidence

    # -------------------------
    # MODEL (resolver-based)
    # -------------------------

    if result.brand:
        model = resolve_model(result.brand, text)
        if model:
            result.model = model

    # -------------------------
    # PRICE (max)
    # -------------------------
    price_patterns_with_limit = [
        r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*(млн|миллион|m)\b",
        r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*(тыс|к|k)\b",
        r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*(₽|руб|р\.?|р)\b",
        r"\bдо\s*(\d[\d\s]{4,})\b",
    ]

    for p in price_patterns_with_limit:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            unit = m.group(2) if len(m.groups()) > 1 else None
            value = _parse_price_value(m.group(1), unit)
            if value is not None:
                result.price_max = value
                break

    if result.price_max is None:
        price_patterns_soft = [
            r"\b(\d+(?:[\s.,]\d+)?)\s*(млн|миллион|m)\b",
            r"\b(\d+(?:[\s.,]\d+)?)\s*(тыс|к|k)\b",
            r"\b(\d+(?:[\s.,]\d+)?)\s*(₽|руб|р\.?|р)\b",
        ]

        for p in price_patterns_soft:
            m = re.search(p, text, re.IGNORECASE)
            if m:
                value = _parse_price_value(m.group(1), m.group(2))
                if value is not None:
                    result.price_max = value
                    break

    # -------------------------
    # MILEAGE (max)
    # -------------------------
    mileage_patterns = [
        r"\bпробег\s*до\s*(\d+(?:[\s.,]\d+)?)\s*(тыс|т\.км|k|км)?\b",
        r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*(тыс|т\.км|k|км)\b",
        r"\b(\d+(?:[\s.,]\d+)?)\s*(т\.км)\b",
    ]

    for p in mileage_patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            value = _parse_mileage_value(m.group(1), m.group(2))
            if value is not None:
                result.mileage_max = value
                break

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
    fuel_patterns = [
        (r"\b(бенз|бензин|petrol|gasoline)\b", "petrol"),
        (r"\b(диз|дизель|diesel)\b", "diesel"),
        (r"\b(гибрид|hybrid)\b", "hybrid"),
        (r"\b(электро|электр|electric|ev)\b", "electric"),
    ]

    for pattern, fuel_value in fuel_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            result.fuel = fuel_value
            break

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
        "км", "тыс", "руб", "р", "₽", "k", "m",
        "от", "с", "после", "старше", "младше",
        "лет", "год", "года",
        "бенз", "бензин", "petrol", "gasoline",
        "дизель", "диз", "diesel",
        "гибрид", "hybrid",
        "электро", "электр", "electric", "ev",
        "млн", "миллион", "к", "тысяч", "тысяс", "пробег"
    }

    brand_synonyms = set()
    if result.brand and result.brand in BRANDS_CONFIG:
        cfg = BRANDS_CONFIG[result.brand]
        for w in cfg.get("en", []) + cfg.get("ru", []) + cfg.get("aliases", []):
            if isinstance(w, str) and w.strip():
                brand_synonyms.add(w.lower())

    model_synonyms = set()
    if result.brand and result.brand in MODELS_CONFIG:
        brand_models = MODELS_CONFIG.get(result.brand, {})
        for model_name, aliases in brand_models.items():
            if isinstance(model_name, str) and model_name.strip():
                model_synonyms.add(model_name.lower())
            if isinstance(aliases, list):
                for alias in aliases:
                    if isinstance(alias, str) and alias.strip():
                        model_synonyms.add(alias.lower())

    for t in tokens:

        if t.isdigit():
            continue

        if result.brand and t == result.brand:
            continue

        if result.model and t == result.model:
            continue

        if t in brand_synonyms:
            continue

        if t in model_synonyms:
            continue

        if t.startswith("не") and len(t) > 2:
            exclusion = t[1:]
            if (
                exclusion not in STOP_TOKENS
                and exclusion not in brand_synonyms
                and exclusion not in model_synonyms
                and exclusion not in result.exclusions
            ):
                result.exclusions.append(exclusion)
        elif t not in STOP_TOKENS and t not in result.keywords:
            result.keywords.append(t)

    expand_query_keywords(result)

    return result