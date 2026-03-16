from typing import Optional, Dict, List
import re
from datetime import datetime

from domain.query_schema import StructuredQuery
from services.query_normalizer import normalize_query
from services.taxonomy_service import taxonomy_service


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


def _has_mileage_context(text: str) -> bool:
    return bool(re.search(r"\b(пробег|км|т\.км|тыс\s*км)\b", text, re.IGNORECASE))


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
        sq = _parse_with_fallback(normalized_raw_text)
        sq.raw_query = raw_text
        return sq


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
    # BRAND (taxonomy-service driven)
    # -------------------------
    brand, model, confidence = taxonomy_service.resolve_entities(text)

    if brand:
        result.brand = brand
        result.brand_confidence = confidence

    if model:
        result.model = model

    # -------------------------
    # MILEAGE (max)
    # -------------------------
    mileage_patterns = [
        r"\bпробег\s*до\s*(\d+(?:[\s.,]\d+)?)\s*(тыс|т\.км|k|км)?\b",
        r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*(тыс|т\.км|k|км)\b",
        r"\b(\d+(?:[\s.,]\d+)?)\s*(т\.км)\b",
        r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*тыс\s*км\b",
        r"\bпробег\s*(\d+(?:[\s.,]\d+)?)\s*тыс\s*км\b",
    ]

    for p in mileage_patterns:
        m = re.search(p, text, re.IGNORECASE)
        if m:
            unit = m.group(2) if len(m.groups()) > 1 else "км"
            value = _parse_mileage_value(m.group(1), unit)
            if value is not None:
                result.mileage_max = value
                break

    mileage_context = _has_mileage_context(text)

    # -------------------------
    # PRICE (max)
    # -------------------------
    if result.mileage_max is None:
        price_patterns_with_limit = [
            r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*(млн|миллион|m)\b",
            r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*(тыс|к|k)\b",
            r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*(₽|руб|р\.?|р)\b",
            r"\bдо\s*(\d[\d\s]{4,})\b",
        ]

        for p in price_patterns_with_limit:
            m = re.search(p, text, re.IGNORECASE)
            if not m:
                continue

            matched = m.group(0).lower()
            if "км" in matched or "пробег" in text.lower():
                continue

            unit = m.group(2) if len(m.groups()) > 1 else None
            value = _parse_price_value(m.group(1), unit)
            if value is not None and not mileage_context:
                result.price_max = value
                break

    if result.price_max is None and result.mileage_max is None and not mileage_context:
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
    # YEAR_MIN
    # -------------------------

    m = re.search(r"\b(от|с|после)\s*(19\d{2}|20\d{2})\b", text, re.IGNORECASE)
    if m:
        year_val = int(m.group(2))
        if 1985 <= year_val <= current_year + 1:
            result.year_min = year_val

    if result.year_min is None:
        m = re.search(
            r"\bне\s+старше\s*(19\d{2}|20\d{2})(?:\s*г(?:\.|ода)?)?\b",
            text,
            re.IGNORECASE,
        )
        if m:
            year_val = int(m.group(1))
            if 1985 <= year_val <= current_year + 1:
                result.year_min = year_val

    if result.year_min is None:
        m = re.search(
            r"\b(не\s+ниже|не\s+раньше)\s*(19\d{2}|20\d{2})(?:\s*г(?:\.|ода)?)?\b",
            text,
            re.IGNORECASE,
        )
        if m:
            year_val = int(m.group(2))
            if 1985 <= year_val <= current_year + 1:
                result.year_min = year_val

    if result.year_min is None:
        m = re.search(
            r"\b(младше|за\s+последние)\s*(\d+)\s*лет\b",
            text,
            re.IGNORECASE,
        )
        if m:
            years = int(m.group(2))
            if 0 <= years <= 30:
                result.year_min = current_year - years

    # -------------------------
    # FUEL (strict lowercase normalization)
    # -------------------------
    fuel_patterns = [
        (r"\b(газ\s*/\s*бензин|бензин\s*/\s*газ)\b", "gas_petrol"),
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
    if result.brand:
        brand_synonyms = set(taxonomy_service.get_brand_aliases(result.brand))

    model_synonyms = set()
    if result.brand and result.model:
        model_synonyms = set(taxonomy_service.get_model_aliases(result.brand, result.model))
        model_synonyms.add(str(result.model).lower())

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

    return result