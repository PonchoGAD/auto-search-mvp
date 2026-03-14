# apps/api/src/services/query_parser.py

from typing import Optional, Dict, Any
import re
from datetime import datetime

from domain.query_schema import StructuredQuery
from services.brand_detector import detect_brand
from services.model_resolver import resolve_model
from services.query_normalizer import normalize_query


def _parse_price_value(num_str: str, unit: Optional[str]) -> Optional[int]:
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

    if unit_norm in {"млн", "миллион", "миллиона", "миллионов", "m"}:
        value *= 1_000_000
    elif unit_norm in {"тыс", "тысяч", "к", "k"}:
        value *= 1_000
    elif unit_norm in {"₽", "руб", "руб.", "р", "р."}:
        pass
    elif not unit_norm:
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


def _parse_mileage_value(num_str: str, unit: Optional[str]) -> Optional[int]:
    if not num_str:
        return None

    raw = str(num_str).strip().replace(" ", "").replace(",", ".")
    if not raw:
        return None

    try:
        value = float(raw)
    except Exception:
        return None

    unit_norm = re.sub(r"\s+", " ", (unit or "").strip().lower())

    if unit_norm in {"тыс км", "тысяч км", "т.км", "т км", "tkm", "k km"}:
        value *= 1_000
    elif unit_norm in {"км", "km"}:
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
    if not text:
        return False

    mileage_patterns = [
        r"\bпробег\b",
        r"\bкм\b",
        r"\bт\.?\s*км\b",
        r"\bтыс\.?\s*км\b",
        r"\bтысяч\s*км\b",
    ]

    return any(re.search(pattern, text, re.IGNORECASE) for pattern in mileage_patterns)


def _parse_with_llm(raw_text: str) -> Dict[str, Any]:
    raise RuntimeError("LLM not implemented yet")


def parse_query(raw_text: str) -> StructuredQuery:
    raw_text = (raw_text or "").strip()
    normalized_text = normalize_query(raw_text)

    if not raw_text:
        return StructuredQuery(raw_query=raw_text)

    try:
        llm_result = _parse_with_llm(normalized_text)
        structured = StructuredQuery(**llm_result)
        structured.raw_query = raw_text
        return structured
    except Exception:
        return _parse_with_fallback(raw_text, normalized_text)


def _parse_with_fallback(raw_text: str, normalized_text: str) -> StructuredQuery:
    text = (normalized_text or "").lower().strip()
    result = StructuredQuery(raw_query=raw_text)

    current_year = datetime.utcnow().year

    # -------------------------
    # BRAND
    # -------------------------
    brand, confidence = detect_brand(
        title=raw_text or "",
        text=text,
    )
    if brand:
        result.brand = brand.lower()
        result.brand_confidence = confidence

    # -------------------------
    # MODEL
    # -------------------------
    if result.brand:
        model = resolve_model(result.brand, text)
        if model:
            result.model = model

    # -------------------------
    # MILEAGE (max)
    # -------------------------
    mileage_patterns = [
        r"\bпробег\s*до\s*(\d+(?:[\s.,]\d+)?)\s*(тыс\.?\s*км|тысяч\s*км|т\.?\s*км|км|km)\b",
        r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*(тыс\.?\s*км|тысяч\s*км|т\.?\s*км|км|km)\b",
        r"\b(\d+(?:[\s.,]\d+)?)\s*(тыс\.?\s*км|тысяч\s*км|т\.?\s*км)\b",
    ]

    for pattern in mileage_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            value = _parse_mileage_value(match.group(1), match.group(2))
            if value is not None:
                result.mileage_max = value
                break

    mileage_context = _has_mileage_context(text)

    # -------------------------
    # PRICE (max)
    # -------------------------
    if result.mileage_max is None and not mileage_context:
        price_patterns_with_limit = [
            r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*(млн|миллион|миллиона|миллионов|m)\b",
            r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*(тыс|тысяч|к|k)\b",
            r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*(₽|руб\.?|р\.?)\b",
            r"\bдо\s*(\d[\d\s]{4,})\b",
        ]

        for pattern in price_patterns_with_limit:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                unit = match.group(2) if len(match.groups()) > 1 else None
                value = _parse_price_value(match.group(1), unit)
                if value is not None:
                    result.price_max = value
                    break

    if result.price_max is None and result.mileage_max is None and not mileage_context:
        price_patterns_soft = [
            r"\b(\d+(?:[\s.,]\d+)?)\s*(млн|миллион|миллиона|миллионов|m)\b",
            r"\b(\d+(?:[\s.,]\d+)?)\s*(тыс|тысяч|к|k)\b",
            r"\b(\d+(?:[\s.,]\d+)?)\s*(₽|руб\.?|р\.?)\b",
        ]

        for pattern in price_patterns_soft:
            match = re.search(pattern, text, re.IGNORECASE)
            if match:
                value = _parse_price_value(match.group(1), match.group(2))
                if value is not None:
                    result.price_max = value
                    break

    # -------------------------
    # YEAR_MIN
    # -------------------------

    match = re.search(
        r"\b(от|с|после)\s*(19\d{2}|20\d{2})\b",
        text,
        re.IGNORECASE,
    )
    if match:
        year_val = int(match.group(2))
        if 1985 <= year_val <= current_year + 1:
            result.year_min = year_val

    if result.year_min is None:
        match = re.search(
            r"\bне\s+старше\s*(19\d{2}|20\d{2})(?:\s*г(?:\.|ода)?)?\b",
            text,
            re.IGNORECASE,
        )
        if match:
            year_val = int(match.group(1))
            if 1985 <= year_val <= current_year + 1:
                result.year_min = year_val

    if result.year_min is None:
        match = re.search(
            r"\b(не\s+ниже|не\s+раньше)\s*(19\d{2}|20\d{2})(?:\s*г(?:\.|ода)?)?\b",
            text,
            re.IGNORECASE,
        )
        if match:
            year_val = int(match.group(2))
            if 1985 <= year_val <= current_year + 1:
                result.year_min = year_val

    if result.year_min is None:
        match = re.search(
            r"\b(младше|за\s+последние)\s*(\d+)\s*лет\b",
            text,
            re.IGNORECASE,
        )
        if match:
            years = int(match.group(2))
            if 0 <= years <= 30:
                result.year_min = current_year - years

    # -------------------------
    # FUEL
    # -------------------------
    fuel_patterns = [
        (r"\b(газ\s*/\s*бензин|бензин\s*/\s*газ|газ\s+бензин|бензин\s+газ)\b", "gas_petrol"),
        (r"\b(газ|гбо|lpg|cng|метан|пропан)\b", "gas"),
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
    if (
        "без окрас" in text
        or "без окраса" in text
        or "не бит" in text
        or "родная краска" in text
    ):
        result.paint_condition = "original"
    elif "крашен" in text or "бит" in text:
        result.paint_condition = "repainted"

    # -------------------------
    # CITY
    # -------------------------
    match = re.search(
        r"\b(москва|спб|питер|екатеринбург|казань|новосибирск|алматы|астана)\b",
        text,
    )
    if match:
        result.city = match.group(1)

    # -------------------------
    # EXCLUSIONS
    # -------------------------
    tokens = re.findall(r"[a-zа-я0-9]+", text)

    stop_tokens = {
        "до",
        "без",
        "и",
        "или",
        "не",
        "бит",
        "крашен",
        "км",
        "тыс",
        "тысяч",
        "руб",
        "р",
        "k",
        "m",
        "от",
        "с",
        "после",
        "старше",
        "младше",
        "лет",
        "год",
        "года",
        "бенз",
        "бензин",
        "petrol",
        "gasoline",
        "дизель",
        "диз",
        "diesel",
        "гибрид",
        "hybrid",
        "электро",
        "электр",
        "electric",
        "ev",
        "млн",
        "миллион",
        "миллиона",
        "миллионов",
        "к",
        "пробег",
        "газ",
        "гбо",
        "lpg",
        "cng",
        "метан",
        "пропан",
    }

    for token in tokens:
        if token.isdigit():
            continue

        if result.brand and token == result.brand:
            continue

        if result.model and token == result.model:
            continue

        if token.startswith("не") and len(token) > 2:
            exclusion = token[1:]
            if exclusion not in stop_tokens and exclusion not in result.exclusions:
                result.exclusions.append(exclusion)

    return result