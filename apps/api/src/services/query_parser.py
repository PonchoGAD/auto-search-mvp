from typing import Optional, Dict, List, Tuple
import re
from datetime import datetime

from domain.query_schema import StructuredQuery
from services.query_normalizer import normalize_query
from services.taxonomy_service import taxonomy_service


MODEL_TOKEN_RE = re.compile(r"^[a-zа-я]+(?:[-_ ]?[a-zа-я0-9]+)\d[a-zа-я0-9-]*$", re.IGNORECASE)


def _normalize_spaces(text: str) -> str:
    text = (text or "").replace("\u00A0", " ").replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _normalize_parse_text(text: str) -> str:
    text = (text or "").lower()
    text = text.replace("ё", "е")

    # unify separators but preserve model structure
    text = re.sub(r"(?<=\d)\s+(?=км\b)", " ", text)
    text = re.sub(r"(?<=\d)\s+(?=тыс\b)", " ", text)

    # normalize compact price forms: 2м -> 2 млн ; 100к -> 100 тыс only if not mileage-context-resolved later
    text = re.sub(r"\b(\d+(?:[.,]\d+)?)\s*млн\b", r"\1 млн", text)
    text = re.sub(r"\b(\d+(?:[.,]\d+)?)\s*миллион(?:а|ов)?\b", r"\1 млн", text)
    text = re.sub(r"\b(\d+(?:[.,]\d+)?)\s*м\b", r"\1 млн", text)

    # keep 100к as-is for later context resolution
    text = re.sub(r"\b(\d+(?:[.,]\d+)?)\s*к\b", r"\1к", text)
    text = re.sub(r"\b(\d+(?:[.,]\d+)?)\s*k\b", r"\1к", text)

    # preserve popular model spellings
    text = re.sub(r"\be[\s-]?(\d{3})\b", r"e\1", text)
    text = re.sub(r"\bgx[\s-]?(\d{3})\b", r"gx\1", text)
    text = re.sub(r"\bcx[\s-]?(\d)\b", r"cx-\1", text)
    text = re.sub(r"\bcr[\s-]?v\b", "cr-v", text)
    text = re.sub(r"\bx[\s-]?trail\b", "x-trail", text)
    text = re.sub(r"\bs[\s-]?class\b", "s-class", text)
    text = re.sub(r"\bc[\s-]?class\b", "c-class", text)

    text = _normalize_spaces(text)
    return text


def _normalize_model_token(value: str) -> str:
    value = (value or "").strip().lower()
    value = value.replace("_", "-")
    value = re.sub(r"\s+", " ", value)
    value = re.sub(r"\b([a-zа-я]+)\s+(\d{1,4})\b", r"\1\2", value, flags=re.IGNORECASE)
    value = value.replace(" ", "-")
    value = re.sub(r"-{2,}", "-", value)
    return value.strip("-")


def _looks_like_model_token(token: str) -> bool:
    token = (token or "").strip().lower()
    if not token:
        return False

    if len(token) < 2:
        return False

    stop = {
        "до", "от", "с", "после", "не", "старше", "ниже", "раньше",
        "года", "год", "лет", "км", "тыс", "млн", "руб", "р", "бензин",
        "дизель", "гибрид", "электро", "electric", "hybrid", "diesel", "petrol",
        "пробег", "без", "окрас", "бит", "крашен",
    }
    if token in stop:
        return False

    if re.fullmatch(r"(19|20)\d{2}", token):
        return False

    if token.isdigit():
        return False

    if re.search(r"[a-zа-я]", token) and re.search(r"\d", token):
        return True

    if "-" in token and re.search(r"[a-zа-я]", token):
        return True

    if token in {"x5", "x6", "x7", "gle", "gls", "cls", "camry", "corolla", "rav4", "rav-4"}:
        return True

    return bool(MODEL_TOKEN_RE.match(token))


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

    if unit_norm in {"тыс", "т.км", "k", "к"}:
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
    return bool(re.search(r"\b(пробег|км|т\.км|тыс\s*км|\d+км|\d+к)\b", text, re.IGNORECASE))


def _extract_year_min(text: str, current_year: int) -> Optional[int]:
    patterns = [
        r"\b(?:от|с|после)\s*(19\d{2}|20\d{2})\b",
        r"\bне\s+старше\s*(19\d{2}|20\d{2})(?:\s*г(?:\.|ода)?)?\b",
        r"\b(?:не\s+ниже|не\s+раньше)\s*(19\d{2}|20\d{2})(?:\s*г(?:\.|ода)?)?\b",
    ]

    for pattern in patterns:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            year_val = int(m.group(1))
            if 1985 <= year_val <= current_year + 1:
                return year_val

    m = re.search(r"\b(?:младше|за\s+последние)\s*(\d+)\s*лет\b", text, re.IGNORECASE)
    if m:
        years = int(m.group(1))
        if 0 <= years <= 30:
            return current_year - years

    return None


def _extract_fuel(text: str) -> Optional[str]:
    fuel_patterns = [
        (r"\b(газ\s*/\s*бензин|бензин\s*/\s*газ)\b", "gas_petrol"),
        (r"\b(бенз|бензин|petrol|gasoline)\b", "petrol"),
        (r"\b(диз|дизель|diesel)\b", "diesel"),
        (r"\b(гибрид|hybrid)\b", "hybrid"),
        (r"\b(электро|электр|electric|ev)\b", "electric"),
    ]

    for pattern, fuel_value in fuel_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return fuel_value
    return None


def _extract_mileage_max(text: str) -> Optional[int]:
    patterns = [
        r"\bпробег\s*до\s*(\d+(?:[\s.,]\d+)?)\s*(тыс|т\.км|k|к|км)?\b",
        r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*(тыс\s*км|т\.км|kм|км|тыс|к)\b",
        r"\bдо\s*(\d{2,6})\s*км\b",
        r"\bдо\s*(\d+(?:[.,]\d+)?)к\b",
        r"\b(\d+(?:[\s.,]\d+)?)\s*т\.км\b",
        r"\bпробег\s*(\d+(?:[\s.,]\d+)?)\s*тыс\s*км\b",
    ]

    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if not m:
            continue

        num = m.group(1)
        unit = m.group(2) if len(m.groups()) > 1 else "км"

        unit_norm = (unit or "").lower().replace(" ", "")
        if unit_norm in {"тыскм", "тыс"}:
            unit_norm = "тыс"
        elif unit_norm in {"kм"}:
            unit_norm = "км"

        value = _parse_mileage_value(num, unit_norm)
        if value is not None:
            return value

    return None


def _extract_price_max(text: str, mileage_context: bool) -> Optional[int]:
    patterns_with_limit = [
        r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*(млн)\b",
        r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*(тыс|к)\b",
        r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*(₽|руб|р\.?|р)\b",
        r"\bдо\s*(\d[\d\s]{4,})\b",
    ]

    for p in patterns_with_limit:
        m = re.search(p, text, re.IGNORECASE)
        if not m:
            continue

        matched = m.group(0).lower()

        # mileage wins over ambiguous "до 100к"
        if "км" in matched:
            continue
        if mileage_context and (m.group(2).lower() if len(m.groups()) > 1 and m.group(2) else "") in {"к", "тыс"}:
            continue

        unit = m.group(2) if len(m.groups()) > 1 else None
        value = _parse_price_value(m.group(1), unit)
        if value is not None:
            return value

    soft_patterns = [
        r"\b(\d+(?:[\s.,]\d+)?)\s*(млн)\b",
        r"\b(\d+(?:[\s.,]\d+)?)\s*(тыс)\b",
        r"\b(\d+(?:[\s.,]\d+)?)\s*(₽|руб|р\.?|р)\b",
    ]

    if not mileage_context:
        for p in soft_patterns:
            m = re.search(p, text, re.IGNORECASE)
            if m:
                value = _parse_price_value(m.group(1), m.group(2))
                if value is not None:
                    return value

    return None


def _extract_brand_model(text: str) -> Tuple[Optional[str], Optional[str], float]:
    brand, model, confidence = taxonomy_service.resolve_entities(text)

    model_norm = _normalize_model_token(model) if model else None

    if brand and model_norm:
        return brand, model_norm, confidence

    tokens = re.findall(r"[a-zа-я0-9-]+", text, re.IGNORECASE)

    # try to recover missing model if brand was found
    if brand and not model_norm:
        brand_aliases = set(taxonomy_service.get_brand_aliases(brand) or [])
        brand_aliases.add(str(brand).lower())

        for idx, token in enumerate(tokens):
            token_norm = token.lower()
            if token_norm not in brand_aliases:
                continue

            candidates = tokens[idx + 1: idx + 4]
            for candidate in candidates:
                c_norm = _normalize_model_token(candidate)
                if not _looks_like_model_token(c_norm):
                    continue
                if re.fullmatch(r"(19|20)\d{2}", c_norm):
                    continue
                return brand, c_norm, max(float(confidence or 0.0), 0.85)

    # if taxonomy found model but not brand, keep model anyway only if very model-like
    if not brand and model_norm and _looks_like_model_token(model_norm):
        return None, model_norm, float(confidence or 0.0)

    return brand, model_norm, float(confidence or 0.0)


def parse_query(raw_text: str) -> StructuredQuery:
    raw_text = (raw_text or "").strip()
    normalized_raw_text = _normalize_parse_text(normalize_query(raw_text))

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


def _parse_with_llm(raw_text: str) -> dict:
    raise RuntimeError("LLM not implemented yet")


def _parse_with_fallback(raw_text: str) -> StructuredQuery:
    text = _normalize_parse_text(raw_text)
    result = StructuredQuery(raw_query=raw_text)

    current_year = datetime.utcnow().year

    # -------------------------
    # BRAND / MODEL
    # -------------------------
    brand, model, confidence = _extract_brand_model(text)

    if brand:
        result.brand = brand
        result.brand_confidence = confidence

    if model:
        result.model = model

    # -------------------------
    # EXPLICIT STRUCTURED CONSTRAINTS
    # -------------------------
    result.year_min = _extract_year_min(text, current_year)
    result.fuel = _extract_fuel(text)

    mileage_context = _has_mileage_context(text)
    result.mileage_max = _extract_mileage_max(text)

    if result.mileage_max is None:
        result.price_max = _extract_price_max(text, mileage_context=mileage_context)
    else:
        # allow explicit price extraction too, but mileage intent has priority over ambiguous tokens
        result.price_max = _extract_price_max(text, mileage_context=True)

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
        re.IGNORECASE,
    )
    if m:
        result.city = m.group(1)

    # -------------------------
    # RECENCY INTENT
    # -------------------------
    if any(w in text for w in ["свеж", "нов", "последн", "recent", "latest", "new"]):
        result.keywords.append("recent")

    # -------------------------
    # KEYWORDS / EXCLUSIONS
    # -------------------------
    tokens = re.findall(r"[a-zа-я0-9-]+", text, re.IGNORECASE)

    stop_tokens = {
        "до", "без", "и", "или", "не",
        "бит", "крашен", "окрас",
        "км", "тыс", "руб", "р", "k", "m", "м",
        "от", "с", "после", "старше", "младше",
        "лет", "год", "года",
        "бенз", "бензин", "petrol", "gasoline",
        "дизель", "диз", "diesel",
        "гибрид", "hybrid",
        "электро", "электр", "electric", "ev",
        "млн", "миллион", "пробег",
        "нестарше", "ненеже", "раньше", "ниже",
    }

    brand_synonyms = set()
    if result.brand:
        brand_synonyms = {str(x).lower() for x in (taxonomy_service.get_brand_aliases(result.brand) or [])}
        brand_synonyms.add(str(result.brand).lower())

    model_synonyms = set()
    if result.brand and result.model:
        model_synonyms = {str(x).lower() for x in (taxonomy_service.get_model_aliases(result.brand, result.model) or [])}
        model_synonyms.add(str(result.model).lower())

    for token in tokens:
        t = token.lower()

        if t.isdigit():
            continue

        if re.fullmatch(r"(19|20)\d{2}", t):
            continue

        if result.brand and t == str(result.brand).lower():
            continue

        if result.model and t == str(result.model).lower():
            continue

        if t in brand_synonyms or t in model_synonyms:
            continue

        if _looks_like_model_token(t) and result.model and _normalize_model_token(t) == _normalize_model_token(result.model):
            continue

        if t.startswith("не") and len(t) > 2:
            exclusion = t[2:] if t.startswith("не-") else t[2:] if t.startswith("не") else ""
            exclusion = exclusion.strip("-")
            if (
                exclusion
                and exclusion not in stop_tokens
                and exclusion not in brand_synonyms
                and exclusion not in model_synonyms
                and exclusion not in result.exclusions
            ):
                result.exclusions.append(exclusion)
        elif t not in stop_tokens and t not in result.keywords:
            result.keywords.append(t)

    return result
