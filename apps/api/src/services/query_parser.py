from typing import Optional, Tuple
import re
from datetime import datetime

from domain.query_schema import StructuredQuery
from services.query_normalizer import normalize_query
from services.taxonomy_service import taxonomy_service


MODEL_TOKEN_RE = re.compile(
    r"^[a-zа-я]+(?:[-_ ]?[a-zа-я0-9]+)\d[a-zа-я0-9-]$",
    re.IGNORECASE,
)

CITY_MAP = {
    "москва": "moskva",
    "спб": "spb",
    "питер": "spb",
    "санкт-петербург": "spb",
    "екатеринбург": "ekaterinburg",
    "казань": "kazan",
    "новосибирск": "novosibirsk",
    "алматы": "almaty",
    "астана": "astana",
}

STOP_TOKENS = {
    "до", "от", "с", "после", "не", "старше", "младше", "раньше", "ниже",
    "года", "год", "лет", "км", "тыс", "млн", "руб", "р", "бензин",
    "дизель", "гибрид", "электро", "electric", "hybrid", "diesel", "petrol",
    "пробег", "без", "окрас", "бит", "крашен", "цена", "купить", "продажа",
    "машина", "авто", "тачка", "седан", "кроссовер", "внедорожник",
    "свежая", "свежий", "новая", "новый", "последний", "последняя",
}


def _normalize_spaces(text: str) -> str:
    text = (text or "").replace("\u00A0", " ").replace("\xa0", " ")
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _normalize_parse_text(text: str) -> str:
    text = (text or "").lower()
    text = text.replace("ё", "е")

    text = re.sub(r"(?<=\d)\s+(?=км\b)", " ", text)
    text = re.sub(r"(?<=\d)\s+(?=тыс\b)", " ", text)
    text = re.sub(r"(?<=\d)\s+(?=тысяч\b)", " ", text)

    text = re.sub(r"\b(\d+(?:[.,]\d+)?)\s*млн\b", r"\1 млн", text)
    text = re.sub(r"\b(\d+(?:[.,]\d+)?)\s*миллион(?:а|ов)?\b", r"\1 млн", text)
    text = re.sub(r"\b(\d+(?:[.,]\d+)?)\s*м\b", r"\1 млн", text)

    text = re.sub(r"\b(\d+(?:[.,]\d+)?)\s*к\b", r"\1к", text)
    text = re.sub(r"\b(\d+(?:[.,]\d+)?)\s*k\b", r"\1к", text)

    text = re.sub(r"\be[\s-]?(\d{3})\b", r"e\1", text)
    text = re.sub(r"\bgx[\s-]?(\d{3})\b", r"gx\1", text)
    text = re.sub(r"\bcx[\s-]?(\d)\b", r"cx-\1", text)
    text = re.sub(r"\bcr[\s-]?v\b", "cr-v", text)
    text = re.sub(r"\bx[\s-]?trail\b", "x-trail", text)
    text = re.sub(r"\bs[\s-]?class\b", "s-class", text)
    text = re.sub(r"\bc[\s-]?class\b", "c-class", text)
    text = re.sub(r"\be[\s-]?class\b", "e-class", text)
    text = re.sub(r"\b3[\s-]?series\b", "3-series", text)
    text = re.sub(r"\b5[\s-]?series\b", "5-series", text)

    text = _normalize_spaces(text)
    return text


def _normalize_model_token(value: str) -> str:
    value = (value or "").lower().strip()
    value = value.replace("-", "")
    value = value.replace("_", "")
    value = value.replace(" ", "")
    return value


def _looks_like_model_token(token: str) -> bool:
    token = (token or "").strip().lower()
    if not token or len(token) < 2:
        return False

    if token in STOP_TOKENS:
        return False

    if re.fullmatch(r"(19|20)\d{2}", token):
        return False

    if token.isdigit():
        return False

    if re.search(r"[a-zа-я]", token) and re.search(r"\d", token):
        return True

    if "-" in token and re.search(r"[a-zа-я]", token):
        return True

    if token in {
        "x3", "x5", "x6", "x7", "gle", "gls", "cls",
        "camry", "corolla", "rav4", "rav-4", "prado",
        "qashqai", "x-trail", "solaris", "sportage", "sorento",
        "tucson", "monjaro", "coolray"
    }:
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

    if unit_norm in {"млн", "миллион", "m", "м"}:
        value *= 1_000_000
    elif unit_norm in {"тыс", "тысяч", "к", "k"}:
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

    unit_norm = (unit or "").strip().lower().replace(" ", "")

    if unit_norm in {"тыс", "тысяч", "т.км", "tkm", "k", "к", "тыскм"}:
        value *= 1_000
    elif unit_norm in {"км", "km", ""}:
        pass
    else:
        return None

    try:
        int_value = int(value)
    except Exception:
        return None

    if 0 <= int_value <= 1_500_000:
        return int_value

    return None


def _has_mileage_context(text: str) -> bool:
    return bool(
        re.search(
            r"\b(пробег|пробегом|км|km|т\.км|тыс\s*км|\d+\s*км|\d+\s*km|\d+\s*тыс)\b",
            text,
            re.IGNORECASE,
        )
    )


def _extract_year_range(text: str, current_year: int) -> Tuple[Optional[int], Optional[int]]:
    # "не старше 2023", "от 2023", "после 2023"
    patterns_min =[
        r"\b(?:от|с|после)\s*(19\d{2}|20\d{2})\b",
        r"\bне\s+старше\s*(19\d{2}|20\d{2})(?:\s*г(?:\.|ода)?)?\b",
        r"\b(?:не\s+ниже|не\s+раньше)\s*(19\d{2}|20\d{2})(?:\s*г(?:\.|ода)?)?\b",
    ]
    for pattern in patterns_min:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            year_val = int(m.group(1))
            if 1985 <= year_val <= current_year + 1:
                return year_val, None

    # "до 2023", "не новее 2023"
    patterns_max =[
        r"\b(?:до|по)\s*(19\d{2}|20\d{2})\b",
        r"\bне\s+новее\s*(19\d{2}|20\d{2})(?:\s*г(?:\.|ода)?)?\b",
    ]
    for pattern in patterns_max:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            year_val = int(m.group(1))
            if 1985 <= year_val <= current_year + 1:
                return None, year_val

    # "младше 5 лет" -> year_min = current_year - 5
    m = re.search(r"\b(?:младше|за\s+последние)\s*(\d+)\s*лет\b", text, re.IGNORECASE)
    if m:
        years = int(m.group(1))
        if 0 <= years <= 30:
            return current_year - years, None

    # Точный год: "2023 года", "2023 г"
    m = re.search(r"\b(19\d{2}|20\d{2})\s*(?:г\.|год[ау]?|г\b)", text, re.IGNORECASE)
    if m:
        year_val = int(m.group(1))
        if 1985 <= year_val <= current_year + 1:
            return year_val, year_val

    # Просто год как отдельное число (защита от цены или пробега)
    for match in re.finditer(r"\b(19[8-9]\d|20[0-2]\d)\b", text):
        year_val = int(match.group(1))
        if not re.search(rf"{year_val}\s*(?:₽|руб|р|км|km|тыс|k|к)\b", text, re.IGNORECASE):
            if 1985 <= year_val <= current_year + 1:
                return year_val, year_val

    return None, None


def _extract_fuel(text: str) -> Optional[str]:
    fuel_patterns =[
        (r"\b(газ\s*/\s*бензин|бензин\s*/\s*газ|газ\s+бензин|бензин\s+газ)\b", "gas_petrol"),
        (r"\b(бензин|бенз|бенза|petrol|gasoline)\b", "petrol"), # добавили 'бенза'
        (r"\b(дизель|диз|трактор|diesel)\b", "diesel"), # добавили 'трактор' (частый авто-сленг)
        (r"\b(гибрид|hybrid|phev|hev)\b", "hybrid"),
        (r"\b(электро|электр|электричка|electric|ev|электромобиль)\b", "electric"), # добавили 'электричка'
    ]

    for pattern, fuel_value in fuel_patterns:
        if re.search(pattern, text, re.IGNORECASE):
            return fuel_value

    return None


def _extract_mileage_max(text: str) -> Optional[int]:
    # 🔥 Усиленные регулярки для пробега (включая "до 150 тысяч пробега")
    patterns =[
        r"\bс\s*пробегом\s*до\s*(\d+(?:[\s.,]\d+)?)\s*(тыс(?:яч)?|т\.км|tkm|k|к|км|km)?\b",
        r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*(тыс(?:яч)?|т\.км|к|k)?\s*пробег[ау]?\b",
        r"\bпробег[а-я]*\s*до\s*(\d+(?:[\s.,]\d+)?)\s*(тыс(?:яч)?|т\.км|tkm|k|к|км|km)?\b",
        r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*(тыс\s*км|т\.км|tkm|km|км|тыс|тысяч|к)\b",
        r"\bдо\s*(\d{2,6})\s*(км|km)\b",
        r"\b(\d+(?:[\s.,]\d+)?)\s*(тыс\s*км|т\.км)\b",
        r"\bпробег\s*(\d+(?:[\s.,]\d+)?)\s*тыс\s*км\b",
    ]

    for p in patterns:
        m = re.search(p, text, re.IGNORECASE)
        if not m:
            continue

        num = m.group(1)
        unit = m.group(2) if len(m.groups()) > 1 else "км"
        unit_norm = (unit or "").lower().replace(" ", "")

        if unit_norm in {"тыскм", "тыс", "тысяч"}:
            unit_norm = "тыс"
        elif unit_norm in {"km"}:
            unit_norm = "км"

        value = _parse_mileage_value(num, unit_norm)
        if value is not None:
            return value

    return None


def _extract_price_max(text: str, mileage_context: bool) -> Optional[int]:
    # если в запросе есть явный пробег — не трогаем цену по голому числу
    if mileage_context and re.search(r"\b\d+(?:[\s.,]\d+)?\s*(км|km|тыс\s*км|т\.км|пробег)\b", text, re.IGNORECASE):
        return None

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
        if "км" in matched or "km" in matched or "т.км" in matched or "пробег" in matched:
            continue

        unit = m.group(2) if len(m.groups()) > 1 else None
        unit_candidate = (unit or "").lower()

        if mileage_context and unit_candidate in {"к", "тыс", "тысяч"}:
            continue

        value = _parse_price_value(m.group(1), unit)
        if value is not None:
            return value

    soft_patterns =[
        r"\b(\d+(?:[\s.,]\d+)?)\s*(млн)\b",
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


def _extract_city(text: str) -> Optional[str]:
    for raw_city, canonical in CITY_MAP.items():
        if re.search(rf"\b{re.escape(raw_city)}\b", text, re.IGNORECASE):
            return canonical
    return None


def _extract_brand_model(text: str) -> Tuple[Optional[str], Optional[str], float]:
    brand, model, confidence = taxonomy_service.resolve_entities(text)

    model_norm = _normalize_model_token(model) if model else None

    if brand and model_norm:
        return brand, model_norm, float(confidence or 0.0)

    tokens = re.findall(r"[a-zа-я0-9-]+", text, re.IGNORECASE)

    if brand and not model_norm:
        brand_aliases = set(taxonomy_service.get_brand_aliases(brand) or[])
        brand_aliases.add(str(brand).lower())

        for idx, token in enumerate(tokens):
            if token.lower() not in brand_aliases:
                continue

            candidates = tokens[idx + 1: idx + 4]
            for candidate in candidates:
                c_norm = _normalize_model_token(candidate)
                if not _looks_like_model_token(c_norm):
                    continue
                if re.fullmatch(r"(19|20)\d{2}", c_norm):
                    continue
                return brand, c_norm, max(float(confidence or 0.0), 0.85)

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

    brand, model, confidence = _extract_brand_model(text)

    if brand:
        result.brand = brand
        result.brand_confidence = confidence

    if model:
        result.model = model

    # 🔥 ИЗМЕНЕНИЕ: Извлекаем диапазон годов
    year_min, year_max = _extract_year_range(text, current_year)
    result.year_min = year_min
    result.year_max = year_max

    # 🔥 ИЗМЕНЕНИЕ: Извлекаем несколько брендов (для OR-запросов "бмв или ауди")
    possible_brands =[]
    if brand:
        possible_brands.append(brand)
        
    for token in re.findall(r"[a-zа-я0-9-]+", text, re.IGNORECASE):
        canonical_b = taxonomy_service.canonicalize_brand(token)
        if canonical_b and canonical_b not in possible_brands:
            if len(token) > 2 or token.lower() in {"vw", "mg", "li"}:
                possible_brands.append(canonical_b)
                
    result.brands = possible_brands

    result.fuel = _extract_fuel(text)

    mileage_context = _has_mileage_context(text)
    result.mileage_max = _extract_mileage_max(text)

    if result.mileage_max is not None:
        result.price_max = None
    else:
        result.price_max = _extract_price_max(text, mileage_context=mileage_context)

    if "без окрас" in text or "без окраса" in text or "не бит" in text or "родная краска" in text:
        result.paint_condition = "original"
    elif "крашен" in text or "бит" in text:
        result.paint_condition = "repainted"

    result.city = _extract_city(text)

    if any(w in text for w in["свеж", "нов", "последн", "recent", "latest", "new"]):
        result.keywords.append("recent")

    tokens = re.findall(r"[a-zа-я0-9-]+", text, re.IGNORECASE)

    for i, token in enumerate(tokens):
        if "км" in token:
            if i > 0:
                value = _parse_mileage_value(tokens[i - 1], "км")
                if value:
                    result.mileage_max = value
                    continue

    brand_synonyms = set()
    if result.brand:
        brand_synonyms = {str(x).lower() for x in (taxonomy_service.get_brand_aliases(result.brand) or[])}
        brand_synonyms.add(str(result.brand).lower())

    model_synonyms = set()
    if result.brand and result.model:
        model_synonyms = {str(x).lower() for x in (taxonomy_service.get_model_aliases(result.brand, result.model) or[])}
        model_synonyms.add(str(result.model).lower())

    for token in tokens:
        t = token.lower()

        if t.isdigit():
            continue
        if re.fullmatch(r"(19|20)\d{2}", t):
            continue
        if t in STOP_TOKENS:
            continue
        if result.brand and t == str(result.brand).lower():
            continue
        if result.model and t == str(result.model).lower():
            continue
        if t in brand_synonyms or t in model_synonyms:
            continue
        if _looks_like_model_token(t) and result.model and _normalize_model_token(t) == _normalize_model_token(result.model):
            continue
        if t in CITY_MAP:
            continue

        if t.startswith("не") and len(t) > 2:
            exclusion = t[2:]
            exclusion = exclusion.strip("-")
            if (
                exclusion
                and exclusion not in STOP_TOKENS
                and exclusion not in brand_synonyms
                and exclusion not in model_synonyms
                and exclusion not in result.exclusions
            ):
                result.exclusions.append(exclusion)
        elif t not in result.keywords:
            result.keywords.append(t)

    return result