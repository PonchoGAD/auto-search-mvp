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

    return _normalize_spaces(text)


def _normalize_model_token(value: str) -> str:
    value = (value or "").lower().strip()
    value = value.replace("-", "").replace("_", "").replace(" ", "")
    return value


def _looks_like_model_token(token: str) -> bool:

    token = (token or "").strip().lower()

    if not token:
        return False

    if token in STOP_TOKENS:
        return False

    if token.isdigit():
        return False

    if re.fullmatch(r"(19|20)\d{2}", token):
        return False

    if re.search(r"[a-zа-я]", token) and re.search(r"\d", token):
        return True

    if "-" in token:
        return True

    return bool(MODEL_TOKEN_RE.match(token))


def _parse_price_value(num_str: str, unit: str | None) -> Optional[int]:

    if not num_str:
        return None

    raw = str(num_str).strip().replace(" ", "").replace(",", ".")

    try:
        value = float(raw)
    except:
        return None

    unit_norm = (unit or "").lower()

    if unit_norm in {"млн", "миллион", "м"}:
        value *= 1_000_000

    elif unit_norm in {"тыс", "к", "k"}:
        value *= 1000

    v = int(value)

    if 10000 <= v <= 200000000:
        return v

    return None


def _parse_mileage_value(num_str: str, unit: str | None):

    if not num_str:
        return None

    raw = str(num_str).replace(",", ".")

    try:
        value = float(raw)
    except:
        return None

    unit_norm = (unit or "").lower()

    if unit_norm in {"тыс", "к"}:
        value *= 1000

    value = int(value)

    if 0 <= value <= 1500000:
        return value

    return None


def _has_mileage_context(text: str):

    return bool(

        re.search(
            r"(пробег|км|km|тыс\s*км|т\.км)",
            text,
            re.I
        )
    )


def _extract_price_min(text: str) -> Optional[int]:

    patterns = [
        r"\bот\s*(\d+(?:[\s.,]\d+)?)\s*(млн)\b",
        r"\bот\s*(\d+(?:[\s.,]\d+)?)\s*(тыс|к)\b",
        r"\bот\s*(\d+(?:[\s.,]\d+)?)\s*(₽|руб|р\.?|р)\b",
    ]

    for p in patterns:

        m = re.search(
            p,
            text,
            re.I
        )

        if not m:
            continue

        value = _parse_price_value(
            m.group(1),
            m.group(2)
        )

        if value:
            return value

    return None


def _extract_mileage_min(
        text: str
)->Optional[int]:

    patterns=[

        r"\bот\s*(\d+)\s*(тыс|км|km|к)\b",

        r"\bпробег\s*от\s*(\d+)\s*(тыс|км|km|к)\b",

    ]

    for p in patterns:

        m=re.search(
            p,
            text,
            re.I
        )

        if not m:
            continue

        v=_parse_mileage_value(
            m.group(1),
            m.group(2)
        )

        if v:
            return v

    return None


def _extract_mileage_max(text: str):

    patterns=[

        r"\bдо\s*(\d+)\s*(тыс|км|km|к)\b",

        r"\bпробег.*?до\s*(\d+)\s*(тыс|км|km|к)\b"

    ]

    for p in patterns:

        m=re.search(
            p,
            text,
            re.I
        )

        if not m:
            continue

        v=_parse_mileage_value(
            m.group(1),
            m.group(2)
        )

        if v:
            return v

    return None


def _extract_price_max(
        text:str,
        mileage_context:bool
):

    if mileage_context and re.search(
            r"\d+\s*(км|тыс\s*км|пробег)",
            text,
            re.I
    ):
        return None

    patterns=[

        r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*(млн)\b",

        r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*(тыс|к)\b",

        r"\bдо\s*(\d+(?:[\s.,]\d+)?)\s*(₽|руб|р\.?|р)\b"

    ]

    for p in patterns:

        m=re.search(
            p,
            text,
            re.I
        )

        if not m:
            continue

        matched=m.group(0).lower()

        unit=m.group(2)

        unit_candidate=(unit or "").lower()

        if mileage_context:

            if unit_candidate in {
                "к",
                "тыс",
                "тысяч",
                "км",
                "km"
            }:
                continue

            matched_text=matched.lower()

            if (
                "км" in matched_text
                or "пробег" in matched_text
                or "тыс" in matched_text
            ):
                continue

        value=_parse_price_value(
            m.group(1),
            unit
        )

        if value:
            return value

    return None


def _extract_city(text:str):

    for raw,canonical in CITY_MAP.items():

        if re.search(
                rf"\b{raw}\b",
                text,
                re.I
        ):
            return canonical

    return None


def _extract_fuel(text: str) -> Optional[str]:
    text = (text or "").lower()
    patterns = [
        ("electric",  r"\b(электро|электромобиль|electric|ev)\b"),
        ("hybrid",    r"\b(гибрид|hybrid|phev|hev)\b"),
        ("diesel",    r"\b(дизель|дизельный|диз|diesel|tdi|cdi|dci)\b"),
        ("gas_petrol",r"\b(газ\s*/\s*бензин|бензин\s*/\s*газ|газ\s+бензин|бензин\s+газ)\b"),
        ("gas",       r"\b(газ|lpg|cng|гбо)\b"),
        ("petrol",    r"\b(бензин|бензиновый|бенз|petrol|gasoline|mpi|fsi|tsi|tfsi)\b"),
    ]
    for fuel, pattern in patterns:
        if re.search(pattern, text):
            return fuel
    return None


def _extract_brand_model(text):

    return taxonomy_service.resolve_entities(text)


def _parse_with_llm(raw_text:str):
    raise RuntimeError()


def parse_query(raw_text: str)->StructuredQuery:

    raw_text=(raw_text or "").strip()

    text=_normalize_parse_text(
        normalize_query(raw_text)
    )

    return _parse_with_fallback(text)


def _parse_with_fallback(
        raw_text:str
)->StructuredQuery:

    text=_normalize_parse_text(raw_text)

    result=StructuredQuery(
        raw_query=raw_text
    )

    current_year=datetime.utcnow().year

    brand,model,confidence=\
        _extract_brand_model(text)

    if brand:
        result.brand=brand
        result.brand_confidence=confidence

    if model:
        result.model=model

    possible_brands=[]

    splitters={
        "или",
        "либо",
        "/",
        ","
    }

    if brand:
        possible_brands.append(brand)

    for token in re.findall(
        r"[a-zа-я0-9-]+",
        text,
        re.I
    ):

        if token in splitters:
            continue

        canonical_b=taxonomy_service.canonicalize_brand(token)

        if canonical_b and canonical_b in taxonomy_service.brand_to_aliases:

            if canonical_b not in possible_brands:

                possible_brands.append(
                    canonical_b
                )

    result.brands=possible_brands

    result.fuel=_extract_fuel(text)

    mileage_context=_has_mileage_context(
        text
    )

    result.mileage_max=_extract_mileage_max(text)

    result.mileage_min=_extract_mileage_min(text)

    if result.mileage_max is not None:

        result.price_max=None

    else:

        result.price_max=_extract_price_max(
            text,
            mileage_context
        )

    result.price_min=_extract_price_min(
        text
    )

    result.city=_extract_city(text)

    if result.city:

        city_region={

            "moskva":"moscow_region",

            "spb":"spb_region",

            "almaty":"almaty_region",

            "astana":"astana_region"

        }

        result.region=city_region.get(
            result.city
        )

    tokens=re.findall(
        r"[a-zа-я0-9-]+",
        text,
        re.I
    )

    for token in tokens:

        t=token.lower()

        if t in STOP_TOKENS:
            continue

        if t not in result.keywords:

            result.keywords.append(
                t
            )

    result.debug={

        "pipeline":[

            "normalize",
            "brand_model",
            "year",
            "fuel",
            "price",
            "mileage",
            "keywords"

        ]
    }

    return result