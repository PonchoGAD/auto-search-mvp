import re

from services.brand_detector import detect_brand
from services.model_resolver import resolve_model

try:
    from services.taxonomy_service import taxonomy_service
except Exception:
    taxonomy_service = None


def _norm(text: str) -> str:
    text = text or ""
    text = text.replace("\u00A0", " ").replace("\xa0", " ")
    text = re.sub(r"[-_/]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()


def _digits_only(value: str) -> str:
    return re.sub(r"[^\d]", "", value or "")


def _is_speed_noise(text: str) -> bool:
    t = _norm(text)
    return any(x in t for x in ["км/ч", "km/h", "скорость", "средняя скорость"])


RE_PRICE = re.compile(r"(\d[\d\s\u00A0]{3,12})\s*(₽|руб|р)\b", re.IGNORECASE)
RE_PRICE_GLUE = re.compile(r"(\d[\d\s\u00A0]{3,12})(?:₽|руб|р)(?=[a-zа-я])", re.IGNORECASE)

PRICE_PATTERNS = [
    r"(\d[\d\s\u00A0]{4,12})\s*₽",
    r"(\d[\d\s\u00A0]{4,12})\s*руб",
    r"(\d[\d\s\u00A0]{4,12})\s*rub",
    r"(\d[\d\s\u00A0]{4,12})\s*р\b",
]

RE_YEAR = re.compile(r"\b(19\d{2}|20\d{2})\b")

RE_MILEAGE = re.compile(r"\b(\d[\d\s\u00A0]{2,8})\s*(км|km)\b", re.IGNORECASE)
RE_MILEAGE_K = re.compile(r"\b(\d{1,3}(?:[.,]\d+)?)\s*(тыс\.?\s*км|тыс\.?|т\.км|k|ткм)\b", re.IGNORECASE)
RE_MILEAGE_LABEL = re.compile(r"\b(?:пробег|mileage)\s*[:\-]?\s*(\d[\d\s\u00A0]{2,8})\b", re.IGNORECASE)

FUEL_MAP = {
    "бензин": "petrol",
    "бенз": "petrol",
    "бензиновый": "petrol",
    "на бензине": "petrol",
    "petrol": "petrol",
    "gasoline": "petrol",

    "дизель": "diesel",
    "диз": "diesel",
    "дизельный": "diesel",
    "на дизеле": "diesel",
    "diesel": "diesel",

    "гибрид": "hybrid",
    "гибридный": "hybrid",
    "hybrid": "hybrid",
    "plug in hybrid": "hybrid",
    "phev": "hybrid",

    "электро": "electric",
    "электр": "electric",
    "электрический": "electric",
    "электромобиль": "electric",
    "electric": "electric",
    "ev": "electric",

    "газ": "gas",
    "гбо": "gas",
    "lpg": "gas",

    "газ бензин": "gas_petrol",
    "бензин газ": "gas_petrol",
}


def _normalize_model(model: str | None) -> str | None:
    if not model:
        return None
    m = model.strip().lower().replace("_", "-")
    m = re.sub(r"\s+", " ", m)
    m = re.sub(r"\b([a-zа-я]+)\s+(\d{1,4})\b", r"\1\2", m, flags=re.IGNORECASE)
    m = m.replace(" ", "-")
    m = re.sub(r"-{2,}", "-", m)
    return m.strip("-") or None


def _detect_brand_with_fallback(title: str, content: str, text: str):
    brand = None

    try:
        detected = detect_brand(title=title or "", text=content or text)
    except TypeError:
        try:
            detected = detect_brand(text)
        except Exception:
            detected = None
    except Exception:
        detected = None

    brand_conf = None

    if isinstance(detected, tuple):
        brand = detected[0] if len(detected) > 0 else None
        brand_conf = detected[1] if len(detected) > 1 else None
    else:
        brand = detected

    if brand:
        return brand, brand_conf

    if taxonomy_service:
        for method_name in [
            "detect_brand_by_model_alias",
            "resolve_brand_by_model_alias",
            "detect_brand",
        ]:
            try:
                if hasattr(taxonomy_service, method_name):
                    inferred = getattr(taxonomy_service, method_name)(text)
                    if isinstance(inferred, tuple):
                        inferred = inferred[0] if inferred else None
                    if inferred:
                        return inferred, brand_conf
            except Exception:
                pass

    return None, brand_conf


def _resolve_model_with_fallback(brand: str | None, text: str):
    model = None

    if brand:
        try:
            model = resolve_model(brand, text)
        except Exception:
            model = None

    if model:
        return _normalize_model(model)

    if taxonomy_service:
        try:
            if hasattr(taxonomy_service, "resolve_model"):
                model = taxonomy_service.resolve_model(brand, text)
                if model:
                    return _normalize_model(model)
        except Exception:
            pass

        if not brand:
            try:
                if hasattr(taxonomy_service, "resolve_model_with_brand"):
                    resolved = taxonomy_service.resolve_model_with_brand(text)
                    if isinstance(resolved, tuple):
                        inferred_model = resolved[1] if len(resolved) > 1 else None
                        if inferred_model:
                            return _normalize_model(inferred_model)
                    elif resolved:
                        return _normalize_model(resolved)
            except Exception:
                pass

    return None


def extract_price(text: str):
    if not text:
        return None

    t = text.replace("\xa0", " ")

    lemon_match = re.search(r"(\d+(?:[.,]\d+)?)\s?🍋", t)
    if lemon_match:
        try:
            value = float(lemon_match.group(1).replace(",", "."))
            price = int(value * 1_000_000)
            if 10_000 < price < 200_000_000:
                return price
        except Exception:
            pass

    k_match = re.search(r"(\d+(?:[.,]\d+)?)\s?[kк]\b", t.lower())
    if k_match:
        try:
            value = float(k_match.group(1).replace(",", "."))
            price = int(value * 1000)
            if 10_000 < price < 200_000_000:
                return price
        except Exception:
            pass

    m_match = re.search(r"(\d+(?:[.,]\d+)?)\s?[mм]\b", t.lower())
    if m_match:
        try:
            value = float(m_match.group(1).replace(",", "."))
            price = int(value * 1_000_000)
            if 10_000 < price < 200_000_000:
                return price
        except Exception:
            pass

    m = RE_PRICE.search(t)
    if m:
        try:
            price = int(_digits_only(m.group(1)))
            if 10_000 < price < 200_000_000:
                return price
        except Exception:
            pass

    m = RE_PRICE_GLUE.search(t)
    if m:
        try:
            price = int(_digits_only(m.group(1)))
            if 10_000 < price < 200_000_000:
                return price
        except Exception:
            pass

    for p in PRICE_PATTERNS:
        m = re.search(p, t, re.I)
        if m:
            try:
                price = int(_digits_only(m.group(1)))
                if 10_000 < price < 200_000_000:
                    return price
            except Exception:
                pass

    return None


def extract_year(text):
    if not text:
        return None

    m = RE_YEAR.search(text)
    if not m:
        return None

    try:
        y = int(m.group(1))
        if 1985 <= y <= 2030:
            return y
    except Exception:
        pass

    return None


def extract_mileage(text):
    if not text:
        return None

    if _is_speed_noise(text):
        return None

    t = text.replace("\xa0", " ")

    m = RE_MILEAGE_LABEL.search(t)
    if m:
        try:
            val = int(_digits_only(m.group(1)))
            if 0 <= val <= 500_000:
                return val
        except Exception:
            pass

    m = RE_MILEAGE.search(t)
    if m:
        try:
            val = int(_digits_only(m.group(1)))
            if 0 <= val <= 500_000:
                return val
        except Exception:
            pass

    m = RE_MILEAGE_K.search(t.lower())
    if m:
        try:
            raw = m.group(1).replace(",", ".")
            val = int(float(raw) * 1000)
            if 0 <= val <= 500_000:
                return val
        except Exception:
            pass

    return None


def extract_fuel(text):
    if not text:
        return None

    t = _norm(text)

    if "газ/бензин" in t or "газ бензин" in t or "бензин/газ" in t or "бензин газ" in t:
        return "gas_petrol"

    if "plug in hybrid" in t or "phev" in t:
        return "hybrid"

    for k, v in FUEL_MAP.items():
        if f" {k} " in f" {t} ":
            return v

    return None


def extract_car_entities(title, content):
    text = f"{title or ''} {content or ''}".strip()

    brand, _brand_conf = _detect_brand_with_fallback(
        title=title or "",
        content=content or text,
        text=text,
    )

    model = _resolve_model_with_fallback(brand, text)

    if not brand and model and taxonomy_service:
        for method_name in [
            "detect_brand_by_model_alias",
            "resolve_brand_by_model_alias",
            "get_brand_by_model",
        ]:
            try:
                if hasattr(taxonomy_service, method_name):
                    if method_name == "get_brand_by_model":
                        inferred = getattr(taxonomy_service, method_name)(model)
                    else:
                        inferred = getattr(taxonomy_service, method_name)(text)
                    if inferred:
                        brand = inferred
                        break
            except Exception:
                pass

    price = extract_price(text)
    year = extract_year(text)
    mileage = extract_mileage(text)
    fuel = extract_fuel(text)

    return {
        "brand": brand or None,
        "model": model or None,
        "price": price,
        "year": year,
        "mileage": mileage,
        "fuel": fuel or None,
    }
