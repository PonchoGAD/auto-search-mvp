import re

from services.taxonomy_service import taxonomy_service


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


def extract_price(text: str):
    if not text:
        return None

    # ❗ игнорим только если рядом с числом явно пробег
    if re.search(r"\d+\s*(км|km)", text.lower()):
        # но если есть ₽ → это точно цена
        if "₽" not in text and "руб" not in text:
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

    brand = None
    model = None

    # ✅ используем ТОЛЬКО taxonomy
    if taxonomy_service:
        try:
            brand, model, _ = taxonomy_service.resolve_entities(text)
        except Exception:
            pass

    return {
        "brand": brand,
        "model": model,
        "price": extract_price(text),
        "year": extract_year(text),
        "mileage": extract_mileage(text),
        "fuel": extract_fuel(text),
    }