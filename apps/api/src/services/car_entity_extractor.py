import re

# =========================
# TEXT NORMALIZATION
# =========================

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


# =========================
# PRICE
# =========================

RE_PRICE = re.compile(
    r"(\d[\d\s\u00A0]{3,12})\s*(₽|руб|р)\b",
    re.IGNORECASE
)

RE_PRICE_GLUE = re.compile(
    r"(\d[\d\s\u00A0]{3,12})(?:₽|руб|р)(?=[a-zа-я])",
    re.IGNORECASE
)

PRICE_PATTERNS = [
    r"(\d[\d\s\u00A0]{4,12})\s*₽",
    r"(\d[\d\s\u00A0]{4,12})\s*руб",
    r"(\d[\d\s\u00A0]{4,12})\s*rub",
    r"(\d[\d\s\u00A0]{4,12})\s*р\b",
]

# =========================
# YEAR
# =========================

RE_YEAR = re.compile(r"\b(19\d{2}|20\d{2})\b")

# =========================
# MILEAGE
# =========================

RE_MILEAGE = re.compile(
    r"\b(\d[\d\s\u00A0]{2,8})\s*(км|km)\b",
    re.IGNORECASE
)

RE_MILEAGE_K = re.compile(
    r"\b(\d{1,3}(?:[.,]\d+)?)\s*(тыс\.?|т\.км|k|ткм)\b",
    re.IGNORECASE
)

RE_MILEAGE_LABEL = re.compile(
    r"\b(?:пробег|mileage)\s*[:\-]?\s*(\d[\d\s\u00A0]{2,8})\b",
    re.IGNORECASE
)

# =========================
# FUEL MAP
# =========================

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

# =========================
# BRAND / MODEL MAPS
# =========================

BRANDS = [
    "toyota", "bmw", "mercedes", "mercedes benz", "audi",
    "honda", "nissan", "mazda", "lexus", "infiniti",
    "kia", "hyundai", "volkswagen", "skoda", "porsche",
    "chevrolet", "ford", "cadillac", "gmc", "jeep",
    "land rover", "range rover", "volvo", "opel",
    "peugeot", "citroen", "renault", "lada",
    "chery", "geely", "haval", "jetour", "exeed",
    "byd", "xpeng", "tesla", "zeekr",
    "bentley", "lamborghini", "ferrari", "rolls royce",
    "changan", "li auto"
]

MODEL_TO_BRAND = {
    "camry": "toyota",
    "corolla": "toyota",
    "rav4": "toyota",
    "prado": "toyota",
    "land cruiser": "toyota",
    "land cruiser prado": "toyota",
    "highlander": "toyota",
    "hilux": "toyota",

    "x1": "bmw",
    "x3": "bmw",
    "x5": "bmw",
    "x6": "bmw",
    "x7": "bmw",
    "3 series": "bmw",
    "5 series": "bmw",

    "e class": "mercedes",
    "c class": "mercedes",
    "s class": "mercedes",
    "glc": "mercedes",
    "glc class": "mercedes",
    "gle": "mercedes",
    "gls": "mercedes",
    "v class": "mercedes",

    "x trail": "nissan",
    "qashqai": "nissan",
    "teana": "nissan",
    "patrol": "nissan",

    "solaris": "hyundai",
    "sonata": "hyundai",
    "elantra": "hyundai",
    "tucson": "hyundai",
    "santa fe": "hyundai",
    "creta": "hyundai",

    "rio": "kia",
    "ceed": "kia",
    "cerato": "kia",
    "k5": "kia",
    "seltos": "kia",
    "sportage": "kia",
    "sorento": "kia",
    "pegas": "kia",

    "cx 5": "mazda",
    "cx 60": "mazda",
    "mazda 3": "mazda",
    "mazda 6": "mazda",

    "monjaro": "geely",
    "atlas": "geely",
    "coolray": "geely",

    "jolion": "haval",
    "h6": "haval",
    "f7": "haval",

    "cruze": "chevrolet",

    "antara": "opel",
    "astra": "opel",

    "xc60": "volvo",
    "xc90": "volvo",

    "l7": "li_auto",
    "l9": "li_auto",

    "discovery sport": "land_rover",
    "range rover sport": "land_rover",
    "evoque": "land_rover",
}

COMMON_MODELS = [
    "camry", "corolla", "rav4", "prado", "land cruiser", "land cruiser prado", "highlander",
    "x1", "x3", "x5", "x6", "x7", "3 series", "5 series",
    "e class", "c class", "s class", "glc", "glc class", "gle", "gls", "v class",
    "x trail", "qashqai", "teana", "patrol",
    "solaris", "sonata", "elantra", "tucson", "santa fe", "creta",
    "rio", "ceed", "cerato", "k5", "seltos", "sportage", "sorento", "pegas",
    "cx 5", "cx 60", "mazda 3", "mazda 6",
    "monjaro", "atlas", "coolray",
    "jolion", "h6", "f7",
    "cruze",
    "antara", "astra",
    "xc60", "xc90",
    "l7", "l9",
    "discovery sport", "range rover sport", "evoque",
]

# =========================
# BRAND
# =========================

def extract_brand(text):
    if not text:
        return None

    t = _norm(text)
    t = t.replace("-", " ")

    for brand in BRANDS:
        if f" {brand} " in f" {t} ":
            if brand == "li auto":
                return "li_auto"
            if brand == "mercedes benz":
                return "mercedes"
            if brand == "rolls royce":
                return "rolls-royce"
            if brand == "land rover":
                return "land_rover"
            return brand

    for model, brand in MODEL_TO_BRAND.items():
        if f" {model} " in f" {t} ":
            return brand

    return None

# =========================
# MODEL
# =========================

def extract_model(text: str, brand: str | None):
    if not text:
        return None

    t = _norm(text)

    for model in COMMON_MODELS:

        if f" {model} " not in f" {t} ":
            continue

        if brand and MODEL_TO_BRAND.get(model) and MODEL_TO_BRAND.get(model) != brand:
            continue

        return model

    m = re.search(r"\b(x[1-7])\b", t)
    if m:
        return m.group(1)

    m = re.search(r"\b([1-7]\s+series)\b", t)
    if m:
        return m.group(1)

    mercedes_aliases = [
        ("e class", r"\be\s*class\b"),
        ("c class", r"\bc\s*class\b"),
        ("s class", r"\bs\s*class\b"),
        ("glc class", r"\bglc\s*class\b"),
        ("v class", r"\bv\s*class\b"),
    ]
    for canon, pattern in mercedes_aliases:
        if re.search(pattern, t, re.IGNORECASE):
            return canon

    return None

# =========================
# PRICE
# =========================

def extract_price(text: str):
    if not text:
        return None

    t = text.replace("\xa0", " ")

    lemon_match = re.search(r"(\d+)\s?🍋", t)
    if lemon_match:
        try:
            value = int(lemon_match.group(1))
            return value * 1_000_000
        except Exception:
            pass

    k_match = re.search(r"(\d+(?:\.\d+)?)\s?[kк]\b", t.lower())
    if k_match:
        try:
            value = float(k_match.group(1))
            price = int(value * 1000)
            if 10_000 < price < 200_000_000:
                return price
        except Exception:
            pass

    m_match = re.search(r"(\d+(?:\.\d+)?)\s?[mм]\b", t.lower())
    if m_match:
        try:
            value = float(m_match.group(1))
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

# =========================
# YEAR
# =========================

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

# =========================
# MILEAGE
# =========================

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

# =========================
# FUEL
# =========================

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

# =========================
# MAIN EXTRACTOR
# =========================

def extract_car_entities(title, content):
    text = f"{title or ''} {content or ''}"

    brand = extract_brand(text)
    model = extract_model(text, brand)

    if not brand and model:
        brand = MODEL_TO_BRAND.get(model)

    price = extract_price(text)
    year = extract_year(text)
    mileage = extract_mileage(text)
    fuel = extract_fuel(text)

    return {
        "brand": brand,
        "model": model,
        "price": price,
        "year": year,
        "mileage": mileage,
        "fuel": fuel,
    }