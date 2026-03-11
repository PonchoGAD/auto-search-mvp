import re

# =========================
# PRICE
# =========================

RE_PRICE = re.compile(
    r'(\d[\d\s]{3,10})\s*(₽|руб|р)?',
    re.IGNORECASE
)

# дополнительные паттерны цены
PRICE_PATTERNS = [
    r"(\d[\d\s]{4,})\s*₽",
    r"(\d[\d\s]{4,})\s*руб",
    r"(\d[\d\s]{4,})\s*rub"
]

# =========================
# YEAR
# =========================

RE_YEAR = re.compile(
    r'\b(19\d{2}|20\d{2})\b'
)

# =========================
# MILEAGE
# =========================

RE_MILEAGE = re.compile(
    r'(\d[\d\s]{2,7})\s*(км|km)',
    re.IGNORECASE
)

RE_MILEAGE_ALT = re.compile(
    r"(\d{2,6})\s*(км|km)",
    re.IGNORECASE
)

# =========================
# FUEL MAP
# =========================

FUEL_MAP = {
    "бенз": "petrol",
    "бензин": "petrol",
    "petrol": "petrol",
    "gasoline": "petrol",

    "дизель": "diesel",
    "diesel": "diesel",

    "гибрид": "hybrid",
    "hybrid": "hybrid",

    "электро": "electric",
    "electric": "electric",
}

# =========================
# BRAND LIST
# =========================

BRANDS = [
    "toyota","bmw","mercedes","mercedes-benz","audi",
    "honda","nissan","mazda","lexus","infiniti",
    "kia","hyundai","volkswagen","skoda","porsche",
    "chevrolet","ford","cadillac","gmc","jeep",
    "land rover","range rover","volvo","opel",
    "peugeot","citroen","renault","lada",
    "chery","geely","haval","jetour","exeed",
    "li","byd","xpeng","tesla","zeekr",
    "bentley","lamborghini","ferrari","rolls-royce",
    "changan"
]

# =========================
# MODELS
# =========================

COMMON_MODELS = [
    "camry","prado","corolla","rav4","land cruiser",
    "x5","x6","x3","x7","x-trail","q5","q7",
    "glc","gle","g63","c-class","e-class",
    "monjaro","jolion","atlas","coolray","l7","l9"
]

MODEL_PATTERNS = [
    r"(x\d)",
    r"(\d{1}-series)",
    r"(camry)",
    r"(prado)",
    r"(glc)",
    r"(gle)",
    r"(c-class)",
    r"(e-class)"
]

# =========================
# BRAND
# =========================

def extract_brand(text):

    if not text:
        return None

    t = text.lower()

    for b in BRANDS:

        if b in t:
            return b

    return None

# =========================
# MODEL
# =========================

def extract_model(text: str, brand: str | None):

    if not text:
        return None

    t = text.lower()

    # BMW patterns
    m = re.search(r"\b(x[1-7])\b", t)
    if m:
        return m.group(1)

    # numeric series
    m = re.search(r"\b([1-7]-series)\b", t)
    if m:
        return m.group(1)

    for model in COMMON_MODELS:
        if model in t:
            return model

    return None

# =========================
# PRICE
# =========================

def extract_price(text):

    if not text:
        return None

    # основной regex
    m = RE_PRICE.search(text)

    if m:
        try:

            price = int(
                m.group(1).replace(" ", "")
            )

            if 10000 < price < 200000000:
                return price

        except:
            pass

    # fallback паттерны
    lower = text.lower()

    for p in PRICE_PATTERNS:

        m = re.search(p, lower)

        if m:
            try:
                return int(m.group(1).replace(" ", ""))
            except:
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

    except:
        pass

    return None

# =========================
# MILEAGE
# =========================

def extract_mileage(text):

    if not text:
        return None

    m = RE_MILEAGE.search(text)

    if m:
        try:

            mileage = int(
                m.group(1).replace(" ", "")
            )

            if mileage < 1000000:
                return mileage

        except:
            pass

    m = RE_MILEAGE_ALT.search(text)

    if m:
        try:
            return int(m.group(1))
        except:
            pass

    return None

# =========================
# FUEL
# =========================

def extract_fuel(text):

    if not text:
        return None

    t = text.lower()

    for k, v in FUEL_MAP.items():

        if k in t:
            return v

    return None

# =========================
# MAIN EXTRACTOR
# =========================

def extract_car_entities(title, content):

    text = f"{title or ''} {content or ''}"

    brand = extract_brand(text)

    model = extract_model(text, brand)

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
        "fuel": fuel
    }