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

MODEL_TO_BRAND = {

"camry": "toyota",
"corolla": "toyota",
"rav4": "toyota",
"prado": "toyota",
"land cruiser": "toyota",
"hilux": "toyota",

"x1": "bmw",
"x3": "bmw",
"x5": "bmw",
"x6": "bmw",

"glc": "mercedes",
"gle": "mercedes",
"g63": "mercedes",
"v-class": "mercedes",

"x-trail": "nissan",

"monjaro": "geely",
"atlas": "geely",

"jolion": "haval",
"h6": "haval",

"seltos": "kia",
"pegas": "kia",

"cruze": "chevrolet",

"antara": "opel",

"discovery sport": "land rover",
}

# =========================
# MODELS
# =========================

COMMON_MODELS = [

# Toyota
"camry",
"corolla",
"rav4",
"prado",
"land cruiser",
"land cruiser prado",
"hilux",
"isis",

# BMW
"x1",
"x3",
"x5",
"x6",
"3 series",

# Mercedes
"glc",
"glc-class",
"gle",
"g63",
"v-class",
"e-class",
"c-class",
"s-class",

# Nissan
"x-trail",

# Geely
"monjaro",
"atlas",

# Haval
"jolion",
"h6",

# Kia
"seltos",
"pegas",

# Chevrolet
"cruze",

# Opel
"antara",

# Land Rover
"discovery sport",

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

    for brand in BRANDS:
        if brand in t:
            return brand

    for model, brand in MODEL_TO_BRAND.items():
        if model in t:
            return brand

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

def extract_price(text: str):

    if not text:
        return None

    # 800k / 1.2m / 🍋 support
    k_match = re.search(r"(\d+(?:\.\d+)?)\s?[kк]", text.lower())
    if k_match:
        try:
            value = float(k_match.group(1))
            return int(value * 1000)
        except:
            pass

    m_match = re.search(r"(\d+(?:\.\d+)?)\s?[mм]", text.lower())
    if m_match:
        try:
            value = float(m_match.group(1))
            return int(value * 1000000)
        except:
            pass

    lemon_match = re.search(r"(\d+)\s?🍋", text)
    if lemon_match:
        try:
            value = int(lemon_match.group(1))
            return value * 1000000
        except:
            pass

    t = text.replace("\xa0", " ")

    patterns = [

        r"(\d[\d\s]{4,})\s?₽",
        r"(\d[\d\s]{4,})\s?руб",
        r"(\d[\d\s]{4,})\s?rub",
        r"(\d[\d\s]{4,})\s?р",
        r"(\d[\d\s]{4,})\s?\$",
        r"(\d[\d\s]{4,})\s?€",
    ]

    for p in patterns:

        m = re.search(p, t, re.I)

        if m:

            raw = m.group(1)

            price = int(re.sub(r"\D", "", raw))

            if 10000 < price < 200000000:

                return price

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