import re


RE_YEAR = re.compile(r"\b(19\d{2}|20\d{2})\b")
RE_PRICE = re.compile(r"(\d[\d\s]{3,10})\s*(₽|руб|р)?", re.I)
RE_MILEAGE = re.compile(r"(\d[\d\s]{2,7})\s*(км|km)", re.I)

FUEL_MAP = {
    "бенз": "petrol",
    "бензин": "petrol",
    "petrol": "petrol",

    "дизель": "diesel",
    "diesel": "diesel",

    "гибрид": "hybrid",
    "hybrid": "hybrid",

    "электро": "electric",
    "electric": "electric"
}


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


def extract_price(text):

    if not text:
        return None

    m = RE_PRICE.search(text)

    if not m:
        return None

    try:

        price = int(m.group(1).replace(" ", ""))

        if 10000 < price < 200000000:
            return price

    except:
        pass

    return None


def extract_mileage(text):

    if not text:
        return None

    m = RE_MILEAGE.search(text)

    if not m:
        return None

    try:

        mileage = int(m.group(1).replace(" ", ""))

        if mileage < 1000000:
            return mileage

    except:
        pass

    return None


def extract_fuel(text):

    if not text:
        return None

    t = text.lower()

    for key, value in FUEL_MAP.items():

        if key in t:
            return value

    return None


def extract_car_entities(title, content):

    text = f"{title or ''} {content or ''}"

    year = extract_year(text)

    price = extract_price(text)

    mileage = extract_mileage(text)

    fuel = extract_fuel(text)

    return {
        "brand": None,
        "model": None,
        "year": year,
        "price": price,
        "mileage": mileage,
        "fuel": fuel
    }