import re


BUY_WORDS = [
    "купить",
    "ищу",
    "нужна",
    "нужен",
    "подобрать",
    "подбор",
    "варианты",
    "что взять",
    "что купить",
    "продажа",
    "цена",
    "стоимость",
    "до",
    "от",
    "млн",
    "тыс",
    "к",
    "пробег",
    "дизель",
    "бенз",
    "бензин",
    "гибрид",
    "электро",
    "km",
    "км",
]

BRAND_TOKENS = [
    "bmw", "toyota", "mercedes", "audi", "nissan", "kia", "hyundai",
    "lexus", "mazda", "honda", "volkswagen", "skoda", "porsche",
    "geely", "chery", "haval", "mitsubishi", "subaru", "renault",
    "ford", "chevrolet", "infiniti",
]

MODEL_HINTS = [
    "camry", "corolla", "rav4", "rav-4", "prado", "x5", "x3", "x6", "x7",
    "qashqai", "x-trail", "sportage", "sorento", "solaris", "tucson",
    "monjaro", "coolray", "glc", "gle", "gls", "c-class", "e-class",
]


def detect_car_intent(query: str):
    if not query:
        return "browse"

    q = query.lower().strip()

    if any(w in q for w in BUY_WORDS):
        return "buy"

    if any(b in q for b in BRAND_TOKENS):
        return "buy"

    if any(m in q for m in MODEL_HINTS):
        return "buy"

    if re.search(r"\b(19\d{2}|20\d{2})\b", q):
        return "buy"

    if re.search(r"\b\d+(?:[.,]\d+)?\s*(млн|тыс|к|₽|руб|р)\b", q):
        return "buy"

    if re.search(r"\b\d+\s*(км|km|т\.км|тыс\s*км)\b", q):
        return "buy"

    return "browse"
