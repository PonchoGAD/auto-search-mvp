import re
import yaml

MIN_TEXT_LEN = 80


# =========================
# LOAD BRANDS WHITELIST
# =========================

def load_brands():
    try:
        with open("brands.yaml", "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            return data.get("brands", {})
    except Exception:
        return {}


BRANDS_WHITELIST = load_brands()


# =========================
# FILTERS
# =========================

STOP_WORDS = [
    "подписывайтесь",
    "вакансия",
    "работа",
    "скидк",
    "акция",
    "ищу",
    "куплю",
    "обсуждение",
    "опрос",
    "новости",
]

RE_YEAR = re.compile(r"\b(19\d{2}|20\d{2})\b")
RE_PRICE = re.compile(r"(\d[\d\s]{1,10})\s*(₽|руб|тыс)")
RE_MILEAGE = re.compile(r"\d[\d\s]{1,8}\s*км")


def contains_brand(text: str) -> bool:
    t = text.lower()
    for aliases in BRANDS_WHITELIST.values():
        for a in aliases:
            if a.lower() in t:
                return True
    return False


def contains_digits(text: str) -> bool:
    return bool(
        RE_YEAR.search(text)
        or RE_PRICE.search(text)
        or RE_MILEAGE.search(text)
    )


def is_valid_telegram_post(text: str) -> bool:
    if not text:
        return False

    t = text.lower()

    if len(t) < MIN_TEXT_LEN:
        return False

    for w in STOP_WORDS:
        if w in t:
            return False

    # 🔑 ключевая логика
    if not contains_brand(t):
        return False

    if not contains_digits(t):
        return False

    return True
