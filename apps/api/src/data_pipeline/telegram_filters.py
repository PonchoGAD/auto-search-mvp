import re
import yaml
from typing import Dict, Tuple, Optional
from pathlib import Path

MIN_TEXT_LEN = 80
MIN_BRAND_ONLY_LEN = 120


def load_brands() -> Dict[str, dict]:
    try:
        base_dir = Path(__file__).resolve().parent.parent
        brands_path = base_dir / "config" / "brands.yaml"
        with open(brands_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            return data.get("brands", {})
    except Exception:
        return {}


def load_models() -> Dict[str, dict]:
    try:
        base_dir = Path(__file__).resolve().parent.parent
        models_path = base_dir / "config" / "models.yaml"
        with open(models_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            return data.get("models", {})
    except Exception:
        return {}


BRANDS_WHITELIST = load_brands()
MODELS_WHITELIST = load_models()

# 🔥 ЖЕСТКИЙ СПИСОК СТОП-СЛОВ (Всё, что связано с ремонтом и запчастями)
STOP_WORDS =[
    "подписывайтесь", "вакансия", "работа", "скидк", "акция", "обсуждение",
    "опрос", "новости", "ремонт", "диагностика", "ошибка", "проблема",
    "запчасти", "разбор", "по запчастям", "бампер", "фара", "капот",
    "крыло", "дверь", "двигатель", "двс", "акпп", "мкпп", "коробка",
    "приборка", "диски", "шины", "колеса", "резина", "мишлен", "michelin",
    "чек", "подскажите", "вопрос", "куплю", "ищу", "приобрету", "замена",
    "сервис", "колодки", "масло", "ваносы", "ксентри", "xentry",
    "кодирование", "чиптюнинг", "чип тюнинг"
]

SALE_POSITIVE_WORDS =[
    "продам", "продаю", "продается", "продаётся", "продажа",
    "срочно продам", "торг", "обмен", "рассмотрю обмен",
    "for sale", "selling", "sell",
]

SALE_NEGATIVE_WORDS =[
    "ищу", "куплю", "нужен", "подскажите", "помогите", "вопрос",
    "looking for", "help", "question", "repair",
]

RE_PRICE = re.compile(
    r"(\d[\d\s]{2,10})\s*(₽|руб|р\.?|тыс|k|к|\$|€|usd|eur)",
    re.IGNORECASE,
)
RE_PHONE = re.compile(r"\+?\d[\d\-\(\)\s]{8,}")
RE_MILEAGE = re.compile(r"\d[\d\s]{1,8}\s*км", re.IGNORECASE)
RE_YEAR = re.compile(r"\b(19[8-9]\d|20[0-2]\d)\b") # 🔥 Обязательное условие: год от 1980 до 2029

SPAM_PATTERNS =[
    "подпишись", "подписывайтесь", "ссылка в био", "мой канал", "перейдите",
]

CAR_SELL_PATTERNS =[
    "двигатель", "пробег", "год", "комплектация", "коробка",
]


def contains_car_entity(text: str) -> bool:
    if not text:
        return False

    t = text.lower()

    for brand_data in BRANDS_WHITELIST.values():
        if not isinstance(brand_data, dict):
            continue
        for group in brand_data.values():
            if not isinstance(group, list):
                continue
            for alias in group:
                if isinstance(alias, str) and alias.strip() and alias.lower() in t:
                    return True

    for model_data in MODELS_WHITELIST.values():
        if isinstance(model_data, dict):
            for group in model_data.values():
                if not isinstance(group, list):
                    continue
                for alias in group:
                    if isinstance(alias, str) and len(alias.strip()) >= 3 and alias.lower() in t:
                        return True
        elif isinstance(model_data, list):
            for alias in model_data:
                if isinstance(alias, str) and len(alias.strip()) >= 3 and alias.lower() in t:
                    return True

    return False


def has_price(text: str) -> bool:
    if not text:
        return False
    return bool(RE_PRICE.search(text))


def is_sale_intent(text: str, min_score: int = 2) -> bool:
    if not text:
        return False

    t = text.lower()
    score = 0.0

    for w in SALE_POSITIVE_WORDS:
        if w in t:
            score += 2

    if has_price(t):
        score += 1

    if RE_PHONE.search(t):
        score += 1

    if RE_MILEAGE.search(t):
        score += 0.5

    for p in CAR_SELL_PATTERNS:
        if p in t:
            score += 0.5

    for w in SALE_NEGATIVE_WORDS:
        if w in t:
            score -= 2

    if score >= min_score:
        return True

    if has_price(t) and contains_car_entity(t):
        return True

    return False


def is_valid_telegram_post(text: str) -> Tuple[bool, str]:
    if not text:
        return False, "spam"

    t = text.lower().strip()

    # Обязательная проверка: Если нет года (напр. 2018), это не продажа целого авто!
    if not RE_YEAR.search(t):
        return False, "no_year_found"

    emoji_count = sum(1 for c in t if ord(c) > 10000)
    if emoji_count > 20:
        return False, "emoji_spam"

    if t.count("@") > 5:
        return False, "mention_spam"

    if t.count("http") > 3:
        return False, "link_spam"

    for s in SPAM_PATTERNS:
        if s in t:
            return False, "spam_pattern"

    if len(t) < MIN_TEXT_LEN:
        return False, "too_short"

    # 🔥 Блокировка запчастей и ремонта с использованием точного поиска (границы слова)
    # Чтобы слово "дверь" блокировало объявление, но не блокировало случайное слово внутри другого
    for w in STOP_WORDS:
        if re.search(rf"\b{w}\b", t):
            return False, f"blocked_by_stop_word_{w}"

    sale_ok = is_sale_intent(t)
    price_ok = has_price(t)
    entity_ok = contains_car_entity(t)

    if price_ok and entity_ok:
        return True, "ok"

    if sale_ok and entity_ok and len(t) >= MIN_BRAND_ONLY_LEN:
        return True, "ok"

    if not entity_ok:
        return False, "no_car_entity"

    if not price_ok:
        return False, "no_price"

    return False, "discussion"