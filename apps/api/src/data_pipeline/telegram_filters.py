import re
import yaml
from typing import Dict, Tuple, Optional
from pathlib import Path

# =========================
# CONSTANTS / THRESHOLDS
# =========================

MIN_TEXT_LEN = 80

# =========================
# LOAD BRANDS / MODELS WHITELIST
# =========================

def load_brands() -> Dict[str, dict]:
    """
    Загружаем brands.yaml.
    Используется ТОЛЬКО для быстрого pre-filter Telegram.
    Основная логика брендов — в ingest_quality.
    """
    try:
        base_dir = Path(__file__).resolve().parent.parent
        brands_path = base_dir / "config" / "brands.yaml"

        with open(brands_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            return data.get("brands", {})
    except Exception:
        return {}


def load_models() -> Dict[str, dict]:
    """
    Загружаем models.yaml для быстрого поиска model aliases.
    """
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


# =========================
# REGEX / KEYWORDS
# =========================

STOP_WORDS = [
    "подписывайтесь",
    "вакансия",
    "работа",
    "скидк",
    "акция",
    "обсуждение",
    "опрос",
    "новости",
    "ищу",
    "куплю",
]

SALE_POSITIVE_WORDS = [
    # RU
    "продам",
    "продаю",
    "продается",
    "продаётся",
    "продажа",
    "срочно продам",
    "торг",
    "обмен",
    "рассмотрю обмен",
    # EN
    "for sale",
    "selling",
    "sell",
]

SALE_NEGATIVE_WORDS = [
    # RU
    "ищу",
    "куплю",
    "нужен",
    "подскажите",
    "помогите",
    "вопрос",
    # EN
    "looking for",
    "help",
    "question",
    "repair",
]

RE_PRICE = re.compile(r"(\d[\d\s]{1,10})\s*(₽|руб|р\.|тыс|к|k|\$|€)")
RE_YEAR = re.compile(r"\b(19\d{2}|20\d{2})\b")
RE_MILEAGE = re.compile(r"\d[\d\s]{1,8}\s*км")


# =========================
# HELPERS
# =========================

def contains_car_entity(text: str) -> bool:
    """
    Проверка на наличие бренда или модели.
    Быстро, без confidence.
    """
    if not text:
        return False

    t = text.lower()

    for brand_data in BRANDS_WHITELIST.values():
        for group in brand_data.values():
            if not isinstance(group, list):
                continue
            for alias in group:
                if alias and alias.lower() in t:
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
    """
    Цена — сильный сигнал продажи.
    """
    if not text:
        return False
    return bool(RE_PRICE.search(text))


# =========================
# SALE INTENT (FAST)
# =========================

def is_sale_intent(text: str, min_score: int = 2) -> bool:
    """
    Упрощённый intent-фильтр для Telegram:

    +2 за позитивные слова
    +1 за цену
    -2 за негативные слова
    """
    if not text:
        return False

    t = text.lower()
    score = 0

    for w in SALE_POSITIVE_WORDS:
        if w in t:
            score += 2

    if has_price(t):
        score += 1

    for w in SALE_NEGATIVE_WORDS:
        if w in t:
            score -= 2

    return score >= min_score


# =========================
# MAIN FILTER (PRE-INGEST)
# =========================

def is_valid_telegram_post(text: str) -> Tuple[bool, Optional[str]]:
    """
    Смягчённый Telegram pre-filter.
    Используется ДО ingest и ДО RawDocument.

    Возвращает:
      (ok, reason)

    reason используется для аналитики.
    """

    if not text:
        return False, "spam"

    t = text.lower().strip()

    # 1️⃣ минимальная длина
    if len(t) < MIN_TEXT_LEN:
        return False, "spam"

    # 2️⃣ hard-stop words
    for w in STOP_WORDS:
        if w in t:
            return False, "discussion"

    sale_intent = is_sale_intent(t)
    price = has_price(t)
    car_entity = contains_car_entity(t)

    # 3️⃣ если есть цена и car_entity -> ok, даже если sale_intent слабый
    if price and car_entity:
        return True, "ok"

    # 4️⃣ если нет sale_intent и нет price -> discussion
    if not sale_intent and not price:
        return False, "discussion"

    # 5️⃣ если нет price -> no_price
    if not price:
        return False, "no_price"

    # 6️⃣ если нет car_entity -> no_car_entity
    if not car_entity:
        return False, "no_car_entity"

    return True, "ok"