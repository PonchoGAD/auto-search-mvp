# apps/api/src/data_pipeline/telegram_filters.py

import re
import yaml
from typing import Dict, Tuple, Optional

# =========================
# CONSTANTS / THRESHOLDS
# =========================

MIN_TEXT_LEN = 80

# =========================
# LOAD BRANDS WHITELIST
# =========================

def load_brands() -> Dict[str, dict]:
    """
    Загружаем brands.yaml.
    Используется ТОЛЬКО для быстрого pre-filter Telegram.
    Основная логика брендов — в ingest_quality.
    """
    try:
        with open("apps/api/src/config/brands.yaml", "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            return data.get("brands", {})
    except Exception:
        return {}


BRANDS_WHITELIST = load_brands()


# =========================
# REGEX / KEYWORDS
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
    "ремонт",
    "диагностика",
    "ошибка",
    "проблема",
    "запчасти",
    "разбор",
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
    "sale",
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
    "что лучше",
    "вопрос",
    # EN
    "looking for",
    "help",
    "question",
    "repair",
]

RE_YEAR = re.compile(r"\b(19\d{2}|20\d{2})\b")
RE_PRICE = re.compile(r"(\d[\d\s]{1,10})\s*(₽|руб|р\.|тыс|к|k|\$|€)")
RE_MILEAGE = re.compile(r"\d[\d\s]{1,8}\s*км")


# =========================
# HELPERS
# =========================

def contains_brand(text: str) -> bool:
    """
    Проверка на наличие бренда (RU / EN / aliases).
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
                if alias.lower() in t:
                    return True

    return False


def contains_digits(text: str) -> bool:
    """
    Есть ли признаки объявления:
    - год
    - цена
    - пробег
    """
    if not text:
        return False

    return bool(
        RE_YEAR.search(text)
        or RE_PRICE.search(text)
        or RE_MILEAGE.search(text)
    )


# =========================
# SALE INTENT (FAST)
# =========================

def is_sale_intent(text: str, min_score: int = 2) -> bool:
    """
    Упрощённый intent-фильтр для Telegram (быстро):

    +2 за позитивные слова
    +1 за цену / валюту
    -2 за негативные слова
    """
    if not text:
        return False

    t = text.lower()
    score = 0

    for w in SALE_POSITIVE_WORDS:
        if w in t:
            score += 2

    if RE_PRICE.search(t):
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
    Жёсткий Telegram pre-filter.
    Используется ДО ingest и ДО RawDocument.

    Возвращает:
      (ok, reason)

    reason нужен для логирования / статистики.
    """

    if not text:
        return False, "empty_text"

    t = text.lower()

    # 1️⃣ короткие сообщения — почти всегда шум
    if len(t) < MIN_TEXT_LEN:
        return False, "text_too_short"

    # 2️⃣ стоп-слова (глобальный шум)
    for w in STOP_WORDS:
        if w in t:
            return False, "stop_word"

    # 3️⃣ intent продажи
    if not is_sale_intent(t):
        return False, "not_sale_intent"

    # 4️⃣ бренд (иначе это обсуждение)
    if not contains_brand(t):
        return False, "no_brand"

    # 5️⃣ цифры (цена / год / пробег)
    if not contains_digits(t):
        return False, "no_numeric_signals"

    return True, "ok"
