import re
import yaml
from typing import Dict, Tuple, Optional

# =========================
# CONSTANTS / THRESHOLDS
# =========================

MIN_TEXT_LEN = 20  # 🔥 Ослаблено для диагностики
MIN_BRAND_ONLY_LEN = 80  # 🔥 если есть бренд, но нет цены — принимаем только если текст достаточно длинный

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
        with open("/app/config/brands.yaml", "r", encoding="utf-8") as f:
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
    "обсуждение",
    "опрос",
    "новости",
    "ремонт",
    "диагностика",
    "ошибка",
    "проблема",
    "запчасти",
    "разбор",
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

RE_PRICE = re.compile(
    r"(\d[\d\s]{2,10})\s*(₽|руб|р\.?|тыс|k|к|\$|€|usd|eur)",
    re.IGNORECASE,
)
RE_YEAR = re.compile(r"\b(19\d{2}|20\d{2})\b")
RE_MILEAGE = re.compile(r"\d[\d\s]{1,8}\s*км")

RE_PHONE = re.compile(r"\+?\d[\d\-\(\)\s]{8,}")

SPAM_PATTERNS = [
    "подпишись",
    "подписывайтесь",
    "ссылка в био",
    "мой канал",
    "перейдите",
]

CAR_SELL_PATTERNS = [
    "двигатель",
    "пробег",
    "год",
    "комплектация",
    "коробка",
]

COMMON_MODELS = [
    "camry","rav4","corolla","prado",
    "x5","x6","x3","x1",
    "civic","accord","crv",
    "qashqai","xtrail",
    "sportage","sorento",
    "solaris","tucson",
]


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
                if alias.lower() in t:
                    return True

    return False


def contains_model(text: str) -> bool:

    if not text:
        return False

    t = text.lower()

    for m in COMMON_MODELS:
        if m in t:
            return True

    return False


def has_price(text: str) -> bool:
    """
    Цена — ОБЯЗАТЕЛЬНА для продажи.
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

    if RE_PHONE.search(t):
        score += 1

    for p in CAR_SELL_PATTERNS:
        if p in t:
            score += 0.5

    if score >= min_score:
        return True

    # fallback if price + brand
    if has_price(t) and contains_car_entity(t):
        return True

    return False


# =========================
# MAIN FILTER (PRE-INGEST)
# =========================

def is_valid_telegram_post(text: str) -> Tuple[bool, Optional[str]]:
    """
    Ослабленный Telegram pre-filter (диагностика).

    Возвращает:
      (ok, reason)
    """

    if not text:
        print("[TG_FILTER] skip reason=spam", flush=True)
        return False, "spam"

    t = text.lower().strip()

    stats = {
        "accepted": 0,
        "skipped_by_len": 0,
        "skipped_by_stop_word": 0,
        "skipped_by_no_brand": 0,
        "skipped_by_no_price_and_short": 0,
    }

    emoji_count = sum(1 for c in t if ord(c) > 10000)

    if emoji_count > 20:
        return False, "emoji_spam"

    if t.count("@") > 5:
        return False, "mention_spam"

    if t.count("http") > 3:
        return False, "link_spam"

    for s in SPAM_PATTERNS:
        if s in t:
            return False, "spam"

    # 1️⃣ минимальная длина
    if len(t) < MIN_TEXT_LEN:
        stats["skipped_by_len"] += 1
        print(f"[TG_FILTER] skip reason=spam stats={stats}", flush=True)
        return False, "spam"

    # 2️⃣ стоп-слова → обсуждения / сервисы
    for w in STOP_WORDS:
        if w in t:
            stats["skipped_by_stop_word"] += 1
            print(f"[TG_FILTER] skip reason=discussion stats={stats}", flush=True)
            return False, "discussion"

    # ✅ НОВАЯ ЛОГИКА ПРИНЯТИЯ (relax):
    # Документ сохраняется если есть хотя бы:
    # (brand AND price) ИЛИ (brand AND content_length>=N)
    brand_ok = contains_car_entity(t)
    model_ok = contains_model(t)
    price_ok = has_price(t)

    if not brand_ok and not model_ok:
        stats["skipped_by_no_brand"] += 1
        print(f"[TG_FILTER] skip reason=no_brand stats={stats}", flush=True)
        return False, "no_brand"

    if price_ok:
        stats["accepted"] += 1
        print(f"[TG_FILTER] accept reason=brand_and_price stats={stats}", flush=True)
        return True, "ok"

    if len(t) >= MIN_BRAND_ONLY_LEN:
        stats["accepted"] += 1
        print(f"[TG_FILTER] accept reason=brand_and_long_text stats={stats}", flush=True)
        return True, "ok"

    stats["skipped_by_no_price_and_short"] += 1
    print(f"[TG_FILTER] skip reason=no_price stats={stats}", flush=True)
    return False, "no_price"