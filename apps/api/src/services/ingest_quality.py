# apps/api/src/services/ingest_quality.py

import re
import yaml
from typing import Optional, Tuple

# =========================
# CONFIG / DEFAULTS
# =========================

DEFAULT_MIN_SALE_SCORE = int(
    __import__("os").getenv("MIN_SALE_SCORE", "2")
)

# путь к brands.yaml (единый для проекта)
BRANDS_YAML_PATH = "apps/api/src/config/brands.yaml"

# =========================
# SALE INTENT DICTIONARIES
# =========================

POSITIVE_WORDS_RU = [
    "продам",
    "продаю",
    "продаётся",
    "продается",
    "продажа",
    "срочно продам",
    "торг",
    "обмен",
    "рассмотрю обмен",
]

POSITIVE_WORDS_EN = [
    "for sale",
    "sale",
    "selling",
    "sell",
]

NEGATIVE_WORDS_RU = [
    "ищу",
    "куплю",
    "нужен",
    "подскажите",
    "помогите",
    "обсуждение",
    "вопрос",
    "что лучше",
    "ремонт",
    "не заводится",
    "ошибка",
    "диагностика",
]

NEGATIVE_WORDS_EN = [
    "looking for",
    "help",
    "question",
    "repair",
]

# валюты / цена (простая эвристика)
PRICE_PATTERN = re.compile(
    r"(\b\d{3,}\b\s?(руб|₽|р\.|\$|€|тыс|к|k))",
    re.IGNORECASE,
)

# =========================
# BRAND CACHE
# =========================

_BRANDS_CACHE = None


def _load_brands():
    global _BRANDS_CACHE

    if _BRANDS_CACHE is not None:
        return _BRANDS_CACHE

    try:
        with open(BRANDS_YAML_PATH, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            _BRANDS_CACHE = data.get("brands", {})
    except Exception:
        _BRANDS_CACHE = {}

    return _BRANDS_CACHE


# =========================
# SALE INTENT
# =========================

def is_sale_intent(text: str, min_score: int = DEFAULT_MIN_SALE_SCORE) -> bool:
    """
    Определяет, является ли текст объявлением о продаже.

    scoring:
    +2 за позитивные слова
    +1 за цену / валюту
    -2 за негативные слова

    sale_intent = score >= min_score
    """

    if not text:
        return False

    text = text.lower()
    score = 0

    # позитив
    for w in POSITIVE_WORDS_RU + POSITIVE_WORDS_EN:
        if w in text:
            score += 2

    # цена
    if PRICE_PATTERN.search(text):
        score += 1

    # негатив
    for w in NEGATIVE_WORDS_RU + NEGATIVE_WORDS_EN:
        if w in text:
            score -= 2

    return score >= min_score


# =========================
# BRAND DETECTION
# =========================

def detect_brand(text: str) -> Tuple[Optional[str], float]:
    """
    Возвращает:
      (brand_key | None, confidence)

    confidence:
      exact = 1.0
      alias = 0.7
    """

    if not text:
        return None, 0.0

    text = text.lower()
    brands = _load_brands()

    for brand_key, cfg in brands.items():
        # exact en
        for v in cfg.get("en", []):
            if v.lower() in text:
                return brand_key, 1.0

        # exact ru
        for v in cfg.get("ru", []):
            if v.lower() in text:
                return brand_key, 1.0

        # aliases
        for v in cfg.get("aliases", []):
            if v.lower() in text:
                return brand_key, 0.7

    return None, 0.0


# =========================
# META PREFIX (MVP MODE)
# =========================

def build_meta_prefix(
    *,
    brand: Optional[str],
    brand_confidence: float,
    sale_intent: bool,
    source_boost: float,
) -> str:
    """
    Формирует meta-prefix для content без миграций БД.

    Формат:
    __meta__: brand=bmw; brand_conf=1.0; sale_intent=1; source_boost=1.5
    """

    return (
        "__meta__: "
        f"brand={brand or 'none'}; "
        f"brand_conf={round(brand_confidence, 2)}; "
        f"sale_intent={1 if sale_intent else 0}; "
        f"source_boost={round(source_boost, 2)}"
    )


# =========================
# SOURCE BOOST
# =========================

SOURCE_BOOSTS = {
    "forum": 1.5,
    "telegram": 1.0,
    "marketplace": 0.8,
}


def resolve_source_boost(source: str) -> float:
    """
    Преобразует source -> boost.
    """

    if not source:
        return 1.0

    s = source.lower()

    if "club" in s or "forum" in s:
        return SOURCE_BOOSTS["forum"]

    if "telegram" in s:
        return SOURCE_BOOSTS["telegram"]

    return SOURCE_BOOSTS["marketplace"]
