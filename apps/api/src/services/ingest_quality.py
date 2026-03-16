import re
from typing import Optional, Tuple, Dict, Any

from services.model_resolver import resolve_model
from services.brand_detector import detect_brand as detect_brand_main

# taxonomy_service is the single source of truth for brand/model canonicalization.

# =========================
# CONFIG / DEFAULTS
# =========================

DEFAULT_MIN_SALE_SCORE = int(
    __import__("os").getenv("MIN_SALE_SCORE", "2")
)

# 🆕 Anti-noise thresholds (VPS-safe defaults)
DEFAULT_MIN_TEXT_LEN = int(__import__("os").getenv("MIN_TEXT_LEN", "80"))

DEFAULT_MIN_PRICE_RUB = int(__import__("os").getenv("MIN_PRICE_RUB", "150000"))
DEFAULT_MAX_PRICE_RUB = int(__import__("os").getenv("MAX_PRICE_RUB", "20000000"))

DEFAULT_MIN_YEAR = int(__import__("os").getenv("MIN_YEAR", "1995"))
DEFAULT_MAX_MILEAGE_KM = int(__import__("os").getenv("MAX_MILEAGE_KM", "400000"))

# 🆕 blacklist words (anti-noise)
DEFAULT_BLACKLIST_WORDS = [
    "ищу",
    "куплю",
    "вопрос",
    "подскажите",
    "помогите",
    "что лучше",
    "ремонт",
    "диагностика",
    "ошибка",
    "проблема",
    "запчасти",
    "разбор",
]

# =========================
# 🆕 PARTS BLACKLIST
# =========================

PARTS_BLACKLIST = [
    "фары отдельно",
    "бампер отдельно",
    "капот отдельно",
    "дверь отдельно",
    "крыло отдельно",
    "редуктор отдельно",
    "двигатель отдельно",
    "акпп отдельно",
    "кпп отдельно",
    "ноускат",
    "разборка",
    "на запчасти",
]

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

PRICE_PATTERN = re.compile(
    r"(\b\d{3,}\b\s?(руб|₽|р\.|\$|€|тыс|к|k))",
    re.IGNORECASE,
)

PRICE_ANY_PATTERN = re.compile(
    r"(до|<=|<)?\s*(\d+[\d\s]*)\s*(млн|миллион|m|тыс|к|k|₽|руб|р\.|\$|€)",
    re.IGNORECASE,
)
YEAR_PATTERN = re.compile(r"\b(19\d{2}|20\d{2})\b")
MILEAGE_PATTERN = re.compile(
    r"(пробег)?\s*(до|<=|<)?\s*(\d+[\d\s]*)\s*(км|тыс)",
    re.IGNORECASE,
)

REQUIRED_SIGNALS_ANY = ("price", "year", "mileage")

NOISE_PATTERNS = [
    re.compile(r"\bhttp(s)?://\S+\b", re.IGNORECASE),
    re.compile(r"\btelegram\.me/\S+\b", re.IGNORECASE),
    re.compile(r"\bt\.me/\S+\b", re.IGNORECASE),
    re.compile(r"\bподпис(ывай|ыва)т(ь|есь)\b", re.IGNORECASE),
    re.compile(r"\bлайк\b|\bрепост\b|\bподел(ись|итесь)\b", re.IGNORECASE),
]


# =========================
# BRAND / MODEL ADAPTERS
# =========================

def detect_brand(text: str) -> Tuple[Optional[str], float]:
    if not text:
        return None, 0.0

    try:
        brand, conf = detect_brand_main(title=text, text=text)
        return brand, float(conf or 0.0)
    except TypeError:
        try:
            detected = detect_brand_main(text)
            if isinstance(detected, tuple):
                brand = detected[0] if len(detected) > 0 else None
                conf = detected[1] if len(detected) > 1 else 0.0
                return brand, float(conf or 0.0)
            return detected, 0.0
        except Exception:
            return None, 0.0
    except Exception:
        return None, 0.0


def detect_model(text: str, brand: Optional[str]) -> Optional[str]:
    if not text or not brand:
        return None

    try:
        return resolve_model(brand, text)
    except Exception:
        return None


# =========================
# SALE INTENT
# =========================

def is_sale_intent(text: str, min_score: int = DEFAULT_MIN_SALE_SCORE) -> bool:
    if not text:
        return False

    text = text.lower()
    score = 0

    for w in POSITIVE_WORDS_RU + POSITIVE_WORDS_EN:
        if w in text:
            score += 2

    if PRICE_PATTERN.search(text):
        score += 1

    for w in NEGATIVE_WORDS_RU + NEGATIVE_WORDS_EN:
        if w in text:
            score -= 2

    return score >= min_score


# =========================
# QUALITY SIGNAL EXTRACTION
# =========================

def extract_quality_signals(text: str) -> Dict[str, bool]:

    signals = {
        "has_price": False,
        "has_year": False,
        "has_mileage": False,
        "has_brand": False,
        "has_model": False,
    }

    if not text:
        return signals

    t = text.lower()

    if PRICE_PATTERN.search(t) or PRICE_ANY_PATTERN.search(t):
        signals["has_price"] = True

    if YEAR_PATTERN.search(t):
        signals["has_year"] = True

    if MILEAGE_PATTERN.search(t):
        signals["has_mileage"] = True

    brand, _ = detect_brand(text)
    model = detect_model(text, brand) if brand else None

    signals["has_brand"] = bool(brand)
    signals["has_model"] = bool(model)

    return signals


# =========================
# QUALITY SCORE (0..1)
# =========================

def compute_quality_score(text: str) -> float:
    if not text:
        return 0.0

    signals = extract_quality_signals(text)

    score = 0.0
    score += 0.25 if signals.get("has_price") else 0.0
    score += 0.15 if signals.get("has_year") else 0.0
    score += 0.15 if signals.get("has_mileage") else 0.0
    score += 0.20 if signals.get("has_brand") else 0.0
    score += 0.25 if signals.get("has_model") else 0.0

    return float(round(min(score, 1.0), 3))


def _is_telegram_noise(text: str) -> bool:
    t = (text or "").lower()

    hard_noise = [
        "масло",
        "редуктор",
        "допуск",
        "подписывайтесь",
        "репост",
        "лайк",
        "диски",
        "резина",
        "колеса",
        "шины",
        "разбор",
        "запчаст",
        "км/ч",
        "скорость",
    ]

    if any(x in t for x in hard_noise):
        return True

    brand, _ = detect_brand(text)
    model = detect_model(text, brand) if brand else None
    has_price = bool(PRICE_PATTERN.search(t) or PRICE_ANY_PATTERN.search(t))

    if has_price and not brand and not model:
        discussion_words = [
            "это норм",
            "это цена",
            "шутка",
            "реальная цена",
            "подскажите",
            "кто знает",
            "?",
        ]
        if any(x in t for x in discussion_words):
            return True

    return False


def _extract_entity_signals(text: str) -> Dict[str, bool]:
    signals = extract_quality_signals(text)

    brand, _ = detect_brand(text)
    model = detect_model(text, brand) if brand else None

    signals["has_brand"] = bool(brand)
    signals["has_model"] = bool(model)
    return signals


# =========================
# SOURCE BOOST
# =========================

SOURCE_BOOSTS = {
    "forum": 1.5,
    "telegram": 1.0,
    "marketplace": 0.8,
}


def resolve_source_boost(source: str) -> float:
    if not source:
        return 1.0

    s = source.lower()

    if "club" in s or "forum" in s:
        return SOURCE_BOOSTS["forum"]

    if "telegram" in s:
        return SOURCE_BOOSTS["telegram"]

    return SOURCE_BOOSTS["marketplace"]


# =========================
# 🆕 STATS (IN-MEMORY)
# =========================

class SkipStats:
    def __init__(self):
        self.total = 0
        self.kept = 0
        self.skipped = 0
        self.by_reason: Dict[str, int] = {}

    def add(self, skip: bool, reason: str):
        self.total += 1
        if skip:
            self.skipped += 1
            self.by_reason[reason] = self.by_reason.get(reason, 0) + 1
        else:
            self.kept += 1

    def log(self, prefix: str = "[INGEST][QUALITY_GATE]"):
        print(
            f"{prefix} total={self.total} "
            f"kept={self.kept} skipped={self.skipped} "
            f"reasons={self.by_reason}"
        )


# =========================
# MAIN QUALITY GATE
# =========================

def should_skip_doc(
    *,
    text: str,
    source: str = "",
    stats: Optional[SkipStats] = None,
) -> Tuple[bool, Dict[str, Any]]:
    meta: Dict[str, Any] = {}

    try:
        if not text:
            meta["reason"] = "empty_text"
            if stats:
                stats.add(True, "empty_text")
            return True, meta

        lower = text.lower()
        sale = is_sale_intent(text)
        sale_intent = 1 if sale else 0
        quality_score = float(compute_quality_score(text))
        is_tg = "telegram" in (source or "").lower()

        meta["sale_intent"] = sale_intent
        meta["quality_score"] = quality_score

        if is_tg and _is_telegram_noise(text):
            meta["reason"] = "telegram_noise"
            if stats:
                stats.add(True, "telegram_noise")
            return True, meta

        if len(text.strip()) < DEFAULT_MIN_TEXT_LEN and not sale and not is_tg:
            brand_tmp, _ = detect_brand(text)
            model_tmp = detect_model(text, brand_tmp) if brand_tmp else None
            has_price_tmp = bool(PRICE_PATTERN.search(lower) or PRICE_ANY_PATTERN.search(lower))
            has_year_tmp = bool(YEAR_PATTERN.search(lower))

            if not brand_tmp and not model_tmp and not has_price_tmp and not has_year_tmp:
                meta["reason"] = "too_short"
                if stats:
                    stats.add(True, "too_short")
                return True, meta

        for w in PARTS_BLACKLIST:
            if w in lower:
                meta["reason"] = "parts_listing"
                if stats:
                    stats.add(True, "parts_listing")
                return True, meta

        brand_tmp, _ = detect_brand(text)
        model_tmp = detect_model(text, brand_tmp) if brand_tmp else None
        has_price_tmp = bool(PRICE_PATTERN.search(lower) or PRICE_ANY_PATTERN.search(lower))

        for w in DEFAULT_BLACKLIST_WORDS:
            if w in lower and not sale:
                if is_tg:
                    meta["reason"] = "blacklist_word"
                    if stats:
                        stats.add(True, "blacklist_word")
                    return True, meta
                else:
                    if not brand_tmp and not model_tmp and not has_price_tmp:
                        meta["reason"] = "blacklist_word"
                        if stats:
                            stats.add(True, "blacklist_word")
                        return True, meta

        signals = _extract_entity_signals(text)

        strong_signals = sum([
            1 if signals["has_brand"] else 0,
            1 if signals["has_model"] else 0,
            1 if signals["has_price"] else 0,
            1 if signals["has_year"] else 0,
            1 if signals["has_mileage"] else 0,
        ])

        if is_tg:
            if strong_signals < 2 and not sale:
                meta["reason"] = "low_entity_signal"
                if stats:
                    stats.add(True, "low_entity_signal")
                return True, meta
        else:
            if strong_signals == 0 and not sale:
                meta["reason"] = "low_entity_signal_marketplace"
                if stats:
                    stats.add(True, "low_entity_signal_marketplace")
                return True, meta

        meta["reason"] = "ok"
        if stats:
            stats.add(False, "ok")
        return False, meta

    except Exception as e:
        meta["reason"] = "exception"
        meta["error"] = str(e)
        if stats:
            stats.add(True, "exception")
        return True, meta


# =========================
# ONE-SHOT ENRICH
# =========================

def enrich_text_with_meta(
    *,
    raw_text: str,
    source: str,
) -> Tuple[str, Dict[str, Any]]:
    meta: Dict[str, Any] = {}

    brand, brand_conf = detect_brand(raw_text)
    model = detect_model(raw_text, brand)

    sale = is_sale_intent(raw_text)
    sale_intent = 1 if sale else 0
    boost = float(resolve_source_boost(source))
    quality_score = float(compute_quality_score(raw_text))

    meta["brand"] = brand or None
    meta["model"] = model or None
    meta["brand_confidence"] = float(brand_conf or 0.0)
    meta["sale_intent"] = sale_intent
    meta["quality_score"] = quality_score
    meta["source_boost"] = boost

    meta_prefix = (
        "__meta__: "
        f"brand={brand or 'none'}; "
        f"model={model or 'none'}; "
        f"brand_conf={round(float(brand_conf or 0.0), 2)}; "
        f"sale_intent={sale_intent}; "
        f"quality_score={quality_score}; "
        f"source_boost={round(boost, 2)}"
    )

    content = f"{meta_prefix}\n{raw_text}"
    return content, meta


# =====================================================
# META PREFIX BUILDER
# =====================================================

def build_meta_prefix(
    brand: str | None = None,
    model: str | None = None,
    brand_confidence: float | None = None,
    sale_intent: bool | int | None = None,
    source_boost: float | None = None,
    quality_score: float | None = None,
) -> str:

    parts = []

    if brand:
        parts.append(f"brand={brand}")

    if model:
        parts.append(f"model={model}")

    if brand_confidence is not None:
        parts.append(f"brand_conf={round(float(brand_confidence), 2)}")

    if sale_intent is not None:
        parts.append(f"sale_intent={1 if bool(sale_intent) else 0}")

    if quality_score is not None:
        parts.append(f"quality_score={float(quality_score)}")

    if source_boost is not None:
        parts.append(f"source_boost={round(float(source_boost), 2)}")

    if not parts:
        return ""

    return "__meta__: " + "; ".join(parts)


# =====================================================
# APPLY META PREFIX
# =====================================================

def apply_meta_prefix(text: str, meta_prefix: str) -> str:
    """
    Добавляет meta prefix к тексту.
    normalize.py ожидает именно эту функцию.
    """

    if not text:
        return meta_prefix or ""

    if not meta_prefix:
        return text

    return f"{meta_prefix}\n{text}"