import re
import yaml
from typing import Optional, Tuple, Dict, Any
from pathlib import Path

# =========================
# CONFIG / DEFAULTS
# =========================

DEFAULT_MIN_SALE_SCORE = int(
    __import__("os").getenv("MIN_SALE_SCORE", "2")
)

BASE_DIR = Path(__file__).resolve().parent.parent
BRANDS_YAML_PATH = BASE_DIR / "config" / "brands.yaml"

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
# QUALITY SCORE (0..1)
# =========================

def compute_quality_score(text: str) -> float:
    """
    Простая, стабильная эвристика качества (0..1)
    Используется ТОЛЬКО для ranking / explain, не для skip.
    """
    if not text:
        return 0.0

    signals = extract_quality_signals(text)

    score = 0.0
    score += 0.4 if signals.get("has_price") else 0.0
    score += 0.3 if signals.get("has_year") else 0.0
    score += 0.3 if signals.get("has_mileage") else 0.0

    return round(min(score, 1.0), 3)


# =========================
# BRAND DETECTION
# =========================

def detect_brand(text: str) -> Tuple[Optional[str], float]:
    """
    Canonical brand detection
    returns lowercase canonical brand
    """

    if not text:
        return None, 0.0

    text = text.lower()
    brands = _load_brands()

    for brand_key, cfg in brands.items():

        # exact en
        for v in cfg.get("en", []):
            if re.search(rf"\b{re.escape(v.lower())}\b", text):
                return brand_key.lower(), 1.0

        # exact ru
        for v in cfg.get("ru", []):
            if re.search(rf"\b{re.escape(v.lower())}\b", text):
                return brand_key.lower(), 1.0

        # alias
        for v in cfg.get("aliases", []):
            if re.search(rf"\b{re.escape(v.lower())}\b", text):
                return brand_key.lower(), 0.8

    return None, 0.0


def detect_model(text: str, brand: Optional[str]) -> Optional[str]:

    if not text or not brand:
        return None

    text = text.lower()

    pattern = rf"{brand}\s+([a-z0-9\-]+(?:\s+[a-z0-9\-]+)?)"

    m = re.search(pattern, text)

    if m:
        return m.group(1).strip()

    return None


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

    def log(self):
        print(
            f"[INGEST][QUALITY_GATE] total={self.total} "
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

        for w in DEFAULT_BLACKLIST_WORDS:
            if w in lower and not is_sale_intent(text):
                meta["reason"] = "blacklist_word"
                if stats:
                    stats.add(True, "blacklist_word")
                return True, meta

        sale = is_sale_intent(text)
        quality_score = compute_quality_score(text)

        meta["sale_intent"] = 1 if sale else 0
        meta["quality_score"] = quality_score

        # 🔒 QUALITY GATE
        if not sale or quality_score < 0.3:
            meta["reason"] = "low_quality_or_not_sale"
            if stats:
                stats.add(True, "low_quality_or_not_sale")
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
    boost = resolve_source_boost(source)
    quality_score = compute_quality_score(raw_text)

    meta["brand"] = brand
    meta["model"] = model
    meta["brand_confidence"] = float(brand_conf)
    meta["sale_intent"] = 1 if sale else 0
    meta["quality_score"] = quality_score
    meta["source_boost"] = float(boost)

    meta_prefix = (
        "__meta__: "
        f"brand={brand or 'none'}; "
        f"model={model or 'none'}; "
        f"brand_conf={round(brand_conf, 2)}; "
        f"sale_intent={1 if sale else 0}; "
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
    brand_confidence: float | None = None,
    sale_intent: bool | None = None,
    source_boost: float | None = None,
    quality_score: float | None = None,
) -> str:

    parts = []

    if brand:
        parts.append(f"brand={brand}")

    if brand_confidence is not None:
        parts.append(f"brand_conf={round(brand_confidence,2)}")

    if sale_intent is not None:
        parts.append(f"sale_intent={1 if sale_intent else 0}")

    if quality_score is not None:
        parts.append(f"quality_score={quality_score}")

    if source_boost:
        parts.append(f"source_boost={round(source_boost,2)}")

    if not parts:
        return ""

    return "__meta__: " + "; ".join(parts)


# =====================================================
# WARMUP
# =====================================================

try:
    _load_brands()
    print("[INGEST][QUALITY] brands loaded", flush=True)
except Exception as e:
    print(f"[INGEST][QUALITY][WARN] brands load failed: {e}", flush=True)