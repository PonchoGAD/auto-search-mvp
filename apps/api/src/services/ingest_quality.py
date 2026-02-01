# apps/api/src/services/ingest_quality.py

import re
import yaml
from typing import Optional, Tuple, Dict, Any

# =========================
# CONFIG / DEFAULTS
# =========================

DEFAULT_MIN_SALE_SCORE = int(
    __import__("os").getenv("MIN_SALE_SCORE", "2")
)

# путь к brands.yaml (единый для проекта)
BRANDS_YAML_PATH = "apps/api/src/config/brands.yaml"

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

# валюты / цена (простая эвристика)
PRICE_PATTERN = re.compile(
    r"(\b\d{3,}\b\s?(руб|₽|р\.|\$|€|тыс|к|k))",
    re.IGNORECASE,
)

# 🆕 дополнительные паттерны под цену/год/пробег (мягко, MVP)
PRICE_ANY_PATTERN = re.compile(
    r"(до|<=|<)?\s*(\d+[\d\s]*)\s*(млн|миллион|m|тыс|к|k|₽|руб|р\.|\$|€)",
    re.IGNORECASE,
)
YEAR_PATTERN = re.compile(r"\b(19\d{2}|20\d{2})\b")
MILEAGE_PATTERN = re.compile(
    r"(пробег)?\s*(до|<=|<)?\s*(\d+[\d\s]*)\s*(км|тыс)",
    re.IGNORECASE,
)

# 🆕 обязательные сигналы "это похоже на объявление"
# (используется в правиле: если sale_intent=false и нет price/year/mileage -> skip)
REQUIRED_SIGNALS_ANY = ("price", "year", "mileage")

# 🆕 доменные/сервисные шум-артефакты, которые часто прилетают из парсеров
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


# =========================
# 🆕 ANTI-NOISE HELPERS
# =========================

def normalize_text_for_rules(text: str) -> str:
    """
    Мягкая нормализация для правил:
    - lower
    - схлоп пробелы
    - убираем повторяющиеся шумовые шаблоны (ссылки/призывы)
    """
    t = (text or "").lower()
    t = re.sub(r"\s+", " ", t).strip()

    # удаляем явно шумовые куски (не обязательно, но улучшает качество эвристик)
    for pat in NOISE_PATTERNS:
        t = pat.sub(" ", t)

    t = re.sub(r"\s+", " ", t).strip()
    return t


def has_blacklist_words(text: str, blacklist: Optional[list] = None) -> bool:
    """
    True если текст явно "мусор" (ищу/вопрос/ремонт/запчасти и т.п.)
    """
    if not text:
        return True

    blacklist = blacklist or DEFAULT_BLACKLIST_WORDS
    t = normalize_text_for_rules(text)

    for w in blacklist:
        if w and w.lower() in t:
            return True
    return False


def parse_price_rub(text: str) -> Optional[int]:
    """
    Пытается вытащить цену и нормализовать в RUB (очень грубо, MVP).
    Если $/€ — оставляем как None (не уверены).
    """
    if not text:
        return None

    t = normalize_text_for_rules(text)
    m = PRICE_ANY_PATTERN.search(t)
    if not m:
        return None

    raw = m.group(2)
    unit = (m.group(3) or "").lower()

    try:
        val = int(raw.replace(" ", ""))
    except Exception:
        return None

    if unit in ["млн", "миллион", "m"]:
        val *= 1_000_000
    elif unit in ["тыс", "к", "k"]:
        val *= 1_000
    elif unit in ["$", "€"]:
        # MVP: не конвертим без курса
        return None

    return val


def parse_year(text: str) -> Optional[int]:
    if not text:
        return None
    t = normalize_text_for_rules(text)
    m = YEAR_PATTERN.search(t)
    if not m:
        return None
    try:
        y = int(m.group(1))
        return y
    except Exception:
        return None


def parse_mileage_km(text: str) -> Optional[int]:
    """
    Ищет пробег: 'до 120 тыс', '120 000 км'
    """
    if not text:
        return None

    t = normalize_text_for_rules(text)
    m = MILEAGE_PATTERN.search(t)
    if not m:
        return None

    raw = m.group(3)
    unit = (m.group(4) or "").lower()

    try:
        val = int(raw.replace(" ", ""))
    except Exception:
        return None

    if unit == "тыс":
        val *= 1000

    return val


def extract_quality_signals(text: str) -> Dict[str, Any]:
    """
    Вычисляет базовые сигналы качества (для explain / логов / отбора).
    """
    price = parse_price_rub(text)
    year = parse_year(text)
    mileage = parse_mileage_km(text)

    return {
        "price_rub": price,
        "year": year,
        "mileage_km": mileage,
        "has_price": price is not None,
        "has_year": year is not None,
        "has_mileage": mileage is not None,
    }


def passes_min_max_rules(
    *,
    text: str,
    min_text_len: int = DEFAULT_MIN_TEXT_LEN,
    min_price_rub: int = DEFAULT_MIN_PRICE_RUB,
    max_price_rub: int = DEFAULT_MAX_PRICE_RUB,
    min_year: int = DEFAULT_MIN_YEAR,
    max_mileage_km: int = DEFAULT_MAX_MILEAGE_KM,
) -> Tuple[bool, str]:
    """
    Возвращает (ok, reason)
    """
    if not text or len((text or "").strip()) < min_text_len:
        return False, "text_too_short"

    signals = extract_quality_signals(text)

    # price
    price = signals["price_rub"]
    if price is not None:
        if price < min_price_rub:
            return False, "price_too_low"
        if price > max_price_rub:
            return False, "price_too_high"

    # year
    year = signals["year"]
    if year is not None:
        if year < min_year:
            return False, "year_too_old"

    # mileage
    mileage = signals["mileage_km"]
    if mileage is not None:
        if mileage > max_mileage_km:
            return False, "mileage_too_high"

    return True, "ok"


def has_any_required_signals(text: str) -> bool:
    """
    True если в тексте есть хотя бы один из сигналов: цена / год / пробег.
    Нужен для правила:
    "если sale_intent=false и нет цены/года/пробега — skip"
    """
    signals = extract_quality_signals(text)
    return bool(signals.get("has_price") or signals.get("has_year") or signals.get("has_mileage"))


# =========================
# 🆕 STATS (IN-MEMORY)
# =========================

class SkipStats:
    """
    In-memory счётчик причин отсева.
    - VPS-safe (без внешних зависимостей)
    - Можно печатать прогресс каждые N документов или в конце источника
    """

    def __init__(self):
        self.total: int = 0
        self.kept: int = 0
        self.skipped: int = 0
        self.by_reason: Dict[str, int] = {}

    def add(self, *, skip: bool, reason: str):
        self.total += 1
        if skip:
            self.skipped += 1
            self.by_reason[reason] = self.by_reason.get(reason, 0) + 1
        else:
            self.kept += 1

    def snapshot(self) -> Dict[str, Any]:
        return {
            "total": self.total,
            "kept": self.kept,
            "skipped": self.skipped,
            "by_reason": dict(sorted(self.by_reason.items(), key=lambda x: x[1], reverse=True)),
        }

    def log(self, prefix: str = "[INGEST][ANTI_NOISE]"):
        snap = self.snapshot()
        print(
            f"{prefix} total={snap['total']} kept={snap['kept']} skipped={snap['skipped']} reasons={snap['by_reason']}"
        )


# =========================
# MAIN DECISION: SHOULD SKIP
# =========================

def should_skip_doc(
    *,
    text: str,
    source: str = "",
    min_sale_score: int = DEFAULT_MIN_SALE_SCORE,
    blacklist: Optional[list] = None,
) -> Tuple[bool, Dict[str, Any]]:
    """
    Главная функция Anti-noise.
    Возвращает:
      (skip, meta)
    meta содержит объяснение почему.

    ВАЖНО:
    - НЕ ДОЛЖНА бросать исключения наружу.
    - Должна быть быстрым фильтром до записи RawDocument и до индексации в Qdrant.
    """
    meta: Dict[str, Any] = {}

    try:
        if not text:
            meta["reason"] = "empty_text"
            return True, meta

        # нормализация только для правил (исходный text сохраняем как есть)
        norm = normalize_text_for_rules(text)
        meta["source"] = source or ""
        meta["text_len"] = len(norm)

        # 1) blacklist words
        if has_blacklist_words(norm, blacklist=blacklist):
            meta["reason"] = "blacklist_word"
            return True, meta

        # 2) min/max rules
        ok, reason = passes_min_max_rules(text=norm)
        if not ok:
            meta["reason"] = reason
            return True, meta

        # 3) sale intent
        sale = is_sale_intent(norm, min_score=min_sale_score)
        meta["sale_intent"] = sale

        # 4) правило: если sale_intent=false и нет price/year/mileage — skip
        #    (иначе иногда пролетают "привет всем" и прочий флуд)
        if not sale:
            if not has_any_required_signals(norm):
                meta["reason"] = "not_sale_and_no_signals"
                return True, meta

            # если сигналы есть (например цена/год/пробег), но sale_intent слабый
            # оставляем шанс (MVP), но пометим:
            meta["reason"] = "weak_sale_but_has_signals"
            return False, meta

        meta["reason"] = "ok"
        return False, meta

    except Exception as e:
        # fail-safe: лучше пропустить документ, чем поломать ingest
        meta["reason"] = "exception"
        meta["error"] = str(e)
        return True, meta


# =========================
# 🆕 META UTILITIES
# =========================

META_LINE_RE = re.compile(r"^__meta__:\s*(.*)$", re.IGNORECASE)


def extract_meta_from_text(text: str) -> Dict[str, Any]:
    """
    Если content начинается с "__meta__: k=v; ..." — распарсим.
    """
    if not text:
        return {}

    first_line = text.splitlines()[0].strip()
    m = META_LINE_RE.match(first_line)
    if not m:
        return {}

    body = m.group(1)
    parts = [p.strip() for p in body.split(";") if p.strip()]
    out: Dict[str, Any] = {}
    for p in parts:
        if "=" not in p:
            continue
        k, v = p.split("=", 1)
        out[k.strip()] = v.strip()
    return out


def apply_meta_prefix(text: str, meta_prefix: str) -> str:
    """
    Добавляет meta-prefix в начало текста, если его там ещё нет.
    """
    if not text:
        return text
    if text.lstrip().lower().startswith("__meta__:"):
        return text
    return f"{meta_prefix}\n{text}"


# =========================
# 🆕 ONE-SHOT: BUILD META + APPLY
# =========================

def enrich_text_with_meta(
    *,
    raw_text: str,
    source: str,
) -> Tuple[str, Dict[str, Any]]:
    """
    Утилита для ingest.py:

    1) detect_brand
    2) is_sale_intent
    3) resolve_source_boost
    4) build_meta_prefix
    5) apply_meta_prefix

    Возвращает:
      - content_with_meta (str)
      - meta (dict) расширенный (brand, conf, sale_intent, boost)
    """
    meta: Dict[str, Any] = {}

    brand, brand_conf = detect_brand(raw_text)
    sale = is_sale_intent(raw_text)
    boost = resolve_source_boost(source)

    meta["brand"] = brand
    meta["brand_confidence"] = float(brand_conf)
    meta["sale_intent"] = bool(sale)
    meta["source_boost"] = float(boost)

    meta_prefix = build_meta_prefix(
        brand=brand,
        brand_confidence=brand_conf,
        sale_intent=sale,
        source_boost=boost,
    )

    content = apply_meta_prefix(raw_text, meta_prefix)
    return content, meta
