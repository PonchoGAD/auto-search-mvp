import re
from typing import Optional, Dict, Tuple
from pathlib import Path
import yaml

from db.session import SessionLocal, engine
from db.models import Base, RawDocument, NormalizedDocument

# 🆕 Anti-noise / ingest quality
from services.ingest_quality import (
    should_skip_doc,
    detect_brand,
    is_sale_intent,
    resolve_source_boost,
    build_meta_prefix,
    apply_meta_prefix,
)

from services.model_resolver import resolve_model


# =========================
# META PARSING
# =========================

# __meta__: brand=bmw; sale_intent=1; source_boost=1.5
META_PREFIX_RE = re.compile(
    r"^__meta__:\s*(.+?)(?:\n|$)",
    re.IGNORECASE,
)

# =====================================================
# PRODUCTION EXTRACTION REGEX
# =====================================================

RE_YEAR = re.compile(r"\b(19\d{2}|20\d{2})\b")

RE_PRICE = re.compile(
    r"(\d[\d\s\u00A0]{3,12})\s*(₽|руб(?:\.|лей)?|р(?:\.|уб)?\b)",
    re.IGNORECASE,
)

RE_PRICE_TITLE_GLUE = re.compile(
    r"(\d[\d\s\u00A0]{3,12})(?:₽|руб(?:\.|лей)?|р(?:\.|уб)?)(?=[A-Za-zА-Яа-я])",
    re.IGNORECASE,
)

RE_MILEAGE = re.compile(
    r"(\d[\d\s,\u00A0]{2,10})\s*(км|km)\b",
    re.IGNORECASE,
)

RE_MILEAGE_K = re.compile(
    r"(\d{1,3}(?:[.,]\d)?)\s*(тыс\.?|т\.км|k)\b",
    re.IGNORECASE,
)

RE_FUEL = re.compile(
    r"\b("
    r"бензин|бенз|petrol|gasoline|"
    r"дизель|диз|diesel|"
    r"гибрид|hybrid|"
    r"электро|электр|electric|ev"
    r")\b",
    re.IGNORECASE,
)

FUEL_MAP = {
    "бензин": "petrol",
    "бенз": "petrol",
    "petrol": "petrol",
    "gasoline": "petrol",

    "дизель": "diesel",
    "диз": "diesel",
    "diesel": "diesel",

    "гибрид": "hybrid",
    "hybrid": "hybrid",

    "электро": "electric",
    "электр": "electric",
    "electric": "electric",
    "ev": "electric",
}


def normalize_title_format(text: str) -> str:

    if not text:
        return ""

    text = text.replace("\u00A0", " ")
    text = text.replace("₽", " ₽ ")

    # 2012Москва -> 2012 Москва
    text = re.sub(r"(\d{4})([А-ЯA-Z])", r"\1 \2", text)

    # 799000₽Insignia -> 799000 ₽ Insignia
    text = re.sub(r"(₽)([A-Za-zА-Яа-я])", r"\1 \2", text)

    # Camry,2019 -> Camry, 2019
    text = re.sub(r",(\d{4})", r", \1", text)

    # Q30,2019Москва -> Q30, 2019 Москва
    text = re.sub(r"(\d{4})([А-ЯA-Z])", r"\1 \2", text)

    text = re.sub(r"\s+", " ", text)

    return text.strip()


def _digits_only(value: str) -> str:
    if not value:
        return ""
    return re.sub(r"[^\d]", "", value)


def parse_meta(text: str) -> Tuple[Dict[str, str], str]:
    """
    Извлекает meta-префикс из content и возвращает:
    - meta dict
    - очищенный текст (без meta)
    """
    meta: Dict[str, str] = {}

    if not text:
        return meta, ""

    m = META_PREFIX_RE.match(text)
    if not m:
        return meta, text

    raw_meta = m.group(1)
    clean_text = text[m.end():]

    for part in raw_meta.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        meta[k.strip()] = v.strip()

    return meta, clean_text


# =========================
# BRANDS CONFIG
# =========================

def load_brands():
    try:
        base_dir = Path(__file__).resolve().parent.parent
        brands_path = base_dir / "config" / "brands.yaml"

        with open(brands_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}

        return data.get("brands", {})

    except Exception as e:
        print(f"[NORMALIZE][WARN] brands.yaml load failed: {e}")
        return {}


BRANDS_CONFIG = load_brands()


# =========================
# TEXT HELPERS
# =========================

def clean_text(text: str) -> str:

    if not text:
        return ""

    text = text.replace("₽", " ₽ ")

    text = re.sub(r"\s+", " ", text)

    return text.strip()


def strip_drom_noise(text: str) -> str:
    if not text:
        return ""

    cut_markers = [
        "Отзывы владельцев",
        "Мнения владельцев",
        "Вы смотрите раздел",
        "В разделе \"Продажа авто\"",
        "Технические характеристики",
        "Запчасти на",
        "Статистика цен",
        "О проекте Помощь Правила Для СМИ",
    ]

    min_keep_len = 300
    cleaned = text

    for marker in cut_markers:
        idx = cleaned.find(marker)
        if idx != -1 and idx >= min_keep_len:
            cleaned = cleaned[:idx].strip()
            break

    return cleaned


def extract_brand_fallback(text: str) -> Optional[str]:
    """
    Fallback brand detection using brands.yaml
    """

    if not text:
        return None

    lower = text.lower()

    for brand_key, cfg in BRANDS_CONFIG.items():

        # en
        for v in cfg.get("en", []):
            if v.lower() in lower:
                return brand_key

        # ru
        for v in cfg.get("ru", []):
            if v.lower() in lower:
                return brand_key

        # aliases
        for v in cfg.get("aliases", []):
            if v.lower() in lower:
                return brand_key

    return None


def extract_model(text: str, brand: Optional[str]) -> Optional[str]:
    """
    Простое извлечение модели рядом с брендом
    """

    if not brand:
        return None

    pattern = rf"\b{brand}\b\s+([a-z0-9\-]+)"

    m = re.search(pattern, text.lower())
    if m:
        return m.group(1)

    return None


# =========================
# FIELD EXTRACTION
# =========================

def extract_fields(text: str) -> Dict[str, Optional[object]]:

    text = text or ""
    lower = text.lower()

    # =====================================================
    # YEAR
    # =====================================================

    year = None

    for y in RE_YEAR.findall(text):
        try:
            y_int = int(y)
            if 1985 <= y_int <= 2026:
                year = y_int
                break
        except Exception:
            pass

    # =====================================================
    # PRICE
    # =====================================================

    price = None
    currency = None

    m = RE_PRICE.search(text)

    if m:
        raw = _digits_only(m.group(1))
        try:
            val = int(raw)
            if 10_000 <= val <= 200_000_000:
                price = val
                currency = "RUB"
        except Exception:
            pass

    if price is None:
        title_part = text[:140]
        m = RE_PRICE_TITLE_GLUE.search(title_part)
        if m:
            raw = _digits_only(m.group(1))
            try:
                val = int(raw)
                if 10_000 <= val <= 200_000_000:
                    price = val
                    currency = "RUB"
            except Exception:
                pass

    # fallback только для title, чтобы не ловить случайные годы / пробеги из body
    if price is None:
        title_part = text[:140]
        m = re.search(r"^\D{0,15}(\d[\d\s\u00A0]{4,12})", title_part)
        if m:
            raw = _digits_only(m.group(1))
            try:
                val = int(raw)
                if 100_000 <= val <= 200_000_000:
                    price = val
                    currency = "RUB"
            except Exception:
                pass

    # =====================================================
    # MILEAGE
    # =====================================================

    mileage = None

    m = RE_MILEAGE.search(text)
    if m:
        raw = _digits_only(m.group(1))
        try:
            val = int(raw)
            if 0 <= val <= 500_000:
                mileage = val
        except Exception:
            pass

    if mileage is None:
        m = RE_MILEAGE_K.search(lower)
        if m:
            raw = m.group(1).replace(",", ".")
            try:
                val = float(raw) * 1000
                val = int(val)
                if 0 <= val <= 500_000:
                    mileage = val
            except Exception:
                pass

    # дополнительный fallback: "пробег 120000"
    if mileage is None:
        m = re.search(r"пробег[:\s]+(\d[\d\s\u00A0]{2,10})\b", lower, re.IGNORECASE)
        if m:
            raw = _digits_only(m.group(1))
            try:
                val = int(raw)
                if 0 <= val <= 500_000:
                    mileage = val
            except Exception:
                pass

    # =====================================================
    # FUEL
    # =====================================================

    fuel = None

    m = RE_FUEL.search(lower)
    if m:
        raw = m.group(1).lower()
        fuel = FUEL_MAP.get(raw)

    # =====================================================
    # PAINT CONDITION
    # =====================================================

    paint_condition = None

    if (
        "без окрас" in lower
        or "без окраса" in lower
        or "не бит" in lower
        or "не крашен" in lower
        or "не крашена" in lower
    ):
        paint_condition = "original"

    elif (
        "крашен" in lower
        or "крашена" in lower
        or "бит" in lower
        or "окрас" in lower
    ):
        paint_condition = "repainted"

    return {
        "year": year,
        "mileage": mileage,
        "price": price,
        "currency": currency,
        "fuel": fuel,
        "paint_condition": paint_condition,
    }


# =========================
# MAIN NORMALIZE
# =========================

def run_normalize(limit: int = 500, force_rebuild: bool = False):
    """
    Normalize pipeline:
    - Anti-noise (skip мусор)
    - build meta (__meta__)
    - парсит meta
    - очищает текст
    - вытаскивает поля
    - подготавливает данные для ranking
    """

    Base.metadata.create_all(bind=engine)
    session = SessionLocal()

    if force_rebuild:
        print("[NORMALIZE] force_rebuild=True → clearing normalized docs", flush=True)

        session.query(NormalizedDocument).delete()
        session.commit()

    raws = (
        session.query(RawDocument)
        .order_by(RawDocument.id.desc())
        .limit(limit)
        .all()
    )

    if not raws:
        print("[NORMALIZE][WARN] no raw documents found")
        session.close()
        return

    saved = 0
    skipped = 0

    for raw in raws:
        exists = (
            session.query(NormalizedDocument)
            .filter_by(source_url=raw.source_url)
            .first()
        )

        if exists and not force_rebuild:
            continue

        if exists and force_rebuild:
            session.delete(exists)
            session.flush()

        title_text = normalize_title_format((raw.title or "").strip())

        body_text = strip_drom_noise((raw.content or "").strip())

        raw_text = f"{title_text}\n{body_text}".strip()

        raw_text = raw_text.replace("₽", " ₽ ")

        # =====================================================
        # 🧹 ANTI-NOISE (до индексации)
        # =====================================================
        skip, skip_meta = should_skip_doc(
            text=raw_text,
            source=raw.source or "",
        )

        if skip:
            skipped += 1
            continue

        # =====================================================
        # 🧠 META ENRICHMENT (до normalize)
        # =====================================================

        brand_key, brand_conf = detect_brand(title_text)

        if not brand_key:
            brand_key, brand_conf = detect_brand(raw_text)

        sale = is_sale_intent(raw_text)
        source_boost = resolve_source_boost(raw.source or "")

        meta_prefix = build_meta_prefix(
            brand=brand_key,
            brand_confidence=brand_conf,
            sale_intent=sale,
            source_boost=source_boost,
        )

        enriched_content = apply_meta_prefix(raw_text, meta_prefix)

        # =====================================================
        # META PARSE
        # =====================================================
        meta, content_wo_meta = parse_meta(enriched_content)
        text = clean_text(content_wo_meta)

        brand = brand_key

        if not brand:
            brand = extract_brand_fallback(title_text)

        if not brand:
            brand = extract_brand_fallback(text)

        if not brand:
            brand = extract_brand_fallback(parse_meta(enriched_content)[1])

        if brand:
            brand = brand.lower().strip()

        if not brand:
            brand = "unknown"

        # =====================================================
        # FIELD EXTRACTION
        # =====================================================

        parse_text = f"{title_text}\n{text}"

        fields = extract_fields(parse_text)

        parse_text = f"{title_text}\n{text}"

        model = resolve_model(brand, title_text)

        if not model:
            model = resolve_model(brand, parse_text)

        doc = NormalizedDocument(
            raw_id=raw.id,
            source=raw.source,
            source_url=raw.source_url,
            title=raw.title,
            normalized_text=text,
            brand=brand,
            model=model,
            year=fields["year"],
            mileage=fields["mileage"],
            price=fields["price"],
            currency=fields["currency"],
            fuel=fields["fuel"],
            paint_condition=fields["paint_condition"],
        )

        session.add(doc)
        saved += 1

    session.commit()
    session.close()

    print(f"[NORMALIZE] docs_saved={saved} skipped={skipped} total={len(raws)}")