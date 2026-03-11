import re
from datetime import datetime
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

from services.car_entity_extractor import extract_car_entities


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

    "газ": "gas",
    "lpg": "gas",
    "gbo": "gas",
    "газ/бензин": "gas_petrol",
    "газ бензин": "gas_petrol",
}


def normalize_title_format(text: str) -> str:
    if not text:
        return ""

    text = text.replace("\u00A0", " ")
    text = text.replace("\xa0", " ")
    text = text.replace("\t", " ")
    text = text.replace("\r", " ")
    text = text.replace("\n", " ")
    text = text.replace("₽", " ₽ ")

    text = re.sub(r"(₽)([A-Za-zА-Яа-я])", r"\1 \2", text)
    text = re.sub(r",(\d{4})", r", \1", text)
    text = re.sub(r"(\d{4})([А-ЯA-ZА-Яа-я])", r"\1 \2", text)
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

def clean_text(text: str):

    if not text:
        return ""

    text = text.replace("₽", " ₽ ")

    text = re.sub(r"\s+", " ", text)

    return text.strip()


def strip_drom_noise(text: str):
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

        for v in cfg.get("en", []):
            if v.lower() in lower:
                return brand_key

        for v in cfg.get("ru", []):
            if v.lower() in lower:
                return brand_key

        for v in cfg.get("aliases", []):
            if v.lower() in lower:
                return brand_key

    return None


# =========================
# FIELD EXTRACTION
# =========================

def extract_fields(text: str) -> Dict[str, Optional[object]]:

    text = text or ""
    text = text.replace("\u00A0", " ").replace("\xa0", " ")
    lower = text.lower()
    current_year = datetime.utcnow().year

    title_part = text[:140]

    def _valid_year(value: int) -> bool:
        return 1985 <= value <= current_year + 1

    def _valid_price(value: int) -> bool:
        return 10_000 <= value <= 200_000_000

    def _valid_mileage(value: int) -> bool:
        return 0 <= value <= 500_000

    def _extract_year(source_text: str) -> Optional[int]:
        for y in RE_YEAR.findall(source_text or ""):
            try:
                y_int = int(y)
                if _valid_year(y_int):
                    return y_int
            except Exception:
                pass
        return None

    def _extract_price(source_text: str) -> Optional[int]:
        m = RE_PRICE.search(source_text)
        if m:
            raw = _digits_only(m.group(1))
            try:
                val = int(raw)
                if _valid_price(val):
                    return val
            except Exception:
                pass

        m = RE_PRICE_TITLE_GLUE.search(source_text)
        if m:
            raw = _digits_only(m.group(1))
            try:
                val = int(raw)
                if _valid_price(val):
                    return val
            except Exception:
                pass

        return None

    def _extract_title_fallback_price(source_text: str) -> Optional[int]:
        m = re.search(r"^\D{0,15}(\d[\d\s\u00A0]{4,12})", source_text)
        if not m:
            return None

        raw = _digits_only(m.group(1))
        if not raw:
            return None

        try:
            val = int(raw)
        except Exception:
            return None

        if _valid_year(val):
            return None

        if val < 100_000:
            return None

        if _valid_mileage(val):
            prefix = source_text[:m.start(1)].lower()
            nearby = source_text[max(0, m.start(1) - 20):m.end(1) + 20].lower()
            if (
                "км" in nearby
                or "km" in nearby
                or "тыс" in nearby
                or "т.км" in nearby
                or "пробег" in prefix
            ):
                return None

        if _valid_price(val):
            return val

        return None

    def _extract_mileage(source_text: str) -> Optional[int]:
        m = RE_MILEAGE.search(source_text)
        if m:
            raw = _digits_only(m.group(1))
            try:
                val = int(raw)
                if _valid_mileage(val):
                    return val
            except Exception:
                pass

        m = RE_MILEAGE_K.search(source_text.lower())
        if m:
            raw = m.group(1).replace(",", ".")
            try:
                val = int(float(raw) * 1000)
                if _valid_mileage(val):
                    return val
            except Exception:
                pass

        m = re.search(r"пробег[:\s]+(\d[\d\s\u00A0]{2,10})\b", source_text.lower(), re.IGNORECASE)
        if m:
            raw = _digits_only(m.group(1))
            try:
                val = int(raw)
                if _valid_mileage(val):
                    return val
            except Exception:
                pass

        return None

    year = _extract_year(title_part)
    if year is None:
        year = _extract_year(text)

    price = _extract_price(title_part)
    if price is None:
        price = _extract_price(text)
    if price is None:
        price = _extract_title_fallback_price(title_part)

    mileage = _extract_mileage(title_part)
    if mileage is None:
        mileage = _extract_mileage(text)

    fuel = None
    m = RE_FUEL.search(lower)
    if m:
        raw = m.group(1).lower()
        normalized_fuel = FUEL_MAP.get(raw)
        if normalized_fuel in {"petrol", "diesel", "hybrid", "electric", "gas", "gas_petrol"}:
            fuel = normalized_fuel

    paint_condition = None

    if (
        "без окраса" in lower
        or "без окрасов" in lower
        or "без окрас" in lower
        or "не бит" in lower
        or "не крашен" in lower
        or "не крашена" in lower
    ):
        paint_condition = "original"

    elif (
        "крашен" in lower
        or "крашена" in lower
        or "окрас" in lower
        or "бит" in lower
    ):
        paint_condition = "repainted"

    return {
        "year": year if isinstance(year, int) else None,
        "mileage": mileage if isinstance(mileage, int) else None,
        "price": price if isinstance(price, int) else None,
        "currency": "RUB" if isinstance(price, int) else None,
        "fuel": fuel if isinstance(fuel, str) else None,
        "paint_condition": paint_condition,
    }


# =========================
# MAIN NORMALIZE
# =========================

def run_normalize(limit: int = 500, force_rebuild: bool = False):

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

        skip, skip_meta = should_skip_doc(
            text=raw_text,
            source=raw.source or "",
        )

        if skip:
            skipped += 1
            continue

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

        meta, content_wo_meta = parse_meta(enriched_content)
        text = clean_text(content_wo_meta)
        full_text = f"{title_text}\n{text}".strip()

        title = title_text
        content = full_text

        entities = extract_car_entities(
            title or "",
            f"{title or ''} {content or ''}"
        )

        brand = entities.get("brand")
        model = entities.get("model")
        price = entities.get("price")
        mileage = entities.get("mileage")
        fuel = entities.get("fuel")
        year = entities.get("year")

        if not brand:
            brand = brand_key

        if not brand:
            brand = extract_brand_fallback(title_text)

        if not brand:
            brand = extract_brand_fallback(full_text)

        if brand:
            brand = brand.lower().strip()
        else:
            brand = "unknown"

        fields = extract_fields(full_text)

        brand = entities.get("brand") or brand
        model = entities.get("model") or model
        price = entities.get("price") or price
        year = entities.get("year") or year
        mileage = entities.get("mileage") or mileage
        fuel = entities.get("fuel") or fuel

        doc = NormalizedDocument(
            raw_id=raw.id,
            source=raw.source,
            source_url=raw.source_url,
            title=raw.title,
            normalized_text=text,
            brand=brand,
            model=model,
            year=year if isinstance(year, int) else fields["year"],
            mileage=mileage if isinstance(mileage, int) else fields["mileage"],
            price=price if isinstance(price, int) else fields["price"],
            currency="RUB" if isinstance(price, int) else fields["currency"],
            fuel=fuel if isinstance(fuel, str) else fields["fuel"],
            paint_condition=fields["paint_condition"],
        )

        session.add(doc)
        saved += 1

    session.commit()
    session.close()

    print(f"[NORMALIZE] docs_saved={saved} skipped={skipped} total={len(raws)}")