#  apps\api\src\data_pipeline\normalize.py

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

# =========================
# META PARSING
# =========================

# __meta__: brand=bmw; sale_intent=1; source_boost=1.5
META_PREFIX_RE = re.compile(
    r"^__meta__:\s*(.+?)(?:\n|$)",
    re.IGNORECASE,
)


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
    return re.sub(r"\s+", " ", text or "").strip()


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
    lower = text.lower()

    # год
    year = None
    m = re.search(r"\b(19\d{2}|20\d{2})\b", lower)
    if m:
        year = int(m.group(1))

    current_year = 2026

    if year and year < 1980:
        year = None

    if year and year > current_year + 1:
        year = None

    # пробег
    mileage = None
    m = re.search(r"(\d[\d\s]{1,8})\s*(км|тыс)\b", lower)
    if m:
        num = int(m.group(1).replace(" ", ""))
        mileage = num * 1000 if m.group(2) == "тыс" else num

    # цена
    price = None
    currency = None
    m = re.search(r"(\d[\d\s]{1,10})\s*(₽|руб|р)\b", lower)
    if m:
        price = int(m.group(1).replace(" ", ""))
        currency = "RUB"

    # sanity price
    if price and price < 10000:
        price = None

    if price and price > 200000000:
        price = None

    # топливо
    fuel = None
    if "бенз" in lower:
        fuel = "petrol"
    elif "диз" in lower:
        fuel = "diesel"
    elif "гибрид" in lower:
        fuel = "hybrid"
    elif "электро" in lower:
        fuel = "electric"

    # состояние окраса
    paint_condition = None
    if "без окрас" in lower or "не бит" in lower:
        paint_condition = "original"
    elif "крашен" in lower or "бит" in lower:
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

def run_normalize(limit: int = 500):
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
        if exists:
            continue

        raw_text = raw.content or ""

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

        # brand: meta → fallback
        brand = meta.get("brand")
        if brand and brand != "none":
            brand = brand.lower()
        else:
            brand = extract_brand_fallback(text)

        # =====================================================
        # FIELD EXTRACTION
        # =====================================================
        fields = extract_fields(text)

        model = extract_model(text, brand)

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