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

from services.model_resolver import resolve_model


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

    lower = text.lower()

    # =========================
    # YEAR
    # =========================

    year = None

    m = re.search(r"\b(19\d{2}|20\d{2})\b", lower)

    if m:
        try:
            year = int(m.group(1))
        except:
            year = None

    current_year = 2026

    if year and year < 1980:
        year = None

    if year and year > current_year + 1:
        year = None

    # =========================
    # MILEAGE
    # =========================

    mileage = None

    m = re.search(r"(\d[\d\s,]{3,7})\s*(км|km)", lower)

    if m:

        raw = m.group(1)

        raw = raw.replace(",", "")
        raw = raw.replace(" ", "")

        try:
            mileage = int(raw)
        except:
            mileage = None

    # тыс км
    m = re.search(r"(\d{1,3})\s*тыс\s*(км)?", lower)

    if m and not mileage:
        try:
            mileage = int(m.group(1)) * 1000
        except:
            mileage = None

    # =========================
    # PRICE
    # =========================

    price = None
    currency = None

    m = re.search(r"(\d[\d\s\u00A0]{3,})\s*(₽|руб|р)", lower)

    if m:

        raw = m.group(1)

        raw = raw.replace(" ", "")
        raw = raw.replace("\u00A0", "")

        try:
            price = int(raw)
            currency = "RUB"
        except:
            price = None
            currency = None

    # title fallback
    if not price:

        title_part = text[:120]

        m = re.search(r"(\d[\d\s]{3,})\s*(₽|руб|р)", title_part)

        if m:

            raw = re.sub(r"\D", "", m.group(1))

            try:
                price = int(raw)
                currency = "RUB"
            except:
                pass

    # sanity
    if price and price < 10000:
        price = None

    if price and price > 200000000:
        price = None

    # =========================
    # FUEL
    # =========================

    fuel = None

    if "дизел" in lower:
        fuel = "diesel"

    elif "бенз" in lower:
        fuel = "petrol"

    elif "гибрид" in lower:
        fuel = "hybrid"

    elif "электр" in lower:
        fuel = "electric"

    # =========================
    # PAINT
    # =========================

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

        title_text = (raw.title or "").strip()

        # normalize title spacing
        title_text = title_text.replace("₽", " ₽ ")
        title_text = re.sub(r"\s+", " ", title_text)

        body_text = strip_drom_noise((raw.content or "").strip())

        raw_text = f"{title_text}\n{body_text}".strip()

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

        if brand:
            brand = brand.lower()

        if not brand:
            brand = "unknown"

        # =====================================================
        # FIELD EXTRACTION
        # =====================================================

        parse_text = f"{title_text} {text}"

        fields = extract_fields(parse_text)

        model = resolve_model(brand, title_text + " " + text)

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