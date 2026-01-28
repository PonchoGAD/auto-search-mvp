# apps/api/src/data_pipeline/normalize.py

import re
from typing import Optional, Dict, Tuple

from db.session import SessionLocal, engine
from db.models import Base, RawDocument, NormalizedDocument


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
# TEXT HELPERS
# =========================

def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def extract_brand_fallback(text: str) -> Optional[str]:
    """
    Минимальный fallback-детектор бренда.
    Используется только если brand не пришёл из meta.
    """
    brands = [
        "bmw",
        "audi",
        "mercedes",
        "toyota",
        "lexus",
        "volkswagen",
        "porsche",
        "skoda",
        "volvo",
        "ford",
        "tesla",
    ]

    lower = text.lower()
    for b in brands:
        if b in lower:
            return b.upper()
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
    - парсит __meta__
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

    for raw in raws:
        exists = (
            session.query(NormalizedDocument)
            .filter_by(source_url=raw.source_url)
            .first()
        )
        if exists:
            continue

        # =========================
        # META
        # =========================
        meta, content_wo_meta = parse_meta(raw.content or "")
        text = clean_text(content_wo_meta)

        # brand: meta → fallback
        brand = meta.get("brand")
        if brand:
            brand = brand.upper()
        else:
            brand = extract_brand_fallback(text)

        # ⚠️ дополнительные сигналы (пока остаются в meta)
        # meta.get("sale_intent")
        # meta.get("source_boost")

        # =========================
        # FIELDS
        # =========================
        fields = extract_fields(text)

        doc = NormalizedDocument(
            raw_id=raw.id,
            source=raw.source,
            source_url=raw.source_url,
            title=raw.title,
            normalized_text=text,
            brand=brand,
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

    print(f"[NORMALIZE] saved: {saved}")
