import re
from typing import Optional, Dict

from db.session import SessionLocal, engine
from db.models import Base, RawDocument, NormalizedDocument


def clean_text(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()


def extract_brand(text: str) -> Optional[str]:
    brands = ["bmw", "audi", "mercedes", "toyota", "lexus", "volkswagen"]
    lower = text.lower()
    for b in brands:
        if b in lower:
            return b.upper()
    return None


def extract_fields(text: str) -> Dict[str, Optional[object]]:
    lower = text.lower()

    year = None
    m = re.search(r"\b(19\d{2}|20\d{2})\b", lower)
    if m:
        year = int(m.group(1))

    mileage = None
    m = re.search(r"(\d[\d\s]{1,8})\s*(км|тыс)\b", lower)
    if m:
        num = int(m.group(1).replace(" ", ""))
        mileage = num * 1000 if m.group(2) == "тыс" else num

    price = None
    currency = None
    m = re.search(r"(\d[\d\s]{1,10})\s*(₽|руб|р)\b", lower)
    if m:
        price = int(m.group(1).replace(" ", ""))
        currency = "RUB"

    fuel = None
    if "бенз" in lower:
        fuel = "petrol"
    elif "диз" in lower:
        fuel = "diesel"
    elif "гибрид" in lower:
        fuel = "hybrid"
    elif "электро" in lower:
        fuel = "electric"

    paint_condition = None
    if "без окрас" in lower or "не бит" in lower:
        paint_condition = "original"
    elif "крашен" in lower or "бит" in lower:
        paint_condition = "repainted"

    return {
        "brand": extract_brand(text),
        "year": year,
        "mileage": mileage,
        "price": price,
        "currency": currency,
        "fuel": fuel,
        "paint_condition": paint_condition,
    }


def run_normalize(limit: int = 500):
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

        text = clean_text(raw.content)
        fields = extract_fields(text)

        doc = NormalizedDocument(
            raw_id=raw.id,
            source=raw.source,
            source_url=raw.source_url,
            title=raw.title,
            normalized_text=text,
            brand=fields["brand"],
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
