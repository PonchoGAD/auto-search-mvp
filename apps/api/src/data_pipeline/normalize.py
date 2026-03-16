import re
from datetime import datetime
from typing import Optional, Dict, Tuple, Any
from collections import Counter

from db.session import SessionLocal, engine, Base
from db.models import RawDocument, NormalizedDocument

from services.ingest_quality import (
    should_skip_doc,
    is_sale_intent,
    resolve_source_boost,
    build_meta_prefix,
    apply_meta_prefix,
)

from services.taxonomy_service import taxonomy_service
# extract_car_entities intentionally removed from primary normalization flow


# =========================
# META PARSING
# =========================

META_PREFIX_RE = re.compile(
    r"^_meta_:\s*(.+?)(?:\n|$)",
    re.IGNORECASE,
)

# =====================================================
# PRODUCTION EXTRACTION REGEX
# =====================================================

RE_YEAR = re.compile(r"\b(19\d{2}|20\d{2})\b")

RE_PRICE = re.compile(
    r"(?<!\d)(\d[\d\s\u00A0]{3,12})\s*(₽|руб(?:\.|лей)?|р\b)(?!\d)",
    re.IGNORECASE,
)

RE_PRICE_TITLE_GLUE = re.compile(
    r"(\d[\d\s\u00A0]{3,12})(?:₽|руб|р)(?=[A-Za-zА-Яа-я])",
    re.IGNORECASE,
)

RE_MILEAGE = re.compile(
    r"(?<!\d)(\d[\d\s,\u00A0]{2,10})\s*(км|km|тыс\.?\s?км|т\.км)\b",
    re.IGNORECASE,
)

RE_MILEAGE_K = re.compile(
    r"(\d{1,3}(?:[.,]\d)?)\s*(тыс\.?|т\.км|k|тыс км)\b",
    re.IGNORECASE,
)

RE_FUEL = re.compile(
    r"\b("
    r"бензин|бензиновый|бенз|petrol|gasoline|mpi|fsi|tsi|tfsi|"
    r"дизель|дизельный|диз|diesel|tdi|dci|cdi|"
    r"гибрид|hybrid|phev|hev|"
    r"электро|электр|electric|ev|"
    r"газ/бензин|газ бензин|газ|lpg|gbo|cng"
    r")\b",
    re.IGNORECASE,
)

FUEL_MAP = {
    "бензин": "petrol",
    "бензиновый": "petrol",
    "бенз": "petrol",
    "petrol": "petrol",
    "gasoline": "petrol",
    "mpi": "petrol",
    "fsi": "petrol",
    "tsi": "petrol",
    "tfsi": "petrol",

    "дизель": "diesel",
    "дизельный": "diesel",
    "диз": "diesel",
    "diesel": "diesel",
    "tdi": "diesel",
    "dci": "diesel",
    "cdi": "diesel",

    "гибрид": "hybrid",
    "hybrid": "hybrid",
    "phev": "hybrid",
    "hev": "hybrid",

    "электро": "electric",
    "электр": "electric",
    "electric": "electric",
    "ev": "electric",

    "газ": "gas",
    "lpg": "gas",
    "gbo": "gas",
    "cng": "gas",

    "газ/бензин": "gas_petrol",
    "газ бензин": "gas_petrol",
}


# =========================
# BASIC NORMALIZATION HELPERS
# =========================

def _norm_text(text: str) -> str:
    return taxonomy_service.normalize_text(text)


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


def _is_speed_noise(text: str) -> bool:
    if re.search(r"\d+\s*(км/ч|km/h)", text or "", re.IGNORECASE):
        return True

    t = (text or "").lower()
    return any(x in t for x in [
        "км/ч",
        "km/h",
        "скорость",
        "средняя скорость",
    ])


def parse_meta(text: str) -> Tuple[Dict[str, str], str]:
    meta: Dict[str, str] = {}

    if not text:
        return meta, ""

    m = META_PREFIX_RE.match(text)
    if not m:
        return meta, text

    raw_meta = m.group(1)
    clean_text_val = text[m.end():]

    for part in raw_meta.split(";"):
        part = part.strip()
        if not part or "=" not in part:
            continue
        k, v = part.split("=", 1)
        meta[k.strip()] = v.strip()

    return meta, clean_text_val


# =========================
# TEXT HELPERS
# =========================

def clean_text(text: str):
    if not text:
        return ""

    text = text.replace("₽", " ₽ ")

    drom_garbage = [
        "Спецтехника",
        "Отзывы",
        "Каталог",
        "Шины",
        "Форумы",
        "ОСАГО",
        "ПДД",
        "Проверка по VIN",
    ]

    for g in drom_garbage:
        text = text.replace(g, "")

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
        matches = RE_YEAR.findall(source_text or "")
        if not matches:
            return None

        valid = []
        for y in matches:
            try:
                y_int = int(y)
                if _valid_year(y_int):
                    valid.append(y_int)
            except Exception:
                continue

        if not valid:
            return None

        title_year = valid[0]
        return title_year

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
        if _is_speed_noise(source_text):
            return None

        lowered = (source_text or "").lower()

        m = re.search(r"пробег[:\s]+(\d[\d\s\u00A0]{2,10})\b", lowered, re.IGNORECASE)
        if m:
            raw = _digits_only(m.group(1))
            try:
                val = int(raw)
                if _valid_mileage(val):
                    return val
            except Exception:
                pass

        m = RE_MILEAGE.search(source_text)
        if m:
            raw = _digits_only(m.group(1))
            unit = (m.group(2) or "").lower()
            try:
                val = int(raw)
                if "тыс" in unit or "т.км" in unit:
                    val *= 1000
                if _valid_mileage(val):
                    return val
            except Exception:
                pass

        m = RE_MILEAGE_K.search(lowered)
        if m:
            raw = m.group(1).replace(",", ".")
            try:
                val = int(float(raw) * 1000)
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
    fuel_matches = RE_FUEL.findall(lower)
    if fuel_matches:
        normalized = []
        for raw in fuel_matches:
            raw_fuel = str(raw).lower().strip()
            mapped = FUEL_MAP.get(raw_fuel)
            if mapped:
                normalized.append(mapped)

        if "gas_petrol" in normalized:
            fuel = "gas_petrol"
        elif normalized:
            fuel = normalized[0]

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
        "price": price if isinstance(price, int) and price > 0 else None,
        "currency": "RUB" if isinstance(price, int) and price > 0 else None,
        "fuel": fuel if isinstance(fuel, str) else None,
        "paint_condition": paint_condition,
    }


# =========================
# PIPELINE HELPERS
# =========================

def _safe_quality_score(
    skip: bool,
    sale_intent: bool,
    brand: Optional[str],
    model: Optional[str],
    fields: Dict[str, Any],
    source_boost: float,
) -> float:
    score = 0.0

    if not skip:
        score += 0.35
    if sale_intent:
        score += 0.20
    if brand:
        score += 0.20
    if model:
        score += 0.10
    if fields.get("price"):
        score += 0.05
    if fields.get("year"):
        score += 0.05
    if fields.get("mileage"):
        score += 0.05

    score += max(0.0, min(0.15, float(source_boost)))
    return round(min(score, 1.0), 4)


def _extract_canonical_entities(title_text: str, body_text: str) -> Tuple[Optional[str], Optional[str], float]:
    title_text = title_text or ""
    body_text = body_text or ""
    raw_text = f"{title_text}\n{body_text}".strip()

    title_brand, title_model, title_conf = taxonomy_service.resolve_entities(title_text)
    if title_brand:
        return (
            taxonomy_service.canonicalize_brand(title_brand),
            taxonomy_service.canonicalize_model(title_brand, title_model) if title_model else None,
            title_conf,
        )

    full_brand, full_model, full_conf = taxonomy_service.resolve_entities(raw_text)
    if full_brand:
        return (
            taxonomy_service.canonicalize_brand(full_brand),
            taxonomy_service.canonicalize_model(full_brand, full_model) if full_model else None,
            full_conf,
        )

    return None, None, 0.0


def _build_normalized_document_kwargs(
    raw: RawDocument,
    normalized_text: str,
    brand: Optional[str],
    model: Optional[str],
    fields: Dict[str, Any],
    sale_intent: bool,
    quality_score: float,
) -> Dict[str, Any]:
    kwargs: Dict[str, Any] = {
        "raw_id": raw.id,
        "source": raw.source,
        "source_url": raw.source_url,
        "title": raw.title,
        "normalized_text": normalized_text,
        "brand": brand,
        "model": model,
        "year": fields.get("year") if isinstance(fields.get("year"), int) else None,
        "mileage": fields.get("mileage") if isinstance(fields.get("mileage"), int) else None,
        "price": fields.get("price") if isinstance(fields.get("price"), int) and fields.get("price") > 0 else None,
        "currency": "RUB" if isinstance(fields.get("price"), int) and fields.get("price") > 0 else None,
        "fuel": fields.get("fuel") if isinstance(fields.get("fuel"), str) else None,
        "paint_condition": fields.get("paint_condition"),
    }

    model_columns = set()
    try:
        model_columns = {c.name for c in NormalizedDocument.__table__.columns}
    except Exception:
        model_columns = set()

    if "sale_intent" in model_columns:
        kwargs["sale_intent"] = sale_intent

    if "quality_score" in model_columns:
        kwargs["quality_score"] = quality_score

    return kwargs


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
        print("[NORMALIZE][WARN] no raw documents found", flush=True)
        session.close()
        return 0

    saved = 0
    skipped = 0

    counters = Counter()
    counters["extracted_brand_count"] = 0
    counters["extracted_model_count"] = 0
    counters["missing_brand_count"] = 0
    counters["missing_model_count"] = 0
    counters["ambiguous_brand_model_count"] = 0
    counters["conflict_count"] = 0

    try:
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

            if (raw.source or "").strip().lower() in {"dev_seed", "seed", "test", "debug"}:
                skipped += 1
                continue

            if raw.source_url and (
                "seed.local" in raw.source_url
                or "localhost" in raw.source_url
                or "example.com" in raw.source_url
            ):
                skipped += 1
                continue

            title_text = normalize_title_format((raw.title or "").strip())
            body_text = strip_drom_noise((raw.content or "").strip())
            raw_text = f"{title_text}\n{body_text}".strip()
            raw_text = raw_text.replace("₽", " ₽ ")

            # 1. quality gate
            skip, _skip_meta = should_skip_doc(
                text=raw_text,
                source=raw.source or "",
            )

            if skip:
                skipped += 1
                continue

            # 2. canonical extraction
            taxonomy_brand, taxonomy_model, brand_conf = _extract_canonical_entities(
                title_text=title_text,
                body_text=body_text,
            )

            final_brand = taxonomy_brand
            final_model = taxonomy_model

            if final_brand:
                final_brand = taxonomy_service.canonicalize_brand(final_brand)

            if final_brand and final_model:
                final_model = taxonomy_service.canonicalize_model(final_brand, final_model)

            # canonical fuel / numeric fields
            fields = extract_fields(raw_text)

            sale = is_sale_intent(raw_text)
            source_boost = resolve_source_boost(raw.source or "")
            quality_score = _safe_quality_score(
                skip=skip,
                sale_intent=sale,
                brand=final_brand,
                model=final_model,
                fields=fields,
                source_boost=source_boost,
            )

            # 3. normalized_text/meta prefix
            meta_prefix = build_meta_prefix(
                brand=final_brand,
                brand_confidence=brand_conf,
                sale_intent=sale,
                source_boost=source_boost,
            )

            enriched_content = apply_meta_prefix(raw_text, meta_prefix)
            _meta, content_wo_meta = parse_meta(enriched_content)
            normalized_text = clean_text(content_wo_meta)

            # 4. counters
            if final_brand:
                counters["extracted_brand_count"] += 1
            else:
                counters["missing_brand_count"] += 1

            if final_model:
                counters["extracted_model_count"] += 1
            else:
                counters["missing_model_count"] += 1

            if final_brand and not final_model:
                counters["ambiguous_brand_model_count"] += 1

            # 5. db save
            doc_kwargs = _build_normalized_document_kwargs(
                raw=raw,
                normalized_text=normalized_text,
                brand=final_brand,
                model=final_model,
                fields=fields,
                sale_intent=sale,
                quality_score=quality_score,
            )

            doc = NormalizedDocument(**doc_kwargs)
            session.add(doc)
            saved += 1

        session.commit()

    finally:
        try:
            session.close()
        except Exception:
            pass

    print(
        "[NORMALIZE] "
        f"docs_saved={saved} skipped={skipped} total={len(raws)} "
        f"extracted_brand_count={counters['extracted_brand_count']} "
        f"extracted_model_count={counters['extracted_model_count']} "
        f"missing_brand_count={counters['missing_brand_count']} "
        f"missing_model_count={counters['missing_model_count']} "
        f"ambiguous_brand_model_count={counters['ambiguous_brand_model_count']} "
        f"conflict_count={counters['conflict_count']}",
        flush=True,
    )
    return saved