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


CITY_BLACKLIST = {
    "moscow",
    "moskva",
    "москва",
    "khimki",
    "himki",
    "мытищи",
    "mytishchi",
    "ramenskoe",
    "korolev",
    "shchelkovo",
    "domodedovo",
    "krasnogorsk",
}


def extract_mileage(text: str) -> Optional[int]:
    text = (text or "").lower().replace("\u00A0", " ").replace("\xa0", " ")

    if _is_speed_noise(text):
        return None

    # 🔥 54.000 → 54000 фикс
    text = re.sub(r"(\d)\.(\d{3})", r"\1\2", text)

    patterns = [
        (r"\bпробег[^\d]{0,10}?(\d[\d\s]{2,10})\b", None),
        (r"\b(\d[\d\s]{2,10})\s*(км|km)\b", "km"),
        (r"\b(\d{1,3}(?:[.,]\d+)?)\s*(тыс\.?\s*км|тыс\.?|т\.км|ткм|k)\b", "thousand"),
        (r"\b(\d{4,6})\s?км\b", "km"),
    ]

    for pattern, multiplier in patterns:
        m = re.search(pattern, text)
        if not m:
            continue

        try:
            raw = re.sub(r"[^\d.]", "", m.group(1))
            value = float(raw)

            value *= multiplier

            value = int(value)

            if 1000 <= value <= 1_500_000:
                return value
        except:
            continue

    return None


def extract_fuel(text: str) -> Optional[str]:
    text = (text or "").lower()

    # 🔥 убираем точки и мусор
    text = text.replace(".", " ")

    patterns = {
        "electric": r"(электро|electric|ev)",
        "hybrid": r"(гибрид|hybrid|phev|hev)",
        "diesel": r"(дизель|diesel|tdi|cdi|dci)",
        "gas_petrol": r"(газ\s*/\s*бензин|бензин\s*/\s*газ)",
        "gas": r"(газ|lpg|cng)",
        "petrol": r"(бензин|petrol|gasoline|mpi|fsi|tsi|tfsi)",
    }

    for fuel, pattern in patterns.items():
        if re.search(pattern, text):
            return fuel

    return None


def extract_sale(text: str) -> str:
    lower = (text or "").lower()
    if any(x in lower for x in ["продам", "продаю", "продажа", "цена", "₽", "руб"]):
        return "1"
    return "0"


def _normalize_fuel_value(v: Optional[str]) -> Optional[str]:
    if not v:
        return None

    v = v.strip().lower()

    fuel_map = {
        "бензин": "petrol",
        "бензиновый": "petrol",
        "бенз": "petrol",
        "petrol": "petrol",
        "gasoline": "petrol",
        "mpi": "petrol",
        "tsi": "petrol",
        "tfsi": "petrol",
        "fsi": "petrol",
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

    return fuel_map.get(v, v if v in {"petrol", "diesel", "electric", "hybrid", "gas", "gas_petrol"} else None)


def _sanitize_mileage_value(v: Optional[int]) -> Optional[int]:
    if v is None:
        return None
    try:
        v = int(v)
    except Exception:
        return None

    if v < 10:
        return None
    if v > 1_500_000:
        return None
    return v


def _brand_is_explicit_in_text(brand: Optional[str], text: str) -> bool:
    if not brand:
        return False

    try:
        aliases = taxonomy_service.get_brand_aliases(brand) or []
    except Exception:
        aliases = []

    text_norm = taxonomy_service.normalize_text(text or "")
    for alias in aliases:
        alias_norm = taxonomy_service.normalize_text(alias or "")
        if not alias_norm:
            continue
        if re.search(rf"(?<![a-zа-яё0-9]){re.escape(alias_norm)}(?![a-zа-яё0-9])", text_norm, re.IGNORECASE):
            return True

    return False


def extract_from_url(url: str) -> Tuple[Optional[str], Optional[str]]:
    if not url:
        return None, None

    url = url.lower()

    # drom / avito pattern
    # /toyota/camry/
    m = re.search(r"/([a-z0-9\-]+)/([a-z0-9\-]+)/", url)
    if m:
        brand = m.group(1)
        model = m.group(2)

        # очистка
        model = model.replace("-", "").replace("_", "")

        return brand, model

    return None, None


META_PREFIX_RE = re.compile(
    r"^_meta_:\s*(.+?)(?:\n|$)",
    re.IGNORECASE,
)

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

MILEAGE_RE = re.compile(r"(\d{1,3})\s?(тыс|000)?\s?(км|km)", re.I)

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


SALE_PATTERNS = [
    "продаю",
    "продам",
    "продажа",
    "selling",
    "for sale",
]


def detect_sale_intent(text: str) -> int:
    t = (text or "").lower()
    for p in SALE_PATTERNS:
        if p in t:
            return 1
    return 0


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


def extract_fields(text: str) -> Dict[str, Optional[object]]:
    text = text or ""
    text = text.replace("\u00A0", " ").replace("\xa0", " ")
    lower = text.lower()

    mileage = None
    fuel = None

    # 🔥 HARD PARSE DROM STRUCTURED BLOCKS

    # 🔥 DROM HARD PARSE (УЛУЧШЕННЫЙ)

    # Пробег: 120 000 км
    m = re.search(r"пробег[^\d]{0,10}?([\d\s]{3,10})", lower)
    if m:
        try:
            val = int(re.sub(r"[^\d]", "", m.group(1)))
            if 0 <= val <= 1_500_000:
                mileage = val
        except:
            pass

    # fallback: 120000 км / 120 тыс км
    if not mileage:
        m = re.search(r"(\d{2,3})\s*(?:тыс|т\.км|ткм)", lower)
        if m:
            try:
                val = int(m.group(1)) * 1000
                if 0 <= val <= 1_500_000:
                    mileage = val
            except:
                pass

    # fallback: 120000 км
    if not mileage:
        m = re.search(r"(\d[\d\s]{3,10})\s*(км|km)", lower)
        if m:
            try:
                val = int(re.sub(r"[^\d]", "", m.group(1)))
                if 0 <= val <= 1_500_000:
                    mileage = val
            except:
                pass

    # fuel: "бензин", "дизель"
    m = re.search(r"\b(бензин|дизель|гибрид|электро|электр|газ|hybrid|diesel|petrol|electric|ev|гбо|lpg|phev)\b", lower)
    if m:
        matched_str = m.group(1).lower()
        if matched_str in ("электр", "ev"): matched_str = "электро"
        if matched_str == "гбо": matched_str = "газ"
        fuel = FUEL_MAP.get(matched_str, None)

    current_year = datetime.utcnow().year

    title_part = text[:180]

    def _valid_year(value: int) -> bool:
        return 1985 <= value <= current_year + 1

    def _valid_price(value: int) -> bool:
        return 10_000 <= value <= 200_000_000

    def _valid_mileage(value: int) -> bool:
        return 0 <= value <= 1_500_000

    def _extract_year(source_text: str) -> Optional[int]:
        matches = RE_YEAR.findall(source_text or "")
        if not matches:
            return None

        for y in matches:
            try:
                y_int = int(y)
                if _valid_year(y_int):
                    return y_int
            except Exception:
                continue

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

        nearby = source_text[max(0, m.start(1) - 20):m.end(1) + 20].lower()
        prefix = source_text[:m.start(1)].lower()

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

        m = re.search(r"\bпробег[:\s]+(\d[\d\s\u00A0]{2,10})\b", lowered, re.IGNORECASE)
        if m:
            try:
                val = int(_digits_only(m.group(1)))
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
            try:
                raw = m.group(1).replace(",", ".")
                val = int(float(raw) * 1000)
                if _valid_mileage(val):
                    return val
            except Exception:
                pass

        fallback = extract_mileage(source_text)
        if isinstance(fallback, int) and _valid_mileage(fallback):
            return fallback

        return None

    year = _extract_year(title_part)
    if year is None:
        year = _extract_year(text)

    price = _extract_price(title_part)
    if price is None:
        price = _extract_price(text)
    if price is None:
        price = _extract_title_fallback_price(title_part)

    if mileage is None:
        mileage = _extract_mileage(title_part)

    if mileage is None:
        mileage = _extract_mileage(text)

    if mileage is not None and mileage < 0:
        mileage = None

    # 🔥 НЕ ПЕРЕТИРАЕМ если уже нашли
    if not fuel:
        fuel_matches = RE_FUEL.findall(lower)
        if fuel_matches:
            normalized = []
            for raw_fuel in fuel_matches:
                value = str(raw_fuel).lower().strip()
                mapped = FUEL_MAP.get(value)
                if mapped:
                    normalized.append(mapped)

            if "gas_petrol" in normalized:
                fuel = "gas_petrol"
            elif normalized:
                fuel = normalized[0]

    if not fuel:
        fuel = extract_fuel(text)

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

            title_text = normalize_title_format((raw.title or "").strip())

            raw_body_text = (raw.content or "").strip()
            body_text = strip_drom_noise(raw_body_text)

            raw_text = f"{title_text}\n{body_text}".strip()

            fields = {}

            clean_pipeline_text = f"{title_text}\n{body_text}".strip()

            skip, _ = should_skip_doc(text=clean_pipeline_text or raw_text, source=raw.source or "")
            if skip:
                skipped += 1
                continue

            taxonomy_brand, taxonomy_model, brand_conf = _extract_canonical_entities(
                title_text=title_text,
                body_text=raw_body_text,
            )

            final_brand = taxonomy_brand
            final_model = taxonomy_model

            # 🔥 SOURCE_URL BOOST (главный фикс)
            url_brand, url_model = extract_from_url(raw.source_url)

            if url_brand:
                try:
                    url_brand = taxonomy_service.canonicalize_brand(url_brand)
                except Exception:
                    pass

            if url_brand and not final_brand:
                final_brand = url_brand

            if url_model and final_brand and not final_model:
                try:
                    final_model = taxonomy_service.canonicalize_model(final_brand, url_model)
                except Exception:
                    final_model = url_model

            entities = None
            if not final_brand or not final_model:
                from services.car_entity_extractor import extract_car_entities
                entities = extract_car_entities(title_text, raw_body_text)

                extracted_brand = entities.get("brand") if entities else None
                extracted_model = entities.get("model") if entities else None

                if not final_brand and extracted_brand and _brand_is_explicit_in_text(extracted_brand, raw_text):
                    final_brand = extracted_brand

                if final_brand and not final_model and extracted_model:
                    final_model = extracted_model

            if final_brand:
                final_brand = taxonomy_service.canonicalize_brand(final_brand)

            # 🔥 FIX — убираем города из бренда
            if final_brand in CITY_BLACKLIST:
                final_brand = None

            if final_brand and final_model:
                try:
                    final_model = taxonomy_service.canonicalize_model(final_brand, final_model)
                except Exception:
                    pass

            if final_brand and not _brand_is_explicit_in_text(final_brand, raw_text):
                if not _brand_is_explicit_in_text(final_brand, title_text):
                    final_brand = final_brand

            title_lower = (raw.title or "").lower()
            if not final_brand:
                brand_map = {
                    "bmw": "bmw",
                    "mercedes": "mercedes",
                    "benz": "mercedes",
                    "toyota": "toyota",
                    "kia": "kia",
                    "hyundai": "hyundai",
                    "lexus": "lexus",
                    "audi": "audi",
                    "ford": "ford",
                    "honda": "honda",
                    "nissan": "nissan",
                    "mazda": "mazda",
                    "lada": "lada",
                    "volvo": "volvo",
                    "land rover": "land_rover",
                    "range rover": "land_rover",
                }

                for k, v in brand_map.items():
                    if k in raw_text.lower():
                        final_brand = v

                        if not final_model:
                            m = re.search(rf"{k}\s+([a-z0-9\-]+)", raw_text.lower())
                            if m:
                                final_model = m.group(1)

                        break

            search_model = None
            if final_model:
                search_model = final_model.replace("_", "").replace("-", "")

            # 🔥 FIX — если модель = бренд → убираем
            if search_model == final_brand:
                search_model = None

            extracted_fields = extract_fields(raw_text)
            if extracted_fields:
                for k, v in extracted_fields.items():
                    if v is None:
                        continue

                    # 🔥 НЕ перезаписываем если уже есть
                    if fields.get(k) is None:
                        fields[k] = v

            # 🔥 HARD fallback — ищем везде
            if not fields.get("fuel"):
                fuel_fallback = (
                    extract_fuel(raw_text)
                    or extract_fuel(title_text)
                    or extract_fuel(raw_body_text)
                )
                if fuel_fallback:
                    fields["fuel"] = _normalize_fuel_value(fuel_fallback)

            if fields.get("fuel"):
                fields["fuel"] = _normalize_fuel_value(fields.get("fuel"))

            if fields.get("mileage") is not None:
                fields["mileage"] = _sanitize_mileage_value(fields.get("mileage"))

            # 🔥 HARD fallback mileage
            if not fields.get("mileage"):
                fallback_mileage = (
                    extract_mileage(raw_text)
                    or extract_mileage(title_text)
                    or extract_mileage(raw_body_text)
                )
                if fallback_mileage:
                    fields["mileage"] = _sanitize_mileage_value(fallback_mileage)

            if entities:
                if not fields.get("mileage") and entities.get("mileage") is not None:
                    try:
                        fields["mileage"] = _sanitize_mileage_value(int(entities.get("mileage")))
                    except Exception:
                        pass

                if not fields.get("fuel") and entities.get("fuel"):
                    fields["fuel"] = _normalize_fuel_value(entities.get("fuel"))

                if not fields.get("price") and entities.get("price") is not None:
                    try:
                        fields["price"] = int(entities.get("price"))
                        fields["currency"] = "RUB"
                    except Exception:
                        pass

                if not fields.get("year") and entities.get("year") is not None:
                    try:
                        fields["year"] = int(entities.get("year"))
                    except Exception:
                        pass

            sale = detect_sale_intent(raw_text)
            if sale == 0:
                sale = int(extract_sale(raw_text))

            source_boost = resolve_source_boost(raw.source or "")
            quality_score = _safe_quality_score(
                skip=skip,
                sale_intent=bool(sale),
                brand=final_brand,
                model=search_model,
                fields=fields,
                source_boost=source_boost,
            )

            meta_prefix = build_meta_prefix(
                brand=final_brand,
                brand_confidence=brand_conf,
                sale_intent=bool(sale),
                source_boost=source_boost,
            )

            enriched_content = apply_meta_prefix(clean_pipeline_text, meta_prefix)
            _meta, content_wo_meta = parse_meta(enriched_content)
            normalized_text = clean_text(content_wo_meta)

            print("[DEBUG NORMALIZE FULL]", {
                "TEXT_SAMPLE": raw_text[:200],
                "brand": final_brand,
                "model": search_model,
                "fuel": fields.get("fuel"),
                "mileage": fields.get("mileage"),
                "price": fields.get("price"),
            })

            # 🔥 FINAL HARD FALLBACK

            if not fields.get("mileage"):
                fields["mileage"] = extract_mileage(raw_text)

            if not fields.get("fuel"):
                fields["fuel"] = extract_fuel(raw_text)

            doc_kwargs = _build_normalized_document_kwargs(
                raw=raw,
                normalized_text=normalized_text,
                brand=final_brand,
                model=search_model,
                fields=fields,
                sale_intent=bool(sale),
                quality_score=quality_score,
            )

            doc = NormalizedDocument(**doc_kwargs)
            session.add(doc)
            saved += 1

        session.commit()

    finally:
        session.close()

    return saved
