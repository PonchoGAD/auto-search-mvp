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

META_PREFIX_RE = re.compile(
    r"^__meta__:\s*(.+?)(?:\n|$)",
    re.IGNORECASE,
)

# =====================================================
# PRODUCTION EXTRACTION REGEX
# =====================================================

RE_YEAR = re.compile(r"\b(19\d{2}|20\d{2})\b")

RE_PRICE = re.compile(
    r"(\d[\d\s\u00A0]{3,12})\s*(₽|руб|р)\b",
    re.IGNORECASE,
)

RE_PRICE_TITLE_GLUE = re.compile(
    r"(\d[\d\s\u00A0]{3,12})(?:₽|руб|р)(?=[A-Za-zА-Яа-я])",
    re.IGNORECASE,
)

RE_MILEAGE = re.compile(
    r"(\d[\d\s,\u00A0]{2,10})\s*(км|km|тыс\.? ?км|т\.км)\b",
    re.IGNORECASE,
)

RE_MILEAGE_K = re.compile(
    r"(\d{1,3}(?:[.,]\d)?)\s*(тыс\.?|т\.км|k|тыс км)\b",
    re.IGNORECASE,
)

RE_FUEL = re.compile(
    r"\b("
    r"бензин|бензиновый|бенз|petrol|gasoline|"
    r"дизель|дизельный|диз|diesel|tdi|dci|"
    r"гибрид|hybrid|"
    r"электро|электр|electric|ev|"
    r"газ|lpg|gbo"
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


def _is_speed_noise(text: str) -> bool:
    if re.search(r"\d+\s*(км/ч|km/h)", text or ""):
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

    # DROM garbage cleanup
    DROM_GARBAGE = [
        "Спецтехника",
        "Отзывы",
        "Каталог",
        "Шины",
        "Форумы",
        "ОСАГО",
        "ПДД",
        "Проверка по VIN",
    ]

    for g in DROM_GARBAGE:
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


def extract_brand_fallback(text: str) -> Optional[str]:

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

        if _is_speed_noise(source_text):
            return None

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
        "price": price if isinstance(price, int) and price > 0 else None,
        "currency": "RUB" if isinstance(price, int) and price > 0 else None,
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

    def _is_telegram_noise(raw_text: str) -> bool:
        t = (raw_text or "").lower()

        hard_noise = [
            "масло",
            "редуктор",
            "допуск",
            "диски",
            "резина",
            "шины",
            "колеса",
            "запчаст",
            "разбор",
            "км/ч",
            "km/h",
            "скорость",
            "средняя скорость",
        ]

        if any(x in t for x in hard_noise):
            return True

        discussion_words = [
            "это норм",
            "это цена",
            "шутка",
            "реальная цена",
            "подскажите",
            "кто знает",
            "?",
        ]

        has_price = bool(re.search(r"\d[\d\s]{3,}\s*(₽|руб|р)", t, re.IGNORECASE))
        brand_tmp, _ = detect_brand(raw_text)
        model_tmp = resolve_model(brand_tmp, raw_text) if brand_tmp else None

        if has_price and not brand_tmp and not model_tmp:
            if any(x in t for x in discussion_words):
                return True

        return False

    def _count_sale_signals(raw_text: str, brand: str | None, model: str | None, fields: dict, sale: bool) -> int:
        return sum([
            1 if brand else 0,
            1 if model else 0,
            1 if fields.get("price") else 0,
            1 if fields.get("year") else 0,
            1 if fields.get("mileage") else 0,
            1 if sale else 0,
        ])

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

        brand_key, brand_conf = detect_brand(title_text)
        if not brand_key:
            brand_key, brand_conf = detect_brand(raw_text)

        fields_preview = extract_fields(raw_text)
        sale = is_sale_intent(raw_text)

        brand_preview = brand_key
        model_preview = resolve_model(brand_preview, raw_text) if brand_preview else None
        signals_count = _count_sale_signals(
            raw_text=raw_text,
            brand=brand_preview,
            model=model_preview,
            fields=fields_preview,
            sale=sale,
        )

        if raw.source == "telegram":
            if _is_telegram_noise(raw_text):
                skipped += 1
                continue

            if signals_count < 1:
                skipped += 1
                continue
        else:
            if skip and signals_count == 0:
                skipped += 1
                continue

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
        content = full_text[:800]

        brand_key, brand_conf = detect_brand(title_text)
        if not brand_key:
            brand_key, brand_conf = detect_brand(full_text)

        brand = brand_key

        if not brand:
            brand = extract_brand_fallback(title_text)

          model = resolve_model(
            brand,
            f"{title_text} {full_text}"
)

        if not brand:
            brand = extract_brand_fallback(full_text)

        if not brand and model:
            from services.brand_detector import MODEL_BRAND_MAP
            if model in MODEL_BRAND_MAP:
                brand = MODEL_BRAND_MAP[model]

        if (not brand or brand == "unknown") and model:

            from services.brand_detector import MODEL_BRAND_MAP

            if model:

                m = model.lower()

                if m in MODEL_BRAND_MAP:
                    brand = MODEL_BRAND_MAP[m]

        entities = extract_car_entities(
            title or "",
            f"{title or ''} {content or ''}"
        ) or {}

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

        if not model:
            model = resolve_model(
                brand,
                f"{title_text} {full_text}"
            )

        if (not brand or brand == "unknown") and model:
            from services.brand_detector import MODEL_BRAND_MAP

            if model:
                m = model.lower().strip()

                if m in MODEL_BRAND_MAP:
                    brand = MODEL_BRAND_MAP[m]

        if brand:
            brand = brand.lower().strip()
        else:
            brand = None

        fields = extract_fields(full_text)

        final_brand = brand
        if entities.get("brand"):
            final_brand = str(entities.get("brand")).lower().strip()

        final_model = model
        if entities.get("model"):
            final_model = entities.get("model")

        final_price = None
        ent_price = entities.get("price")
        if isinstance(ent_price, int) and ent_price > 0:
            final_price = ent_price
        elif isinstance(fields.get("price"), int) and fields.get("price") > 0:
            final_price = fields.get("price")

        final_year = year if isinstance(year, int) else fields.get("year")

        final_mileage = None
        ent_mileage = entities.get("mileage")
        if not _is_speed_noise(full_text):
            if isinstance(ent_mileage, int) and 0 <= ent_mileage <= 500_000:
                final_mileage = ent_mileage
            elif isinstance(fields.get("mileage"), int) and 0 <= fields.get("mileage") <= 500_000:
                final_mileage = fields.get("mileage")

        final_fuel = entities.get("fuel") or fields.get("fuel")

        if not final_model:
            final_model = resolve_model(
                final_brand,
                f"{title_text} {full_text}"
            )

        if final_price == 0:
            final_price = None

        doc = NormalizedDocument(
            raw_id=raw.id,
            source=raw.source,
            source_url=raw.source_url,
            title=raw.title,
            normalized_text=text,
            brand=final_brand,
            model=final_model,
            year=final_year if isinstance(final_year, int) else None,
            mileage=final_mileage if isinstance(final_mileage, int) else None,
            price=final_price if isinstance(final_price, int) and final_price > 0 else None,
            currency="RUB" if isinstance(final_price, int) and final_price > 0 else None,
            fuel=final_fuel if isinstance(final_fuel, str) else None,
            paint_condition=fields["paint_condition"],
        )

        session.add(doc)
        saved += 1

    session.commit()
    session.close()

    print(f"[NORMALIZE] docs_saved={saved} skipped={skipped} total={len(raws)}")