🔴 ЧАСТЬ 1 — QUERY PARSER (делаем его железным)

from typing import List, Optional, Dict, Tuple
from pydantic import BaseModel, Field
import re
import yaml
from pathlib import Path
import datetime


# =========================
# SAFE DIGITS NORMALIZER
# =========================
def _digits_only(s: str) -> str:
    """
    Оставляет только цифры.
    Чистит NBSP, пробелы, точки, запятые и любые разделители.
    """
    if not s:
        return ""
    s = str(s).replace("\xa0", " ")
    return re.sub(r"[^\d]", "", s)


# =========================
# LOAD BRANDS (SINGLE SOURCE OF TRUTH)
# =========================
def load_brands() -> Dict[str, Dict[str, List[str]]]:
    """
    Загружает brands.yaml один раз.

    Ожидаемый формат:
    {
      "bmw": {
        "en": [...],
        "ru": [...],
        "aliases": [...]
      }
    }
    """
    try:
        base_dir = Path(__file__).resolve().parent.parent  # .../src
        brands_path = base_dir / "config" / "brands.yaml"

        with open(brands_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            return data.get("brands", {})

    except Exception as e:
        print(f"[QUERY][WARN] failed to load brands.yaml: {e}")
        return {}


BRANDS_CONFIG = load_brands()


# =========================
# SCHEMA
# =========================
class StructuredQuery(BaseModel):
    raw_query: Optional[str] = None

    brand: Optional[str] = None
    brand_confidence: float = 0.0
    model: Optional[str] = None

    price_max: Optional[int] = None
    mileage_max: Optional[int] = None
    year_min: Optional[int] = None

    fuel: Optional[str] = None
    paint_condition: Optional[str] = None
    city: Optional[str] = None
    region: Optional[str] = None

    keywords: List[str] = Field(default_factory=list)
    exclusions: List[str] = Field(default_factory=list)

    class Config:
        extra = "forbid"


# =========================
# MAIN ENTRY
# =========================
def parse_query(raw_text: str) -> StructuredQuery:
    raw_text = (raw_text or "").strip()

    if not raw_text:
        return StructuredQuery(raw_query=raw_text)

    try:
        llm_result = _parse_with_llm(raw_text)
        sq = StructuredQuery(**llm_result)
        sq.raw_query = raw_text
        return sq

    except Exception:
        return _parse_with_fallback(raw_text)


# =========================
# LLM PLACEHOLDER
# =========================
def _parse_with_llm(raw_text: str) -> dict:
    raise RuntimeError("LLM not implemented yet")


# =========================
# FALLBACK PARSER (RULE-BASED)
# =========================
def _parse_with_fallback(raw_text: str) -> StructuredQuery:
    text = (raw_text or "").lower().replace("\xa0", " ").strip()
    result = StructuredQuery(raw_query=raw_text)

    # -------------------------
    # BRAND
    # -------------------------
    brand, confidence = _extract_brand(text)
    if brand:
        result.brand = brand.lower().strip()
        result.brand_confidence = float(confidence)

    # -------------------------
    # MILEAGE (max) — FIXED
    # -------------------------

    # 1️⃣ строгий пробег: 100 000 км / до 100 000 км / 100 тыс км
    m = re.search(
        r"(?:пробег\s*)?(?:до|<=|<)?\s*([\d\s\xa0.,]+)\s*(тыс\s*км|км|km)\b",
        text
    )

    if m:
        raw = m.group(1)
        unit = (m.group(2) or "").replace(" ", "")

        digits = re.sub(r"[^\d]", "", raw)
        if digits:
            val = int(digits)

            if "тыс" in unit:
                val *= 1000

            if val > 0:
                result.mileage_max = val

                # удаляем этот кусок из текста чтобы price не схватил
                text = text[:m.start()] + text[m.end():]

    # -------------------------
    # PRICE (max) — FIXED
    # -------------------------

    # 2.5 млн
    m = re.search(r"(?:до|<=|<)?\s*(\d+(?:[.,]\d+)?)\s*(млн|миллион|m)\b", text)
    if m:
        value = float(m.group(1).replace(",", "."))
        result.price_max = int(value * 1_000_000)
    else:
        # только если есть руб / ₽
        m = re.search(
            r"(?:до|<=|<)?\s*([\d\s\xa0.,]+)\s*(₽|руб|р\b)",
            text
        )
        if m:
            digits = re.sub(r"[^\d]", "", m.group(1))
            if digits:
                result.price_max = int(digits)

    # -------------------------
    # YEAR
    # -------------------------
    current_year = datetime.datetime.now().year

    m = re.search(r"не\s*старше\s*(\d+)\s*лет", text)
    if m:
        years = int(m.group(1))
        result.year_min = current_year - years

    m = re.search(r"не\s*старше\s*(20\d{2}|19\d{2})\b", text)
    if m:
        result.year_min = int(m.group(1))

    # -------------------------
    # FUEL
    # -------------------------
    if "бенз" in text:
        result.fuel = "petrol"
    elif "диз" in text:
        result.fuel = "diesel"
    elif "гибрид" in text:
        result.fuel = "hybrid"
    elif "электро" in text or "электр" in text:
        result.fuel = "electric"
    elif "газ" in text or "гбо" in text:
        result.fuel = "gas_petrol"

    # -------------------------
    # PAINT CONDITION
    # -------------------------
    if "без окрас" in text or "не бит" in text or "родная краска" in text:
        result.paint_condition = "original"
    elif "крашен" in text or "бит" in text:
        result.paint_condition = "repainted"

    # -------------------------
    # CITY
    # -------------------------
    m = re.search(
        r"\b(москва|спб|питер|екатеринбург|казань|новосибирск|алматы|астана)\b",
        text,
    )
    if m:
        result.city = m.group(1)

    # -------------------------
    # REGION (простая эвристика)
    # -------------------------
    regions = [
        "московская область",
        "ленинградская область",
        "краснодарский край",
        "татарстан",
    ]

    for r in regions:
        if r in text:
            result.region = r
            break

    # -------------------------
    # RECENCY INTENT
    # -------------------------
    if any(w in text for w in ["свеж", "нов", "последн"]):
        result.keywords.append("recent")

    # -------------------------
    # KEYWORDS / EXCLUSIONS
    # -------------------------
    tokens = re.findall(r"[a-zа-я0-9]+", text)

    STOP_TOKENS = {
        "до", "без", "и", "или", "не",
        "бит", "крашен",
        "км", "тыс", "руб", "р", "₽",
        "лет", "старше",
        "год", "года",
    }

    for t in tokens:

        # если это уже распарсенный бренд  не добавляем
        if result.brand and t == result.brand:
            continue

        # если это топливо  не добавляем
        if result.fuel and t in {"бенз", "бензин", "диз", "дизель", "гибрид", "электро", "газ"}:
            continue

        if t.startswith("не") and len(t) > 2 and t not in {"не"}:
            result.exclusions.append(t[2:])
            continue

        if t in STOP_TOKENS:
            continue

        if t not in result.keywords:
            result.keywords.append(t)

    return result


# =========================
# BRAND EXTRACTION LOGIC
# =========================
def _extract_brand(text: str) -> Tuple[Optional[str], float]:
    if not BRANDS_CONFIG:
        return None, 0.0

    for brand, cfg in BRANDS_CONFIG.items():
        words = (cfg.get("en", []) or []) + (cfg.get("ru", []) or [])
        aliases = cfg.get("aliases", []) or []

        for w in words:
            w = (w or "").lower().strip()
            if not w:
                continue
            if re.search(rf"\b{re.escape(w)}\b", text):
                return brand, 1.0

        for a in aliases:
            a = (a or "").lower().strip()
            if not a:
                continue
            if re.search(rf"\b{re.escape(a)}\b", text):
                return brand, 0.8

        for w in words:
            w = (w or "").lower().strip()
            if not w:
                continue
            if w in text:
                return brand, 0.6

    return None, 0.0