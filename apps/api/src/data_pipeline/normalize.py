import re
from datetime import datetime, timezone
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

# Слова-маркеры, по которым мы будем понимать, что это НЕ продажа машины
SPAM_PATTERNS = [
    "запчасти", "разбор", "по запчастям", "бампер", "фара", "капот", "крыло", "дверь",
    "двигатель", "двс", "акпп", "мкпп", "коробка", "приборка", "диски", "шины", "колеса", "резина",
    "ремонт", "ошибка", "чек", "подскажите", "вопрос", "куплю", "ищу", "приобрету", "замена", 
    "сервис", "колодки", "масло", "ваносы", "ксентри", "xentry", "кодирование", "чиптюнинг", "чип тюнинг"
]

CITIES_DB = {
    "москва": ("Москва", "Московская область"),
    "мск": ("Москва", "Московская область"),
    "санкт-петербург": ("Санкт-Петербург", "Ленинградская область"),
    "спб": ("Санкт-Петербург", "Ленинградская область"),
    "владивосток": ("Владивосток", "Приморский край"),
    "краснодар": ("Краснодар", "Краснодарский край"),
    "сочи": ("Сочи", "Краснодарский край"),
    "новосибирск": ("Новосибирск", "Новосибирская область"),
    "екатеринбург": ("Екатеринбург", "Свердловская область"),
    "нижний новгород": ("Нижний Новгород", "Нижегородская область"),
    "казань": ("Казань", "Татарстан"),
    "челябинск": ("Челябинск", "Челябинская область"),
    "омск": ("Омск", "Омская область"),
    "самара": ("Самара", "Самарская область"),
    "ростов": ("Ростов-на-Дону", "Ростовская область"),
    "уфа": ("Уфа", "Башкортостан"),
    "красноярск": ("Красноярск", "Красноярский край"),
    "пермь": ("Пермь", "Пермский край"),
    "воронеж": ("Воронеж", "Воронежская область"),
    "волгоград": ("Волгоград", "Волгоградская область"),
    "тольятти": ("Тольятти", "Самарская область"),
    "иркутск": ("Иркутск", "Иркутская область"),
    "тюмень": ("Тюмень", "Тюменская область"),
    "хабаровск": ("Хабаровск", "Хабаровский край"),
    "барнаул": ("Барнаул", "Алтайский край"),
    "ульяновск": ("Ульяновск", "Ульяновская область"),
    "ярославль": ("Ярославль", "Ярославская область"),
}

REGIONS = [
    "московская область", "ленинградская область", "приморский край", "краснодарский край",
    "новосибирская область", "свердловская область", "нижегородская область", "татарстан",
    "челябинская область", "омской область", "самарская область", "ростовская область",
    "башкортостан", "красноярский край", "пермский край", "воронежская область",
    "волгоградская область", "иркутская область", "тюменская область", "хабаровский край",
    "алтайский край", "ульяновская область", "ярославская область", "дагестан", "чечня"
]


def extract_mileage(text: str) -> Optional[int]:
    text = (text or "").lower().replace("\u00A0", " ").replace("\xa0", " ")

    if _is_speed_noise(text):
        return None

    # 🔥 Усиленный фикс для слитного написания и точек: 6.000km -> 6000 km
    text = re.sub(r"(\d)[.,](\d{3})\b", r"\1\2", text)
    text = text.replace("km", " km").replace("км", " км")

    patterns = [
        (r"пробег\s*[:-]?\s*(\d[\d\s]{1,7})", None),
        (r"(\d{1,3}[\s.,]?\d{1,3})\s*(?:км|km)", None),
        (r"(\d{1,3}(?:[.,]\d+)?)\s*(?:тыс|т\.км|ткм|k|к)\b", "thousand"),
        (r"(\d{4,7})\s*(?:км|km)", None),
    ]

    for pattern, multiplier in patterns:
        m = re.search(pattern, text)
        if not m:
            continue

        try:
            raw = re.sub(r"[^\d.]", "", m.group(1))
            value = float(raw)

            if multiplier == "thousand":
                value *= 1000

            value = int(value)

            if 1000 <= value <= 1_500_000:
                return value
        except:
            continue

    # 🔥 Спасаем машины из ТГ "Без пробега" и конвертируем их в 0 км (если не без пробега ПО РФ).
    if "без пробега" in text and "по рф" not in text and "по россии" not in text and "по р.ф." not in text:
         if not re.search(r"\b\d{1,3}[\s.,]?\d{3}\s*(км|km)", text):
             return 0

    return None


def extract_fuel(text: str) -> Optional[str]:
    text = (text or "").lower()
    text = text.replace(".", " ")

    # 🔥 ИСПОЛЬЗУЕМ ГРАНИЦЫ СЛОВ (\b), ЧТОБЫ ИСКЛЮЧИТЬ СЛОВА ТИПА "электропривод"
    patterns = {
        "electric": r"\b(электро|электромобиль|electric|ev)\b",
        "hybrid": r"\b(гибрид|hybrid|phev|hev)\b",
        "diesel": r"\b(дизель|дизельный|диз|diesel|tdi|cdi|dci)\b",
        "gas_petrol": r"\b(газ\s*/\s*бензин|бензин\s*/\s*газ|газ\s+бензин|бензин\s+газ)\b",
        "gas": r"\b(газ|lpg|cng|гбо)\b",
        "petrol": r"\b(бензин|бензиновый|бенз|petrol|gasoline|mpi|fsi|tsi|tfsi)\b",
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
        "бензин": "petrol", "бензиновый": "petrol", "бенз": "petrol", "petrol": "petrol",
        "gasoline": "petrol", "mpi": "petrol", "tsi": "petrol", "tfsi": "petrol", "fsi": "petrol",
        "дизель": "diesel", "дизельный": "diesel", "диз": "diesel", "diesel": "diesel",
        "tdi": "diesel", "dci": "diesel", "cdi": "diesel",
        "гибрид": "hybrid", "hybrid": "hybrid", "phev": "hybrid", "hev": "hybrid",
        "электро": "electric", "электр": "electric", "electric": "electric", "ev": "electric",
        "газ": "gas", "lpg": "gas", "gbo": "gas", "cng": "gas",
        "газ/бензин": "gas_petrol", "газ бензин": "gas_petrol",
    }

    return fuel_map.get(v, v if v in {"petrol", "diesel", "electric", "hybrid", "gas", "gas_petrol"} else None)


def _sanitize_mileage_value(v: Optional[int]) -> Optional[int]:
    if v is None:
        return None
    try:
        v = int(v)
    except Exception:
        return None

    # 🔥 Разрешаем пробеги >= 0 км, чтобы не удалять абсолютно новые машины!
    if v < 0:
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
    m = re.search(r"/([a-z0-9\-]+)/([a-z0-9\-]+)/", url)
    if m:
        brand = m.group(1)
        model = m.group(2)
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
    "бензин": "petrol", "бензиновый": "petrol", "бенз": "petrol", "petrol": "petrol", "gasoline": "petrol",
    "mpi": "petrol", "fsi": "petrol", "tsi": "petrol", "tfsi": "petrol",
    "дизель": "diesel", "дизельный": "diesel", "диз": "diesel", "diesel": "diesel",
    "tdi": "diesel", "dci": "diesel", "cdi": "diesel",
    "гибрид": "hybrid", "hybrid": "hybrid", "phev": "hybrid", "hev": "hybrid",
    "электро": "electric", "электр": "electric", "electric": "electric", "ev": "electric",
    "газ": "gas", "lpg": "gas", "gbo": "gas", "cng": "gas",
    "газ/бензин": "gas_petrol", "газ бензин": "gas_petrol",
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

    text = text.replace("\u00A0", " ").replace("\xa0", " ").replace("\t", " ")
    text = text.replace("\r", " ").replace("\n", " ").replace("₽", " ₽ ")

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
        "км/ч", "km/h", "скорость", "средняя скорость",
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
        "Спецтехника", "Отзывы", "Каталог", "Шины", "Форумы", "ОСАГО", "ПДД", "Проверка по VIN",
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

    min_keep_len = 200
    cleaned = text

    # 1. Отрезаем по известным маркерам
    for marker in cut_markers:
        idx = cleaned.find(marker)
        if idx != -1 and idx >= min_keep_len:
            cleaned = cleaned[:idx].strip()
            break

    # 🔥 2. УБИЙЦА МУСОРА DROM: Отрезаем блок "Похожие", если пошло много цен подряд
    # Ищем вхождения цен (например, "1 500 000 ₽")
    prices = list(re.finditer(r"\b\d[\d\s]{2,10}\s*₽", cleaned))
    if len(prices) > 3:
        # Если в тексте больше 3 цен, скорее всего 4-я цена - это начало чужих объявлений
        cutoff_idx = prices[3].start()
        if cutoff_idx > min_keep_len:
            cleaned = cleaned[:cutoff_idx].strip()

    return cleaned


def extract_image_url(raw: RawDocument, text: str) -> Optional[str]:
    # 1. Извлечение из медиа-данных и атрибутов raw-документа
    for attr in ["image_url", "image", "preview_url", "preview", "photos", "photo", "media"]:
        if hasattr(raw, attr):
            val = getattr(raw, attr)
            if val:
                if isinstance(val, str) and val.startswith("http"):
                    return val
                elif isinstance(val, list) and len(val) > 0 and isinstance(val[0], str) and val[0].startswith("http"):
                    return val[0]
    
    if hasattr(raw, "meta") and raw.meta:
        meta_str = str(raw.meta)
        m = re.search(r"https?://[^\s'\"<>]+?\.(?:jpg|jpeg|png|webp)", meta_str, re.IGNORECASE)
        if m:
            return m.group(0)

    # 2. Поиск прямой ссылки в тексте
    m = re.search(r"https?://[^\s'\"<>]+?\.(?:jpg|jpeg|png|webp)", text, re.IGNORECASE)
    if m:
        return m.group(0)

    # 3. Fallback None
    return None


def extract_fields(text: str, raw: Optional[RawDocument] = None) -> Dict[str, Optional[object]]:
    text = text or ""
    text = text.replace("\u00A0", " ").replace("\xa0", " ")
    lower = text.lower()

    mileage = None
    fuel = None
    price = None
    year = None
    city = None
    region = None

    current_year = datetime.now(timezone.utc).year

    def _valid_year(value: int) -> bool:
        return 1985 <= value <= current_year + 1

    def _valid_price(value: int) -> bool:
        return 10_000 <= value <= 200_000_000

    def _valid_mileage(value: int) -> bool:
        return 0 <= value <= 1_500_000

    # 🔥 УЛУЧШЕННОЕ ИЗВЛЕЧЕНИЕ ГОДА
    year_patterns = [
        r"\b(19\d{2}|20\d{2})\s*(?:г\.в|г\.|г|год|года)\b",
        r"\b(19\d{2}|20\d{2})\b"
    ]
    for pat in year_patterns:
        matches = re.findall(pat, lower)
        for y in matches:
            try:
                val = int(y)
                if _valid_year(val):
                    year = val
                    break
            except:
                continue
        if year:
            break

    # 🔥 УЛУЧШЕННОЕ ИЗВЛЕЧЕНИЕ ЦЕНЫ (Поддержка "млн", "миллиона")
    mln_match = re.search(r"(\d+(?:[.,]\d+)?)\s*(?:млн|миллиона|миллионов)\b", lower)
    if mln_match:
        try:
            val = float(mln_match.group(1).replace(",", "."))
            val_rub = int(val * 1_000_000)
            if _valid_price(val_rub):
                price = val_rub
        except:
            pass

    if not price:
        price_patterns = [
            r"цена\s*[:-]?\s*(\d[\d\s\.\,]{4,11})(?:\s*₽|\s*руб|\s*р\b|\b|$)",
            r"(?<!\d)(\d[\d\s\u00A0]{3,12})\s*\|?\s*(₽|руб(?:\.|лей)?|р\b)(?!\d)",
        ]
        for pat in price_patterns:
            for m in re.finditer(pat, lower):
                raw_p = re.sub(r"[^\d]", "", m.group(1))
                try:
                    val = int(raw_p)
                    if _valid_price(val):
                        price = val
                        break
                except:
                    pass
            if price:
                break

    if not price:
        m = RE_PRICE_TITLE_GLUE.search(text)
        if m:
            try:
                val = int(_digits_only(m.group(1)))
                if _valid_price(val):
                    price = val
            except:
                pass

    # 🔥 УЛУЧШЕННОЕ ИЗВЛЕЧЕНИЕ ПРОБЕГА
    if "без пробега" in lower and "по рф" not in lower and "по россии" not in lower and "по р.ф." not in lower:
        if not re.search(r"\b\d{1,3}[\s.,]?\d{3}\s*(км|km)", lower):
            mileage = 0

    if mileage is None:
        m = re.search(r"пробег[^\d]{0,10}?([\d\s]{3,10})", lower)
        if m:
            try:
                val = int(re.sub(r"[^\d]", "", m.group(1)))
                if _valid_mileage(val):
                    mileage = val
            except:
                pass

    if mileage is None:
        m = re.search(r"(\d{1,3})\s*(?:тыс|т\.км|ткм)", lower)
        if m:
            try:
                val = int(m.group(1)) * 1000
                if _valid_mileage(val):
                    mileage = val
            except:
                pass

    if mileage is None:
        m = re.search(r"(\d[\d\s]{2,10})\s*(км|km)\b", lower)
        if m:
            try:
                val = int(re.sub(r"[^\d]", "", m.group(1)))
                if _valid_mileage(val):
                    mileage = val
            except:
                pass

    if mileage is None:
        fallback = extract_mileage(text)
        if fallback is not None and _valid_mileage(fallback):
            mileage = fallback

    # 🔥 УЛУЧШЕННОЕ ИЗВЛЕЧЕНИЕ ТОПЛИВА
    fuel_patterns = {
        "electric": r"\b(электро|электромобиль|electric|ev)\b",
        "hybrid": r"\b(гибрид|hybrid|phev|hev)\b",
        "diesel": r"\b(дизель|дизельный|диз|diesel|tdi|cdi|dci)\b",
        "gas_petrol": r"\b(газ\s*/\s*бензин|бензин\s*/\s*газ|газ\s+бензин|бензин\s+газ)\b",
        "gas": r"\b(газ|lpg|cng|гбо)\b",
        "petrol": r"\b(бензин|бензиновый|бенз|petrol|gasoline|mpi|fsi|tsi|tfsi)\b",
    }
    for fuel_key, pattern in fuel_patterns.items():
        if re.search(pattern, lower):
            fuel = fuel_key
            break

    paint_condition = None
    if any(x in lower for x in ["без окраса", "без окрасов", "без окрас", "не бит", "не крашен", "не крашена"]):
        paint_condition = "original"
    elif any(x in lower for x in ["крашен", "крашена", "окрас", "бит"]):
        paint_condition = "repainted"

    # 🔥 ИЗВЛЕЧЕНИЕ ГОРОДА И РЕГИОНА (из source-атрибутов)
    if raw:
        for attr in ["city", "region", "location"]:
            if hasattr(raw, attr):
                val = getattr(raw, attr)
                if val and isinstance(val, str) and val.strip():
                    if attr == "city" or attr == "location":
                        city = val.strip()
                    else:
                        region = val.strip()

    # Извлечение города из текста
    if not city:
        for word, (c_val, r_val) in CITIES_DB.items():
            if re.search(rf"\b{word}\b", lower):
                city = c_val
                region = r_val
                break

    if not region:
        for reg in REGIONS:
            if reg in lower:
                region = reg.title()
                break

    return {
        "year": year,
        "mileage": mileage,
        "price": price,
        "currency": "RUB" if price else None,
        "fuel": fuel,
        "paint_condition": paint_condition,
        "city": city,
        "region": region,
    }


def _safe_quality_score(skip: bool, sale_intent: bool, brand: Optional[str], model: Optional[str], fields: Dict[str, Any], source_boost: float) -> float:
    score = 0.0
    if not skip: score += 0.35
    if sale_intent: score += 0.20
    if brand: score += 0.20
    if model: score += 0.10
    if fields.get("price"): score += 0.05
    if fields.get("year"): score += 0.05
    if fields.get("mileage"): score += 0.05
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


def _build_normalized_document_kwargs(raw: RawDocument, normalized_text: str, brand: Optional[str], model: Optional[str], fields: Dict[str, Any], sale_intent: bool, quality_score: float) -> Dict[str, Any]:
    # Сбор и нормализация created_at
    raw_created_at = getattr(raw, "created_at", None)
    if not raw_created_at:
        raw_created_at_ts = getattr(raw, "created_at_ts", None)
        if raw_created_at_ts:
            try:
                raw_created_at = datetime.fromtimestamp(int(raw_created_at_ts), tz=timezone.utc).isoformat()
            except:
                pass
    if not raw_created_at:
        raw_created_at = datetime.now(timezone.utc).isoformat()

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
        kwargs["sale_intent"] = int(sale_intent)

    if "quality_score" in model_columns:
        kwargs["quality_score"] = quality_score

    # Динамическая поддержка обогащенных данных
    if "city" in model_columns:
        kwargs["city"] = fields.get("city")

    if "region" in model_columns:
        kwargs["region"] = fields.get("region")

    if "image_url" in model_columns:
        kwargs["image_url"] = fields.get("image_url")

    if "created_at" in model_columns:
        kwargs["created_at"] = raw_created_at

    if "created_at_ts" in model_columns:
        try:
            ts = int(datetime.fromisoformat(raw_created_at).timestamp())
        except:
            ts = int(datetime.now(timezone.utc).timestamp())
        kwargs["created_at_ts"] = ts

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

    try:
        for raw in raws:
            exists = session.query(NormalizedDocument).filter_by(source_url=raw.source_url).first()

            if exists and not force_rebuild:
                continue

            if exists and force_rebuild:
                session.delete(exists)
                session.flush()

            # Фильтр: если нет title -> пропускаем
            title_text = normalize_title_format((raw.title or "").strip())
            if not title_text:
                print(f"[DEBUG NORMALIZE] Пропущено (нет title): raw_id={raw.id}")
                skipped += 1
                continue

            raw_body_text = (raw.content or "").strip()
            body_text = strip_drom_noise(raw_body_text)

            raw_text = f"{title_text}\n{body_text}".strip()
            clean_pipeline_text = raw_text

            # 🔥 СПАМ-ФИЛЬТР ТЕЛЕГРАМА (Вопросы, запчасти, болтовня)
            is_tg = "t.me" in (raw.source_url or "").lower() or "telegram" in (raw.source or "").lower()
            if is_tg:
                lower_text = clean_pipeline_text.lower()
                is_spam = False
                
                # Ищем слова-маркеры болтовни или запчастей
                for w in SPAM_PATTERNS:
                    if re.search(rf"\b{w}\b", lower_text):
                        is_spam = True
                        print(f"[DEBUG TG SPAM] Пропущено из-за слова '{w}': {title_text[:50]}")
                        break
                
                # Если в сообщении нет года авто - это 100% не продажа машины
                has_year = bool(RE_YEAR.search(lower_text))
                if is_spam or not has_year:
                    skipped += 1
                    continue

            fields = {}

            skip, _ = should_skip_doc(text=clean_pipeline_text, source=raw.source or "")
            if skip:
                skipped += 1
                continue

            taxonomy_brand, taxonomy_model, brand_conf = _extract_canonical_entities(
                title_text=title_text,
                body_text=raw_body_text,
            )

            final_brand = taxonomy_brand
            final_model = taxonomy_model

            # 🔥 SOURCE_URL BOOST
            url_brand, url_model = extract_from_url(raw.source_url)
            if url_brand:
                try: url_brand = taxonomy_service.canonicalize_brand(url_brand)
                except: pass

            if url_brand and not final_brand:
                final_brand = url_brand

            if url_model and final_brand and not final_model:
                try: final_model = taxonomy_service.canonicalize_model(final_brand, url_model)
                except: final_model = url_model

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

                if final_brand and extracted_model:
                    if extracted_model != final_brand:
                        final_model = extracted_model
                        if final_model == final_brand:
                            final_model = None

            if final_brand:
                final_brand = taxonomy_service.canonicalize_brand(final_brand)

            if final_brand in CITY_BLACKLIST:
                final_brand = None

            if final_brand and final_model:
                try: final_model = taxonomy_service.canonicalize_model(final_brand, final_model)
                except: pass

            title_lower = (raw.title or "").lower()
            if not final_brand:
                brand_map = {
                    "bmw": "bmw", "mercedes": "mercedes", "benz": "mercedes", "toyota": "toyota",
                    "kia": "kia", "hyundai": "hyundai", "lexus": "lexus", "audi": "audi",
                    "ford": "ford", "honda": "honda", "nissan": "nissan", "mazda": "mazda",
                    "lada": "lada", "volvo": "volvo", "land rover": "land_rover", "range rover": "land_rover",
                }
                for k, v in brand_map.items():
                    if k in raw_text.lower():
                        final_brand = v
                        if not final_model:
                            m = re.search(rf"{k}\s+([a-z0-9\-]+)", raw_text.lower())
                            if m:
                                final_model = m.group(1)
                        break

            search_model = final_model.replace("_", "").replace("-", "") if final_model else None

            if search_model == final_brand:
                search_model = None

            extracted_fields = extract_fields(raw_text, raw=raw)
            if extracted_fields:
                for k, v in extracted_fields.items():
                    if v is not None and fields.get(k) is None:
                        fields[k] = v

            # 🔥 Извлечение ссылки на изображение
            if not fields.get("image_url"):
                fields["image_url"] = extract_image_url(raw, raw_text)

            # 🔥 АВТООПРЕДЕЛЕНИЕ ТОПЛИВА ПО МАРКЕ (Спасаем Zeekr, Li, Tesla и т.д.)
            def _infer_fuel(b):
                b = (b or "").lower()
                if b in {"zeekr", "tesla", "byd", "xiaomi", "avatr", "hiphi", "nio", "rivian"}: return "electric"
                if b in {"li_auto", "lixiang", "aito"}: return "hybrid"
                return None

            if not fields.get("fuel"):
                fields["fuel"] = _infer_fuel(final_brand)

            # 🔥 HARD fallback — ищем везде топливо
            if not fields.get("fuel"):
                fuel_fallback = extract_fuel(raw_text) or extract_fuel(title_text) or extract_fuel(raw_body_text)
                if fuel_fallback: fields["fuel"] = _normalize_fuel_value(fuel_fallback)

            fuel_val = fields.get("fuel")

            if fuel_val:
                normalized_fuel = _normalize_fuel_value(fuel_val)
                if normalized_fuel:
                    fields["fuel"] = normalized_fuel
                else:
                    fields["fuel"] = fuel_val
                
            if fields.get("mileage") is not None:
                fields["mileage"] = _sanitize_mileage_value(fields.get("mileage"))

            # 🔥 HARD fallback mileage
            if not fields.get("mileage"):
                fallback_mileage = extract_mileage(raw_text) or extract_mileage(title_text) or extract_mileage(raw_body_text)
                if fallback_mileage: fields["mileage"] = _sanitize_mileage_value(fallback_mileage)

            if entities:
                if not fields.get("mileage") and entities.get("mileage") is not None:
                    try: fields["mileage"] = _sanitize_mileage_value(int(entities.get("mileage")))
                    except: pass
                if not fields.get("fuel") and entities.get("fuel"):
                    fields["fuel"] = _normalize_fuel_value(entities.get("fuel"))
                if not fields.get("price") and entities.get("price") is not None:
                    try: 
                        fields["price"] = int(entities.get("price"))
                        fields["currency"] = "RUB"
                    except: pass
                if not fields.get("year") and entities.get("year") is not None:
                    try: fields["year"] = int(entities.get("year"))
                    except: pass

            # Фильтр: если нет price -> пропускаем
            final_price = fields.get("price")
            if final_price is None:
                print(f"[DEBUG NORMALIZE] Пропущено (нет price): {title_text[:50]}")
                skipped += 1
                continue

            sale = detect_sale_intent(raw_text)
            if sale == 0:
                sale = int(extract_sale(raw_text))

            source_boost = resolve_source_boost(raw.source or "")
            quality_score = _safe_quality_score(
                skip=skip, sale_intent=bool(sale), brand=final_brand,
                model=search_model, fields=fields, source_boost=source_boost,
            )

            meta_prefix = build_meta_prefix(
                brand=final_brand, brand_confidence=brand_conf,
                sale_intent=bool(sale), source_boost=source_boost,
            )

            enriched_content = apply_meta_prefix(clean_pipeline_text, meta_prefix)
            _meta, content_wo_meta = parse_meta(enriched_content)
            normalized_text = clean_text(content_wo_meta)

            print("[DEBUG NORMALIZE FULL]", {
                "TEXT_SAMPLE": raw_text.replace('\n', ' ')[:200], # Выводим чище без переносов
                "brand": final_brand,
                "model": search_model,
                "fuel": fields.get("fuel"),
                "mileage": fields.get("mileage"),
                "price": fields.get("price"),
                "city": fields.get("city"),
                "region": fields.get("region"),
                "image_url": fields.get("image_url")[:60] if fields.get("image_url") else None,
            })

            doc_kwargs = _build_normalized_document_kwargs(
                raw=raw, normalized_text=normalized_text, brand=final_brand,
                model=search_model, fields=fields, sale_intent=bool(sale),
                quality_score=quality_score,
            )

            # 🔥 SQL ПАТЧ: UPSERT - Если объявление с таким URL уже есть, обновляем его, а не падаем с ошибкой
            from sqlalchemy.dialects.postgresql import insert

            stmt = insert(NormalizedDocument).values(**doc_kwargs)
            stmt = stmt.on_conflict_do_update(
                index_elements=['source_url'], # По этому индексу ловили ошибку UniqueViolation
                set_=doc_kwargs                # Обновляем все поля свежими данными
            )
            session.execute(stmt)
            saved += 1

        session.commit()

    finally:
        session.close()

    return saved