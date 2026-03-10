import re
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import yaml


def load_brands() -> Dict[str, Dict[str, List[str]]]:
    try:
        base_dir = Path(__file__).resolve().parent.parent
        brands_path = base_dir / "config" / "brands.yaml"

        with open(brands_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            brands = data.get("brands", {})
            return brands if isinstance(brands, dict) else {}

    except Exception as e:
        print(f"[BRAND][WARN] brands.yaml load failed: {e}", flush=True)
        return {}


BRANDS = load_brands()


MODEL_BRAND_MAP = {
    # =========================
    # TOYOTA
    # =========================
    "camry": "toyota",
    "corolla": "toyota",
    "corolla fielder": "toyota",
    "rav4": "toyota",
    "rav 4": "toyota",
    "prado": "toyota",
    "land cruiser": "toyota",
    "land cruiser prado": "toyota",
    "highlander": "toyota",
    "hilux": "toyota",
    "tacoma": "toyota",
    "vista": "toyota",
    "ipsum": "toyota",
    "isis": "toyota",

    # =========================
    # MERCEDES
    # =========================
    "e class": "mercedes",
    "c class": "mercedes",
    "s class": "mercedes",
    "glc": "mercedes",
    "glc class": "mercedes",
    "gle": "mercedes",
    "gls": "mercedes",
    "cla": "mercedes",
    "cls": "mercedes",
    "gla": "mercedes",
    "glb": "mercedes",
    "g class": "mercedes",
    "e200": "mercedes",
    "e300": "mercedes",
    "c200": "mercedes",
    "c300": "mercedes",

    # =========================
    # BMW
    # =========================
    "x1": "bmw",
    "x2": "bmw",
    "x3": "bmw",
    "x4": "bmw",
    "x5": "bmw",
    "x6": "bmw",
    "x7": "bmw",
    "1 series": "bmw",
    "2 series": "bmw",
    "3 series": "bmw",
    "4 series": "bmw",
    "5 series": "bmw",
    "6 series": "bmw",
    "7 series": "bmw",
    "320": "bmw",
    "330": "bmw",
    "520": "bmw",
    "530": "bmw",

    # =========================
    # AUDI
    # =========================
    "a3": "audi",
    "a4": "audi",
    "a6": "audi",
    "a8": "audi",
    "q3": "audi",
    "q5": "audi",
    "q7": "audi",
    "q8": "audi",

    # =========================
    # NISSAN
    # =========================
    "x trail": "nissan",
    "x-trail": "nissan",
    "qashqai": "nissan",
    "patrol": "nissan",
    "teana": "nissan",
    "note": "nissan",
    "murano": "nissan",
    "maxima": "nissan",

    # =========================
    # HYUNDAI
    # =========================
    "solaris": "hyundai",
    "sonata": "hyundai",
    "elantra": "hyundai",
    "tucson": "hyundai",
    "santa fe": "hyundai",
    "grand santa fe": "hyundai",
    "creta": "hyundai",
    "palisade": "hyundai",
    "i40": "hyundai",
    "getz": "hyundai",

    # =========================
    # KIA
    # =========================
    "rio": "kia",
    "cerato": "kia",
    "k5": "kia",
    "optima": "kia",
    "sportage": "kia",
    "sorento": "kia",
    "carnival": "kia",
    "tasman": "kia",
    "pegas": "kia",
    "ceed": "kia",

    # =========================
    # HONDA
    # =========================
    "civic": "honda",
    "accord": "honda",
    "cr v": "honda",
    "cr-v": "honda",
    "stream": "honda",
    "pilot": "honda",

    # =========================
    # MAZDA
    # =========================
    "cx 5": "mazda",
    "cx-5": "mazda",
    "cx 60": "mazda",
    "cx-60": "mazda",
    "mazda3": "mazda",
    "mazda 3": "mazda",
    "mazda6": "mazda",
    "mazda 6": "mazda",

    # =========================
    # LAND ROVER
    # =========================
    "range rover": "land_rover",
    "range rover sport": "land_rover",
    "discovery": "land_rover",
    "discovery sport": "land_rover",
    "evoque": "land_rover",
    "velar": "land_rover",

    # =========================
    # LEXUS
    # =========================
    "rx": "lexus",
    "rx350": "lexus",
    "nx": "lexus",
    "lx": "lexus",

    # =========================
    # GEELY
    # =========================
    "monjaro": "geely",

    # =========================
    # CHEVROLET
    # =========================
    "cruze": "chevrolet",

    # =========================
    # GENESIS
    # =========================
    "gv70": "genesis",
    "gv80": "genesis",

    # =========================
    # LI AUTO
    # =========================
    "l7": "li_auto",
    "l9": "li_auto",

    # =========================
    # RAM
    # =========================
    "1500": "ram",

    # =========================
    # LADA / ВАЗ
    # =========================
    "vesta": "lada",
    "granta": "lada",
    "niva": "lada",
    "largus": "lada",
}


def _normalize_text(text: str) -> str:
    text = text or ""
    text = text.replace("\u00A0", " ")
    text = text.replace("\xa0", " ")
    text = re.sub(r"[-_/]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()


def _build_candidates(cfg: Dict[str, List[str]]) -> List[str]:
    values: List[str] = []

    if not isinstance(cfg, dict):
        return values

    for key in ("en", "ru", "aliases"):
        items = cfg.get(key, [])
        if not isinstance(items, list):
            continue
        for v in items:
            if isinstance(v, str) and v.strip():
                values.append(_normalize_text(v))

    return list(dict.fromkeys(values))


def _contains_phrase(text: str, phrase: str) -> bool:
    if not phrase:
        return False

    pattern = r"\b" + r"\s+".join(re.escape(p) for p in phrase.split()) + r"\b"
    return bool(re.search(pattern, text, re.IGNORECASE))


def _match_brand(text: str) -> Tuple[Optional[str], float]:
    """
    Phase A: direct brand match from brands.yaml
    Phase B: model -> brand fallback
    """
    if not text:
        return None, 0.0

    text_norm = _normalize_text(text)

    # =====================================
    # PHASE A — direct yaml brand match
    # =====================================
    best_brand = None
    best_len = 0

    for brand_key, cfg in BRANDS.items():
        candidates = _build_candidates(cfg)

        for candidate in candidates:
            if _contains_phrase(text_norm, candidate):
                clen = len(candidate)

                if clen > best_len:
                    best_brand = str(brand_key).lower()
                    best_len = clen

    if best_brand:
        confidence = 0.95 + min(0.05, best_len / 50)
        if confidence > 1.0:
            confidence = 1.0
        return best_brand, confidence

    # =====================================
    # PHASE B — model -> brand fallback
    # =====================================
    best_model_brand = None
    best_model_len = 0

    for model_token, brand in MODEL_BRAND_MAP.items():
        token_norm = _normalize_text(model_token)

        if _contains_phrase(text_norm, token_norm):
            tlen = len(token_norm)

            if tlen > best_model_len:
                best_model_brand = brand
                best_model_len = tlen

    if best_model_brand:
        confidence = 0.82 + min(0.06, best_model_len / 40)
        if confidence > 0.88:
            confidence = 0.88
        return best_model_brand, confidence

    return None, 0.0


def detect_brand(title: str = "", text: str = "") -> Tuple[Optional[str], float]:
    """
    Production brand detection priority:
    1) title direct yaml match
    2) title phrase yaml match
    3) full text direct yaml match
    4) title model->brand fallback
    5) full text model->brand fallback
    """

    title = (title or "").strip()
    text = (text or "").strip()

    # 1–2 title yaml match
    brand, conf = _match_brand(title)
    if brand and conf >= 0.95:
        return brand, conf

    # 3 full text yaml match
    brand, conf = _match_brand(text)
    if brand and conf >= 0.95:
        return brand, conf

    # 4 title model fallback
    brand, conf = _match_brand(title)
    if brand and 0.82 <= conf <= 0.88:
        return brand, conf

    # 5 text model fallback
    brand, conf = _match_brand(text)
    if brand and 0.82 <= conf <= 0.88:
        return brand, conf

    return None, 0.0