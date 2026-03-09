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
    "camry": "toyota",
    "corolla": "toyota",
    "rav4": "toyota",
    "rav 4": "toyota",
    "prado": "toyota",
    "land cruiser": "toyota",

    "x1": "bmw",
    "x2": "bmw",
    "x3": "bmw",
    "x4": "bmw",
    "x5": "bmw",
    "x6": "bmw",
    "x7": "bmw",
    "3 series": "bmw",
    "5 series": "bmw",

    "c class": "mercedes",
    "e class": "mercedes",
    "s class": "mercedes",
    "glc": "mercedes",
    "gle": "mercedes",
    "gls": "mercedes",
    "g class": "mercedes",

    "a4": "audi",
    "a6": "audi",
    "q3": "audi",
    "q5": "audi",
    "q7": "audi",
    "q8": "audi",

    "solaris": "hyundai",
    "elantra": "hyundai",
    "sonata": "hyundai",
    "tucson": "hyundai",
    "santa fe": "hyundai",
    "creta": "hyundai",

    "rio": "kia",
    "cerato": "kia",
    "ceed": "kia",
    "k5": "kia",
    "optima": "kia",
    "sportage": "kia",
    "sorento": "kia",

    "civic": "honda",
    "accord": "honda",
    "cr v": "honda",
    "cr-v": "honda",
    "pilot": "honda",

    "mazda 3": "mazda",
    "mazda3": "mazda",
    "mazda 6": "mazda",
    "mazda6": "mazda",
    "cx 5": "mazda",
    "cx-5": "mazda",

    "qashqai": "nissan",
    "x trail": "nissan",
    "x-trail": "nissan",
    "teana": "nissan",
    "patrol": "nissan",

    "rx": "lexus",
    "rx350": "lexus",
    "nx": "lexus",
    "lx": "lexus",

    "range rover": "land_rover",
    "range rover sport": "land_rover",
    "evoque": "land_rover",
    "velar": "land_rover",
    "discovery": "land_rover",
}


def _normalize_text(text: str) -> str:
    text = text or ""
    text = text.replace("\u00A0", " ")
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
    if not text:
        return None, 0.0

    text_norm = _normalize_text(text)

    # 1. exact phrase / token from brands.yaml
    best_brand = None
    best_score = 0.0
    best_len = 0

    for brand_key, cfg in BRANDS.items():
        candidates = _build_candidates(cfg)

        for candidate in candidates:
            if _contains_phrase(text_norm, candidate):
                score = 1.0 if candidate in [_normalize_text(x) for x in cfg.get("en", []) + cfg.get("ru", [])] else 0.9
                clen = len(candidate)

                if clen > best_len or (clen == best_len and score > best_score):
                    best_brand = str(brand_key).lower()
                    best_score = score
                    best_len = clen

    if best_brand:
        return best_brand, best_score

    # 2. fallback by strong model tokens
    best_model_brand = None
    best_model_len = 0

    for model_token, brand in MODEL_BRAND_MAP.items():
        token_norm = _normalize_text(model_token)
        if _contains_phrase(text_norm, token_norm):
            if len(token_norm) > best_model_len:
                best_model_brand = brand
                best_model_len = len(token_norm)

    if best_model_brand:
        return best_model_brand, 0.82

    return None, 0.0


def detect_brand(title: str = "", text: str = "") -> Tuple[Optional[str], float]:
    """
    Production brand detection priority:
    1) title exact/phrase
    2) full text exact/phrase
    3) title model fallback
    4) text model fallback
    """
    title = (title or "").strip()
    text = (text or "").strip()

    brand, conf = _match_brand(title)
    if brand:
        return brand, conf

    brand, conf = _match_brand(text)
    if brand:
        return brand, min(conf, 0.9)

    return None, 0.0