# apps/api/src/services/brand_detector.py

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

        return data.get("brands", {})
    except Exception as e:
        print(f"[BRAND][WARN] brands.yaml load failed: {e}", flush=True)
        return {}


BRANDS = load_brands()


def _build_candidates(cfg: Dict[str, List[str]]) -> List[str]:
    values: List[str] = []
    for key in ("en", "ru", "aliases"):
        for v in cfg.get(key, []):
            if isinstance(v, str) and v.strip():
                values.append(v.strip().lower())
    return list(dict.fromkeys(values))


def _match_brand(text: str) -> Tuple[Optional[str], float]:
    if not text:
        return None, 0.0

    text = text.lower()
    tokens = re.findall(r"[a-zа-я0-9&\-]+", text)

    # 1. exact token/phrase
    for brand_key, cfg in BRANDS.items():
        candidates = _build_candidates(cfg)

        for candidate in candidates:
            candidate_tokens = re.findall(r"[a-zа-я0-9&\-]+", candidate)

            if not candidate_tokens:
                continue

            if len(candidate_tokens) == 1:
                if candidate_tokens[0] in tokens:
                    if candidate in cfg.get("en", []) or candidate in cfg.get("ru", []):
                        return brand_key.lower(), 1.0
                    return brand_key.lower(), 0.9
            else:
                phrase = " ".join(candidate_tokens)
                if phrase in " ".join(tokens):
                    if candidate in cfg.get("en", []) or candidate in cfg.get("ru", []):
                        return brand_key.lower(), 0.98
                    return brand_key.lower(), 0.88

    # 2. soft substring fallback
    for brand_key, cfg in BRANDS.items():
        candidates = _build_candidates(cfg)
        for candidate in candidates:
            if candidate and candidate in text:
                if candidate in cfg.get("en", []) or candidate in cfg.get("ru", []):
                    return brand_key.lower(), 0.8
                return brand_key.lower(), 0.7

    return None, 0.0


def detect_brand(title: str = "", text: str = "") -> Tuple[Optional[str], float]:
    """
    Production brand detection:
    1) title exact/phrase
    2) title soft
    3) full text fallback
    """
    title = (title or "").strip()
    text = (text or "").strip()

    brand, conf = _match_brand(title)
    if brand:
        return brand, conf

    brand, conf = _match_brand(text)
    if brand:
        return brand, min(conf, 0.85)

    for model, brand in MODEL_BRAND_MAP.items():
        if model in text.lower():
            return brand, 0.8

    lower = text.lower()

    for model, brand in MODEL_BRAND_MAP.items():
        if model in lower:
            return brand, 0.8

    return None, 0.0


MODEL_BRAND_MAP = {

    "camry": "toyota",
    "corolla": "toyota",
    "rav4": "toyota",
    "land cruiser": "toyota",

    "x5": "bmw",
    "x6": "bmw",
    "x3": "bmw",

    "c200": "mercedes",
    "e200": "mercedes",
    "gle": "mercedes",

    "solaris": "hyundai",
    "elantra": "hyundai",

    "sportage": "kia",
    "sorento": "kia",

    "insignia": "opel",
    "astra": "opel",

    "q5": "audi",
    "q7": "audi",
}