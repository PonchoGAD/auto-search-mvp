import re
from pathlib import Path
from typing import Optional, Dict, List, Tuple

import yaml


def load_models() -> Dict[str, Dict[str, List[str]]]:
    base = Path(__file__).resolve().parent.parent
    path = base / "config" / "models.yaml"

    try:
        with open(path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f) or {}
            return data if isinstance(data, dict) else {}
    except Exception as e:
        print(f"[MODEL][WARN] models.yaml load failed: {e}", flush=True)
        return {}


MODELS = load_models()


def _normalize_spaces(text: str) -> str:
    text = text or ""
    text = text.replace("\u00A0", " ")
    text = text.replace("\xa0", " ")
    text = re.sub(r"[-_/]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text.strip().lower()


def _make_pattern(value: str) -> str:
    value = _normalize_spaces(value)
    parts = [re.escape(p) for p in value.split()]
    if not parts:
        return ""
    return r"\b" + r"[\s\-_]*".join(parts) + r"\b"


def resolve_model(brand: Optional[str], text: str) -> Optional[str]:

    if not brand:
        return None

    brand = str(brand).lower().strip()

    if brand == "unknown":
        return None

    brand_models = MODELS.get(brand, {})

    if not isinstance(brand_models, dict) or not brand_models:
        return None

    text_norm = _normalize_spaces(text)

    # normalize class naming
    text_norm = text_norm.replace("class", "")

    candidates: List[Tuple[int, int, int, str]] = []

    for model, aliases in brand_models.items():

        if not isinstance(model, str) or not model.strip():
            continue

        model_norm = _normalize_spaces(model)
        pattern = _make_pattern(model_norm)

        if pattern and re.search(pattern, text_norm, re.IGNORECASE):
            hit_len = len(model_norm)
            priority = 3
            is_canonical = 1
            candidates.append((priority, hit_len, is_canonical, model))

        if isinstance(aliases, list):

            for alias in aliases:

                if not isinstance(alias, str):
                    continue

                alias_norm = _normalize_spaces(alias)

                if not alias_norm:
                    continue

                if len(alias_norm) < 2:
                    continue

                tokens = alias_norm.split()
                if len(tokens) == 1 and len(tokens[0]) < 2:
                    continue

                pattern = _make_pattern(alias_norm)

                if not pattern:
                    continue

                if re.search(pattern, text_norm, re.IGNORECASE):
                    hit_len = len(alias_norm)
                    priority = 2
                    is_canonical = 0
                    candidates.append((priority, hit_len, is_canonical, model))

    if not candidates:
        return None

    candidates.sort(
        key=lambda x: (x[0] * 100 + x[1] * 10 + x[2]),
        reverse=True
    )

    return candidates[0][3]