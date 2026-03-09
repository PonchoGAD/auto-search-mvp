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

    candidates: List[Tuple[int, int, str]] = []

    for model, aliases in brand_models.items():

        if not isinstance(model, str):
            continue

        values = [model]

        if isinstance(aliases, list):
            values.extend([a for a in aliases if isinstance(a, str)])

        best_hit_len = 0
        best_priority = 0

        for idx, value in enumerate(values):
            pattern = _make_pattern(value)

            if not pattern:
                continue

            if re.search(pattern, text_norm, re.IGNORECASE):
                hit_len = len(_normalize_spaces(value))
                priority = 2 if idx == 0 else 1

                if hit_len > best_hit_len or (hit_len == best_hit_len and priority > best_priority):
                    best_hit_len = hit_len
                    best_priority = priority

        if best_hit_len > 0:
            candidates.append((best_priority, best_hit_len, model))

    if not candidates:
        return None

    # сначала приоритет точного имени модели, потом длина совпадения
    candidates.sort(key=lambda x: (x[0], x[1]), reverse=True)

    return candidates[0][2]