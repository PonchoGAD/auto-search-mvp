# apps/api/src/services/taxonomy_service.py

from __future__ import annotations

import re
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple

import yaml


def _norm_text(text: str) -> str:
    text = text or ""
    text = text.replace("\u00A0", " ")
    text = text.replace("\xa0", " ")
    text = text.lower().strip()
    text = re.sub(r"[-_/]+", " ", text)
    text = re.sub(r"\s+", " ", text)
    return text


def _to_key(text: str) -> str:
    text = _norm_text(text)
    text = text.replace(" ", "_")
    return text


def _phrase_pattern(value: str) -> str:
    value = _norm_text(value)
    parts = [re.escape(p) for p in value.split() if p.strip()]
    if not parts:
        return ""
    return r"\b" + r"[\s\-_]*".join(parts) + r"\b"


def _contains_phrase(text: str, phrase: str) -> bool:
    pattern = _phrase_pattern(phrase)
    if not pattern:
        return False
    return bool(re.search(pattern, text, re.IGNORECASE))


AMBIGUOUS_MODEL_ALIASES = {
    "x1", "x2", "x3", "x4", "x5", "x6", "x7",
    "1 series", "2 series", "3 series", "4 series", "5 series", "6 series", "7 series",
    "glc", "gle", "gls",
    "rx", "nx", "es", "is", "ls",
    "a3", "a4", "a6", "a8", "q3", "q5", "q7", "q8",
}


class TaxonomyService:
    """
    Single source of truth for:
    - brand aliases -> canonical brand
    - model aliases -> canonical model within brand
    - global model alias -> possible brands
    """

    def __init__(self) -> None:
        base_dir = Path(__file__).resolve().parent.parent
        self.brands_path = base_dir / "config" / "brands.yaml"
        self.models_path = base_dir / "config" / "models.yaml"

        self.brands_raw = self._load_brands()
        self.models_raw = self._load_models()

        self.brand_alias_to_canonical: Dict[str, str] = {}
        self.brand_to_aliases: Dict[str, Set[str]] = {}

        self.brand_model_alias_to_canonical: Dict[str, Dict[str, str]] = {}
        self.brand_model_to_aliases: Dict[str, Dict[str, Set[str]]] = {}

        self.global_model_alias_to_brands: Dict[str, Set[str]] = {}

        self._build_brand_index()
        self._build_model_index()

    def _load_brands(self) -> Dict[str, Dict[str, List[str]]]:
        try:
            with open(self.brands_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            brands = data.get("brands", {})
            return brands if isinstance(brands, dict) else {}
        except Exception as e:
            print(f"[TAXONOMY][WARN] failed to load brands.yaml: {e}", flush=True)
            return {}

    def _load_models(self) -> Dict[str, Dict[str, List[str]]]:
        try:
            with open(self.models_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}
            return data if isinstance(data, dict) else {}
        except Exception as e:
            print(f"[TAXONOMY][WARN] failed to load models.yaml: {e}", flush=True)
            return {}

    def _build_brand_index(self) -> None:
        for brand_key, cfg in self.brands_raw.items():
            canonical_brand = _to_key(str(brand_key))
            aliases: Set[str] = set()

            aliases.add(_norm_text(canonical_brand))
            aliases.add(_norm_text(str(brand_key)))

            if isinstance(cfg, dict):
                for field in ("en", "ru", "aliases"):
                    items = cfg.get(field, [])
                    if isinstance(items, list):
                        for item in items:
                            if isinstance(item, str) and item.strip():
                                aliases.add(_norm_text(item))

            self.brand_to_aliases[canonical_brand] = aliases

            for alias in aliases:
                existing = self.brand_alias_to_canonical.get(alias)
                if existing and existing != canonical_brand:
                    print(
                        f"[TAXONOMY][WARN] duplicate brand alias '{alias}' "
                        f"for '{existing}' and '{canonical_brand}'",
                        flush=True,
                    )
                    continue
                self.brand_alias_to_canonical[alias] = canonical_brand

    def _build_model_index(self) -> None:
        for raw_brand, models in self.models_raw.items():
            canonical_brand = self.brand_alias_to_canonical.get(
                _norm_text(raw_brand),
                _to_key(raw_brand),
            )

            if canonical_brand not in self.brand_model_alias_to_canonical:
                self.brand_model_alias_to_canonical[canonical_brand] = {}
            if canonical_brand not in self.brand_model_to_aliases:
                self.brand_model_to_aliases[canonical_brand] = {}

            if not isinstance(models, dict):
                continue

            for raw_model, aliases in models.items():
                canonical_model = _to_key(raw_model)
                model_aliases: Set[str] = set()

                raw_model_norm = _norm_text(raw_model)
                canonical_model_norm = _norm_text(canonical_model)

                if raw_model_norm and not raw_model_norm.isdigit():
                    model_aliases.add(raw_model_norm)
                if canonical_model_norm and not canonical_model_norm.isdigit():
                    model_aliases.add(canonical_model_norm)

                if isinstance(aliases, list):
                    for alias in aliases:
                        if not isinstance(alias, str):
                            continue
                        alias_norm = _norm_text(alias)
                        if not alias_norm:
                            continue

                        alias_parts = alias_norm.split()
                        if len(alias_parts) == 1 and alias_norm.isdigit():
                            continue
                        if len(alias_norm) <= 2 and not re.search(r"[a-zа-яё]", alias_norm, re.IGNORECASE):
                            continue

                        model_aliases.add(alias_norm)

                self.brand_model_to_aliases[canonical_brand][canonical_model] = model_aliases

                for alias in model_aliases:
                    existing = self.brand_model_alias_to_canonical[canonical_brand].get(alias)
                    if existing and existing != canonical_model:
                        print(
                            f"[TAXONOMY][WARN] duplicate model alias '{alias}' "
                            f"for brand '{canonical_brand}': '{existing}' vs '{canonical_model}'",
                            flush=True,
                        )
                        continue

                    self.brand_model_alias_to_canonical[canonical_brand][alias] = canonical_model
                    self.global_model_alias_to_brands.setdefault(alias, set()).add(canonical_brand)

    def resolve_brand(self, text: str) -> Tuple[Optional[str], float]:
        text_norm = _norm_text(text)
        if not text_norm:
            return None, 0.0

        best_brand: Optional[str] = None
        best_len = 0

        for alias, canonical_brand in self.brand_alias_to_canonical.items():
            if _contains_phrase(text_norm, alias):
                alias_len = len(alias)
                if alias_len > best_len:
                    best_brand = canonical_brand
                    best_len = alias_len

        if best_brand:
            confidence = min(1.0, 0.95 + best_len / 100.0)
            return best_brand, confidence

        return None, 0.0

    def canonicalize_brand(self, brand: Optional[str]) -> Optional[str]:
        if not brand:
            return None
        return self.brand_alias_to_canonical.get(_norm_text(brand), _to_key(brand))

    def resolve_model(self, brand: Optional[str], text: str) -> Optional[str]:
        if not brand:
            return None

        canonical_brand = self.brand_alias_to_canonical.get(_norm_text(brand), _to_key(brand))
        text_norm = _norm_text(text)
        if not text_norm:
            return None

        model_map = self.brand_model_alias_to_canonical.get(canonical_brand, {})
        if not model_map:
            return None

        best_model: Optional[str] = None
        best_len = 0
        best_is_canonical = 0

        canonical_models = set(self.brand_model_to_aliases.get(canonical_brand, {}).keys())

        for alias, canonical_model in model_map.items():
            alias_parts = alias.split()
            if len(alias_parts) == 1 and alias.isdigit():
                continue

            if _contains_phrase(text_norm, alias):
                alias_len = len(alias)
                is_canonical = 1 if _norm_text(canonical_model) == alias else 0

                score = alias_len * 10 + is_canonical
                best_score = best_len * 10 + best_is_canonical

                if score > best_score:
                    best_model = canonical_model
                    best_len = alias_len
                    best_is_canonical = is_canonical

        if best_model in canonical_models:
            return best_model

        return best_model

    def resolve_entities(self, text: str) -> Tuple[Optional[str], Optional[str], float]:
        brand, conf = self.resolve_brand(text)
        if brand:
            model = self.resolve_model(brand, text)
            return brand, model, conf

        fallback_brand, fallback_conf = self.resolve_brand_from_model(text)
        if fallback_brand:
            model = self.resolve_model(fallback_brand, text)
            return fallback_brand, model, fallback_conf

        return None, None, 0.0

    def canonicalize_model(self, brand: Optional[str], model: Optional[str]) -> Optional[str]:
        if not brand or not model:
            return None

        canonical_brand = self.canonicalize_brand(brand)
        if not canonical_brand:
            return None

        model_norm = _norm_text(model)
        model_map = self.brand_model_alias_to_canonical.get(canonical_brand, {})

        resolved = model_map.get(model_norm)
        if resolved:
            return resolved

        return _to_key(model)

    def resolve_brand_from_model(self, text: str) -> Tuple[Optional[str], float]:
        text_norm = _norm_text(text)
        if not text_norm:
            return None, 0.0

        best_brand: Optional[str] = None
        best_len = 0

        for alias, brands in self.global_model_alias_to_brands.items():
            if len(brands) != 1:
                continue

            if alias in AMBIGUOUS_MODEL_ALIASES:
                continue

            if _contains_phrase(text_norm, alias):
                alias_len = len(alias)
                if alias_len > best_len:
                    best_brand = list(brands)[0]
                    best_len = alias_len

        if best_brand:
            confidence = min(0.88, 0.82 + best_len / 100.0)
            return best_brand, confidence

        return None, 0.0

    def get_brand_aliases(self, brand: str) -> List[str]:
        canonical_brand = self.brand_alias_to_canonical.get(_norm_text(brand), _to_key(brand))
        return sorted(self.brand_to_aliases.get(canonical_brand, set()))

    def get_model_aliases(self, brand: str, model: str) -> List[str]:
        canonical_brand = self.brand_alias_to_canonical.get(_norm_text(brand), _to_key(brand))
        canonical_model = _to_key(model)
        return sorted(
            self.brand_model_to_aliases.get(canonical_brand, {}).get(canonical_model, set())
        )


taxonomy_service = TaxonomyService()