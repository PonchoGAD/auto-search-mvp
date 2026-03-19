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
    text = re.sub(r"([a-zа-яё])(\d)", r"\1 \2", text, flags=re.IGNORECASE)
    text = re.sub(r"(\d)([a-zа-яё])", r"\1 \2", text, flags=re.IGNORECASE)
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _to_key(text: str) -> str:
    text = _norm_text(text)
    text = text.replace(" ", "_")
    return text


def _compact_model_text(text: str) -> str:
    text = text or ""
    text = text.lower().strip()
    text = text.replace("\u00A0", " ").replace("\xa0", " ")
    text = re.sub(r"[-_\s]+", "", text)
    return text


def _alias_variants(value: str) -> Set[str]:
    """
    Генерирует безопасные варианты для моделей:
    - x5 <-> x 5
    - gx460 <-> gx 460
    - e200 <-> e 200
    - x-trail <-> x trail
    - cx-5 <-> cx 5
    """
    variants: Set[str] = set()

    norm = _norm_text(value)
    if not norm:
        return variants

    variants.add(norm)

    compact = _compact_model_text(norm)
    if compact:
        variants.add(compact)

    spaced = re.sub(r"([a-zа-яё]+)(\d+)", r"\1 \2", norm, flags=re.IGNORECASE)
    spaced = re.sub(r"(\d+)([a-zа-яё]+)", r"\1 \2", spaced, flags=re.IGNORECASE)
    spaced = re.sub(r"\s+", " ", spaced).strip()
    if spaced:
        variants.add(spaced)

    joined = spaced.replace(" ", "")
    if joined:
        variants.add(joined)

    return {v for v in variants if v}


def _phrase_pattern(value: str) -> str:
    value = _norm_text(value)
    if not value:
        return ""

    parts =[re.escape(p) for p in value.split() if p.strip()]
    if not parts:
        return ""

    return r"(?<![a-zа-яё0-9])" + r"[\s\-_]*".join(parts) + r"(?![a-zа-яё0-9])"


def _contains_phrase(text: str, phrase: str) -> bool:
    pattern = _phrase_pattern(phrase)
    if not pattern:
        return False
    return bool(re.search(pattern, text, re.IGNORECASE))


AMBIGUOUS_MODEL_ALIASES = {
    "x1", "x2", "x3", "x4", "x5", "x6", "x7",
    "ix",
    "1 series", "2 series", "3 series", "4 series", "5 series", "6 series", "7 series",
    "glc", "gle", "gls",
    "rx", "nx", "es", "is", "ls",
    "a3", "a4", "a6", "a8", "q3", "q5", "q7", "q8",
}

MODEL_STOPWORDS = {
    "год", "года", "лет", "цена", "пробег", "км", "руб", "р", "₽",
    "до", "от", "с", "не", "старше", "ниже", "после", "дизель",
    "бензин", "гибрид", "электро",
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

    def normalize_text(self, text: str) -> str:
        return _norm_text(text)

    def _has_explicit_other_brand(self, text: str, excluded_brand: Optional[str]) -> bool:
        text_norm = _norm_text(text)
        if not text_norm:
            return False

        excluded_brand = self.canonicalize_brand(excluded_brand) if excluded_brand else None

        for alias, canonical_brand in self.brand_alias_to_canonical.items():
            if excluded_brand and canonical_brand == excluded_brand:
                continue
            if _contains_phrase(text_norm, alias):
                return True

        return False

    def _load_brands(self) -> Dict[str, Dict[str, List[str]]]:
        try:
            with open(self.brands_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}

            if isinstance(data, dict) and "brands" in data:
                brands = data.get("brands", {})
                return brands if isinstance(brands, dict) else {}

            return data if isinstance(data, dict) else {}
        except Exception as e:
            print(f"[TAXONOMY][WARN] failed to load brands.yaml: {e}", flush=True)
            return {}

    def _load_models(self) -> Dict[str, Dict[str, List[str]]]:
        try:
            with open(self.models_path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f) or {}

            if isinstance(data, dict) and "models" in data:
                models = data.get("models", {})
                return models if isinstance(models, dict) else {}

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
                    items = cfg.get(field,[])
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

                if raw_model_norm and raw_model_norm not in MODEL_STOPWORDS:
                    model_aliases.update(_alias_variants(raw_model_norm))

                if canonical_model_norm and canonical_model_norm not in MODEL_STOPWORDS:
                    model_aliases.update(_alias_variants(canonical_model_norm))

                if isinstance(aliases, list):
                    for alias in aliases:
                        if not isinstance(alias, str):
                            continue

                        alias_norm = _norm_text(alias)
                        if not alias_norm:
                            continue

                        if alias_norm in MODEL_STOPWORDS:
                            continue

                        if alias_norm.isdigit():
                            continue

                        model_aliases.update(_alias_variants(alias_norm))

                cleaned_aliases = set()
                for alias in model_aliases:
                    if not alias:
                        continue
                    if alias.isdigit():
                        continue
                    if len(alias) <= 1:
                        continue
                    cleaned_aliases.add(alias)

                self.brand_model_to_aliases[canonical_brand][canonical_model] = cleaned_aliases

                for alias in cleaned_aliases:
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

        # 🔥 fallback через модель (очень важно для UX)
        fallback_brand, _ = self.maybe_resolve_brand_from_model(text)
        if fallback_brand:
            return fallback_brand, 0.75

        return None, 0.0

    def canonicalize_brand(self, brand: Optional[str]) -> Optional[str]:
        if not brand:
            return None
        return self.brand_alias_to_canonical.get(_norm_text(brand), _to_key(brand))

    def resolve_model(self, brand: Optional[str], text: str) -> Optional[str]:
        text_norm = _norm_text(text)
        text_compact = _compact_model_text(text)

        if not text_norm:
            return None

        if brand:
            canonical_brand = self.canonicalize_brand(brand)
            if not canonical_brand:
                return None

            model_map = self.brand_model_alias_to_canonical.get(canonical_brand, {})
            if not model_map:
                return None

            best_model: Optional[str] = None
            best_score = -1

            canonical_models = set(self.brand_model_to_aliases.get(canonical_brand, {}).keys())

            for alias, canonical_model in model_map.items():
                alias_norm = _norm_text(alias)
                if not alias_norm:
                    continue
                if alias_norm in MODEL_STOPWORDS:
                    continue

                matched = False
                match_score = 0

                if _contains_phrase(text_norm, alias_norm):
                    matched = True
                    match_score = len(alias_norm) * 10

                alias_compact = _compact_model_text(alias_norm)
                if not matched and alias_compact and len(alias_compact) >= 2:
                    if alias_compact in text_compact:
                        if re.search(_phrase_pattern(alias_norm), text_norm, re.IGNORECASE) or len(alias_compact) >= 4:
                            matched = True
                            match_score = len(alias_compact) * 8

                if not matched:
                    continue

                is_canonical_alias = 1 if _norm_text(canonical_model) == alias_norm else 0
                total_score = match_score + is_canonical_alias

                if total_score > best_score:
                    best_model = canonical_model
                    best_score = total_score

            if best_model in canonical_models:
                return best_model

            return best_model

        fallback_brand, fallback_model = self.maybe_resolve_brand_from_model(text)
        if fallback_brand and fallback_model:
            return fallback_model

        return None

    def maybe_resolve_brand_from_model(self, text: str) -> Tuple[Optional[str], Optional[str]]:
        text_norm = _norm_text(text)
        if not text_norm:
            return None, None

        best_alias_len = 0
        candidates: List[Tuple[str, str, int]] =[]

        for alias, brands in self.global_model_alias_to_brands.items():
            alias_norm = _norm_text(alias)
            if not alias_norm:
                continue

            if not _contains_phrase(text_norm, alias_norm):
                continue

            if alias_norm in AMBIGUOUS_MODEL_ALIASES:
                continue

            alias_len = len(alias_norm)
            if alias_len < 3:
                continue

            if alias_len < best_alias_len:
                continue

            matched_brands = sorted(brands)
            if len(matched_brands) != 1:
                continue

            brand = matched_brands[0]
            model = self.brand_model_alias_to_canonical.get(brand, {}).get(alias_norm)
            if not model:
                continue

            # ❗ если в тексте уже есть другой явный бренд  не восстанавливаем бренд по модели
            if self._has_explicit_other_brand(text_norm, brand):
                continue

            if alias_len > best_alias_len:
                best_alias_len = alias_len
                candidates =[(brand, model, alias_len)]
            elif alias_len == best_alias_len:
                candidates.append((brand, model, alias_len))

        if not candidates:
            return None, None

        unique_pairs = {(brand, model) for brand, model, _ in candidates}
        if len(unique_pairs) != 1:
            return None, None

        brand, model = next(iter(unique_pairs))
        return brand, model

    def resolve_entities(self, text: str) -> Tuple[Optional[str], Optional[str], float]:
        brand, conf = self.resolve_brand(text)
        if brand:
            model = self.resolve_model(brand, text)
            return brand, model, conf

        fallback_brand, fallback_model = self.maybe_resolve_brand_from_model(text)
        if fallback_brand:
            return fallback_brand, fallback_model, 0.82

        return None, None, 0.0

    def canonicalize_model(self, brand: Optional[str], model: Optional[str]) -> Optional[str]:
        if not brand or not model:
            return None

        canonical_brand = self.canonicalize_brand(brand)
        if not canonical_brand:
            return None

        model_norm = _norm_text(model)
        model_map = self.brand_model_alias_to_canonical.get(canonical_brand, {})

        if model_norm in model_map:
            return model_map[model_norm]

        for variant in _alias_variants(model_norm):
            resolved = model_map.get(variant)
            if resolved:
                return resolved

        return _to_key(model)

    def get_brand_aliases(self, brand: str) -> List[str]:
        canonical_brand = self.brand_alias_to_canonical.get(_norm_text(brand), _to_key(brand))
        return sorted(self.brand_to_aliases.get(canonical_brand, set()))

    def get_model_aliases(self, brand: str, model: str) -> List[str]:
        canonical_brand = self.brand_alias_to_canonical.get(_norm_text(brand), _to_key(brand))
        canonical_model = self.canonicalize_model(canonical_brand, model) or _to_key(model)
        return sorted(
            self.brand_model_to_aliases.get(canonical_brand, {}).get(canonical_model, set())
        )


taxonomy_service = TaxonomyService()