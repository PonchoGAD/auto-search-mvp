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

    # ФИКС раскладки: х5 -> x5
    homoglyphs = {
        'а': 'a', 'в': 'b', 'с': 'c',
        'е': 'e', 'н': 'h', 'к': 'k',
        'м': 'm', 'о': 'o', 'р': 'p',
        'т': 't', 'х': 'x', 'у': 'y'
    }

    for cyr, lat in homoglyphs.items():
        text = re.sub(
            rf"\b{cyr}(?=\d|\-)",
            lat,
            text
        )

        text = re.sub(
            rf"(?<=\d|\-){cyr}\b",
            lat,
            text
        )

    text = re.sub(
        r"[-_/]+",
        " ",
        text
    )

    text = re.sub(
        r"([a-zа-яё])(\d)",
        r"\1 \2",
        text,
        flags=re.IGNORECASE
    )

    text = re.sub(
        r"(\d)([a-zа-яё])",
        r"\1 \2",
        text,
        flags=re.IGNORECASE
    )

    text = re.sub(
        r"\s+",
        " ",
        text
    )

    return text.strip()


def _to_key(text: str) -> str:
    text = _norm_text(text)
    text = text.replace(" ", "_")
    return text


def _compact_model_text(text: str) -> str:
    text = text or ""
    text = text.lower().strip()

    text = text.replace(
        "\u00A0",
        " "
    ).replace(
        "\xa0",
        " "
    )

    # x 5 -> x5
    text = re.sub(
        r"([a-zа-я]+)\s+(\d+)",
        r"\1\2",
        text,
        flags=re.IGNORECASE
    )

    # gx 460 -> gx460
    text = re.sub(
        r"([a-zа-я]+)\s+(\d+)",
        r"\1\2",
        text,
        flags=re.IGNORECASE
    )

    # e 200 -> e200
    text = re.sub(
        r"([a-zа-я]+)\s+(\d+)",
        r"\1\2",
        text,
        flags=re.IGNORECASE
    )

    # cx 5 -> cx5
    text = re.sub(
        r"([a-zа-я]+)[\-\s]+(\d+)",
        r"\1\2",
        text,
        flags=re.IGNORECASE
    )

    text = text.replace(
        "x trail",
        "xtrail"
    )

    text = text.replace(
        "cr v",
        "crv"
    )

    text = re.sub(
        r"[-_\s]+",
        "",
        text
    )

    return text


def _alias_variants(value: str) -> Set[str]:
    variants: Set[str] = set()

    norm = _norm_text(value)

    if not norm:
        return variants

    variants.add(norm)

    compact = _compact_model_text(norm)

    if compact:
        variants.add(compact)

    spaced = re.sub(
        r"([a-zа-яё]+)(\d+)",
        r"\1 \2",
        norm,
        flags=re.IGNORECASE
    )

    spaced = re.sub(
        r"(\d+)([a-zа-яё]+)",
        r"\1 \2",
        spaced,
        flags=re.IGNORECASE
    )

    spaced = re.sub(
        r"\s+",
        " ",
        spaced
    ).strip()

    if spaced:
        variants.add(spaced)

    joined = spaced.replace(
        " ",
        ""
    )

    if joined:
        variants.add(joined)

    hyphen = norm.replace(
        " ",
        "-"
    )

    if hyphen:
        variants.add(hyphen)

    underscore = norm.replace(
        " ",
        "_"
    )

    if underscore:
        variants.add(underscore)

    compact2 = _compact_model_text(
        norm
    )

    if compact2:
        variants.add(compact2)

    spaced2 = re.sub(
        r"([a-zа-я]+)(\d+)",
        r"\1 \2",
        compact2,
        flags=re.IGNORECASE
    )

    if spaced2:
        variants.add(spaced2)

    return {
        v for v in variants if v
    }


def _phrase_pattern(value: str) -> str:
    value = _norm_text(value)

    if not value:
        return ""

    parts = [
        re.escape(p)
        for p in value.split()
        if p.strip()
    ]

    if not parts:
        return ""

    return (
        r"(?<![a-zа-яё0-9])"
        + r"[\s\-_]*".join(parts)
        + r"(?![a-zа-яё0-9])"
    )


def _contains_phrase(
        text: str,
        phrase: str
) -> bool:

    pattern = _phrase_pattern(
        phrase
    )

    if not pattern:
        return False

    return bool(
        re.search(
            pattern,
            text,
            re.IGNORECASE
        )
    )


AMBIGUOUS_MODEL_ALIASES = {
    "x1","x2","x3","x4",
    "x5","x6","x7",
    "ix",
    "glc","gle","gls"
}

MODEL_STOPWORDS = {

    "год","года","лет",
    "цена","пробег",
    "км","руб","р",

    "до","от","с",
    "не","старше",
    "ниже","после",

    "дизель",
    "бензин",
    "гибрид",
    "электро",

    "bmw",
    "audi",
    "toyota",
    "lexus",
    "mercedes",
    "kia",
    "hyundai",
    "honda",
    "nissan",
    "mazda",
    "geely"
}


class TaxonomyService:

    def __init__(self)->None:

        base_dir=Path(
            __file__
        ).resolve().parent.parent

        self.brands_path=(
            base_dir/
            "config"/
            "brands.yaml"
        )

        self.models_path=(
            base_dir/
            "config"/
            "models.yaml"
        )

        self.brands_raw=self._load_brands()
        self.models_raw=self._load_models()

        self.brand_alias_to_canonical={}
        self.brand_to_aliases={}

        self.brand_model_alias_to_canonical={}
        self.brand_model_to_aliases={}

        self.global_model_alias_to_brands={}

        self._build_brand_index()
        self._build_model_index()

    # весь существующий код оставляем

    def canonicalize_brand(
        self,
        brand: Optional[str]
    )->Optional[str]:

        if not brand:
            return None

        norm = _norm_text(
            brand
        )

        compact = _compact_model_text(
            brand
        )

        if norm in self.brand_alias_to_canonical:
            return self.brand_alias_to_canonical[norm]

        if compact in self.brand_alias_to_canonical:
            return self.brand_alias_to_canonical[compact]

        return self.brand_alias_to_canonical.get(
            norm,
            _to_key(brand)
        )

    def resolve_model(
        self,
        brand: Optional[str],
        text: str
    )->Optional[str]:

        text_norm=_norm_text(text)
        text_compact=_compact_model_text(text)

        if not brand:
            return None

        canonical_brand=(
            self.canonicalize_brand(
                brand
            )
        )

        model_map=(
            self.brand_model_alias_to_canonical.get(
                canonical_brand,
                {}
            )
        )

        best_model=None
        best_score=-1

        for alias,canonical_model in model_map.items():

            alias_norm=_norm_text(
                alias
            )

            if alias_norm in MODEL_STOPWORDS:
                continue

            if alias_norm in self.brand_alias_to_canonical:
                continue

            matched=False
            score=0

            alias_compact=(
                _compact_model_text(
                    alias_norm
                )
            )

            if (
                alias_compact
                and
                alias_compact
                in
                text_compact
            ):

                matched=True
                score=50

            if (
                not matched
                and
                _contains_phrase(
                    text_norm,
                    alias_norm
                )
            ):

                matched=True
                score=len(
                    alias_norm
                )

            if not matched:
                continue

            if score>best_score:
                best_score=score
                best_model=canonical_model

        return best_model

    def canonicalize_model(
        self,
        brand: Optional[str],
        model: Optional[str]
    )->Optional[str]:

        if not brand or not model:
            return None

        canonical_brand=(
            self.canonicalize_brand(
                brand
            )
        )

        model_norm=(
            _norm_text(model)
        )

        model_map=(
            self.brand_model_alias_to_canonical.get(
                canonical_brand,
                {}
            )
        )

        compact_model=(
            _compact_model_text(
                model
            )
        )

        for variant in _alias_variants(
            compact_model
        ):

            resolved=model_map.get(
                variant
            )

            if resolved:
                return resolved

        for variant in _alias_variants(
            model_norm
        ):

            resolved=model_map.get(
                variant
            )

            if resolved:
                return resolved

        return _to_key(model)

    def debug_resolve(
        self,
        text:str
    )->dict:

        brand,model,conf=(
            self.resolve_entities(
                text
            )
        )

        return {

            "input":text,

            "brand":brand,

            "model":model,

            "confidence":conf,

            "brand_aliases":
                self.get_brand_aliases(
                    brand
                ) if brand else[],

            "model_aliases":
                self.get_model_aliases(
                    brand,
                    model
                )
                if brand and model
                else[]
        }


taxonomy_service=TaxonomyService()