from typing import Optional, Tuple

from services.taxonomy_service import taxonomy_service


AMBIGUOUS_MODEL_BRAND_TOKENS = {
    "x1", "x2", "x3", "x4", "x5", "x6", "x7",
    "1 series", "2 series", "3 series", "4 series", "5 series", "6 series", "7 series",
    "glc", "gle", "gls",
    "rx", "nx", "es", "is", "ls",
    "a3", "a4", "a6", "a8", "q3", "q5", "q7", "q8",
}


def detect_brand(title: str = "", text: str = "") -> Tuple[Optional[str], float]:
    """
    Thin wrapper over taxonomy_service.
    Priority:
    1) title direct brand/model resolution
    2) text direct brand/model resolution
    """
    title = (title or "").strip()
    text = (text or "").strip()

    if title:
        brand, conf = taxonomy_service.resolve_brand(title)
        if brand:
            return brand, conf

        brand, conf = taxonomy_service.resolve_brand_from_model(title)
        if brand:
            return brand, conf

    if text:
        brand, conf = taxonomy_service.resolve_brand(text)
        if brand:
            return brand, conf

        brand, conf = taxonomy_service.resolve_brand_from_model(text)
        if brand:
            return brand, conf

    return None, 0.0