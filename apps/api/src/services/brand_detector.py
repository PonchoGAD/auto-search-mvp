# apps/api/src/services/brand_detector.py

from typing import Optional, Tuple

from services.taxonomy_service import taxonomy_service


def detect_brand(title: str = "", text: str = "") -> Tuple[Optional[str], float]:
    """
    Thin wrapper over taxonomy_service.

    Priority:
    1) title direct brand resolution
    2) text direct brand resolution
    3) title model -> brand fallback
    4) text model -> brand fallback
    """

    title = (title or "").strip()
    text = (text or "").strip()

    brand, conf = taxonomy_service.resolve_brand(title)
    if brand:
        return brand, conf

    brand, conf = taxonomy_service.resolve_brand(text)
    if brand:
        return brand, conf

    brand, conf = taxonomy_service.resolve_brand_from_model(title)
    if brand:
        return brand, conf

    brand, conf = taxonomy_service.resolve_brand_from_model(text)
    if brand:
        return brand, conf

    return None, 0.0