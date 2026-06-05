from typing import Optional, Tuple

from services.taxonomy_service import taxonomy_service


def _clean(v: str | None) -> str:
    if not isinstance(v, str):
        return ""
    return v.strip()


def detect_brand(title: str = "", text: str = "") -> Tuple[Optional[str], float]:
    """
    Public canonical brand detector.

    Contract:
    - single public signature for the whole project:
        detect_brand(title: str = "", text: str = "") -> tuple[str | None, float]
    - returns only canonical brand key from taxonomy_service
    - taxonomy_service is the only source of truth
    - no local taxonomy logic here

    Resolution order:
    1) title direct brand resolution
    2) title model->brand resolution
    3) text direct brand resolution
    4) text model->brand resolution
    """

    title = _clean(title)
    text = _clean(text)

    if title:
        brand, conf = taxonomy_service.resolve_brand(title)
        if brand:
            return brand, float(conf or 0.0)

        brand, model = taxonomy_service.maybe_resolve_brand_from_model(title)
        if brand:
            return brand, 0.82

    if text:
        brand, conf = taxonomy_service.resolve_brand(text)
        if brand:
            return brand, float(conf or 0.0)

        brand, model = taxonomy_service.maybe_resolve_brand_from_model(text)
        if brand:
            return brand, 0.82

    return None, 0.0