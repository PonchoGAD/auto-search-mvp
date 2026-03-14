# apps/api/src/services/model_resolver.py

from typing import Optional

from services.taxonomy_service import taxonomy_service


def resolve_model(brand: Optional[str], text: str) -> Optional[str]:
    """
    Thin wrapper over taxonomy_service.
    """

    if not brand:
        return None

    brand = str(brand).strip().lower()

    if not brand or brand == "unknown":
        return None

    return taxonomy_service.resolve_model(brand, text or "")