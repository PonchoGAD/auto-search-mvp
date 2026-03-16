from typing import Optional

from services.taxonomy_service import taxonomy_service


def resolve_model(brand: Optional[str], text: str) -> Optional[str]:
    """
    Returns canonical model key for the given canonical brand and raw text.
    This adapter contains no model resolution logic of its own and only delegates
    to taxonomy_service.
    """
    return taxonomy_service.resolve_model(brand, text)