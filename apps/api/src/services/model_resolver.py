from typing import Optional

from services.taxonomy_service import taxonomy_service


def resolve_model(brand: Optional[str], text: str) -> Optional[str]:
    return taxonomy_service.resolve_model(brand, text)