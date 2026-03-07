import yaml
from pathlib import Path

def load_models():

    base = Path(__file__).resolve().parent.parent
    path = base / "config" / "models.yaml"

    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


MODELS = load_models()


def resolve_model(brand, text):

    if not brand:
        return None

    brand_models = MODELS.get(brand, {})

    text = text.lower()

    for model, aliases in brand_models.items():

        if model in text:
            return model

        for a in aliases:
            if a in text:
                return model

    return None