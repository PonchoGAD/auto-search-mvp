import yaml
from pathlib import Path
import re


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

    models = list(brand_models.keys())

    models = sorted(models, key=len, reverse=True)

    for model in models:

        if re.search(r'\b' + re.escape(model) + r'\b', text):
            return model

        aliases = brand_models.get(model, [])

        for a in aliases:
            if a in text:
                return model

    return None