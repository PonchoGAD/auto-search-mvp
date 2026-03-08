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

    for model, aliases in sorted(
        brand_models.items(),
        key=lambda x: len(x[0]),
        reverse=True
    ):

        if re.search(r"\b" + re.escape(model) + r"\b", text):
            return model

        for a in aliases:

            if re.search(r"\b" + re.escape(a) + r"\b", text):
                return model

    matches = []

    for m in brand_models:
        if m in text:
            matches.append(m)

    if not matches:
        return None

    matches.sort(key=len, reverse=True)

    return matches[0]