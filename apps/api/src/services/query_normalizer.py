# apps/api/src/services/query_normalizer.py

import re


QUERY_REPLACEMENTS = {
    r"\bбмв\b": "bmw",
    r"\bфв\b": "volkswagen",
    r"\bмерс\b": "mercedes",
    r"\bмерседес\b": "mercedes",
    r"\bтойота\b": "toyota",
    r"\bлексус\b": "lexus",
    r"\bниссан\b": "nissan",
    r"\bинфинити\b": "infiniti",
    r"\bхонда\b": "honda",
    r"\bмазда\b": "mazda",
    r"\bсубару\b": "subaru",
    r"\bмитсубиси\b": "mitsubishi",
    r"\bсузуки\b": "suzuki",
    r"\bфорд\b": "ford",
    r"\bшевроле\b": "chevrolet",
    r"\bкадиллак\b": "cadillac",
    r"\bджип\b": "jeep",
    r"\bдодж\b": "dodge",
    r"\bтесла\b": "tesla",
    r"\bауди\b": "audi",
    r"\bфольксваген\b": "volkswagen",
    r"\bпорше\b": "porsche",
    r"\bшкода\b": "skoda",
    r"\bрено\b": "renault",
    r"\bпежо\b": "peugeot",
    r"\bситроен\b": "citroen",
    r"\bвольво\b": "volvo",
    r"\bмини\b": "mini",
    r"\bленд ровер\b": "land rover",
    r"\bягуар\b": "jaguar",
    r"\bбентли\b": "bentley",
    r"\bроллс ройс\b": "rolls royce",
    r"\bферрари\b": "ferrari",
    r"\bламборгини\b": "lamborghini",
    r"\bмазерати\b": "maserati",
    r"\bчери\b": "chery",
    r"\bджили\b": "geely",
    r"\bхавал\b": "haval",
    r"\bэксид\b": "exeed",
    r"\bомода\b": "omoda",
    r"\bчанган\b": "changan",
    r"\bхендай\b": "hyundai",
    r"\bхьюндай\b": "hyundai",
    r"\bхундай\b": "hyundai",
    r"\bкиа\b": "kia",
    r"\bгенезис\b": "genesis",
    r"\bдэу\b": "daewoo",
    r"\bдеу\b": "daewoo",
    r"\bссангйонг\b": "ssangyong",
    r"\bсангйонг\b": "ssangyong",
}


def normalize_query(raw_text: str) -> str:
    text = (raw_text or "").lower().strip()

    # normalize separators
    text = text.replace("—", " ").replace("–", " ").replace("/", " / ")
    text = re.sub(r"[!?,;:()\[\]{}]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    # brand replacements
    for pattern, value in QUERY_REPLACEMENTS.items():
        text = re.sub(pattern, value, text)

    # normalize money patterns
    text = re.sub(r"\b(\d+)\s*млн\b", r"\1 млн", text)
    text = re.sub(r"\b(\d+)\s*тыс\b", r"\1 тыс", text)

    # normalize mileage wording
    text = text.replace("пробегом", "пробег")
    text = text.replace("пробега", "пробег")

    # normalize km dots
    text = text.replace("км.", "км")
    text = text.replace("km.", "km")

    text = re.sub(r"\s+", " ", text).strip()
    return text