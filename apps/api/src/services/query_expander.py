import re


def _norm(q: str) -> str:
    q = (q or "").lower().strip()
    q = q.replace("ё", "е")
    q = re.sub(r"[_/]+", " ", q)
    q = re.sub(r"\s+", " ", q)
    return q


def expand_query(query, structured=None):
    if not query:
        return []

    # ✅ Рекомендация: не расширяем точный запрос
    if structured and getattr(structured, "model", None):
        return []

    q = _norm(query)

    expansions_map = {
        "camry": [
            "toyota camry",
            "camry xv40",
            "camry xv50",
            "camry xv55",
            "camry xv70",
        ],
        "corolla": [
            "toyota corolla",
            "corolla e150",
            "corolla e170",
            "corolla e210",
        ],
        "rav4": [
            "toyota rav4",
            "toyota rav-4",
            "rav4 xa30",
            "rav4 xa40",
            "rav4 xa50",
        ],
        "rav 4": [
            "toyota rav4",
            "toyota rav-4",
            "rav4 xa30",
            "rav4 xa40",
            "rav4 xa50",
        ],
        "rav-4": [
            "toyota rav4",
            "toyota rav-4",
            "rav4 xa30",
            "rav4 xa40",
            "rav4 xa50",
        ],
        "prado": [
            "toyota prado",
            "land cruiser prado",
            "lc150",
            "lc120",
        ],
        "land cruiser": [
            "toyota land cruiser",
            "lc200",
            "lc300",
            "land cruiser 200",
            "land cruiser 300",
        ],
        "x5": [
            "bmw x5",
            "bmw x5 e70",
            "bmw x5 f15",
            "bmw x5 g05",
        ],
        "x3": [
            "bmw x3",
            "bmw x3 f25",
            "bmw x3 g01",
        ],
        "3 series": [
            "bmw 3 series",
            "bmw 3-series",
            "bmw f30",
            "bmw g20",
            "bmw e90",
        ],
        "3-series": [
            "bmw 3 series",
            "bmw 3-series",
            "bmw f30",
            "bmw g20",
            "bmw e90",
        ],
        "5 series": [
            "bmw 5 series",
            "bmw 5-series",
            "bmw f10",
            "bmw g30",
        ],
        "e class": [
            "mercedes e class",
            "mercedes e-class",
            "mercedes w212",
            "mercedes w213",
        ],
        "e-class": [
            "mercedes e class",
            "mercedes e-class",
            "mercedes w212",
            "mercedes w213",
        ],
        "c class": [
            "mercedes c class",
            "mercedes c-class",
            "mercedes w204",
            "mercedes w205",
        ],
        "c-class": [
            "mercedes c class",
            "mercedes c-class",
            "mercedes w204",
            "mercedes w205",
        ],
        "glc": [
            "mercedes glc",
            "glc class",
            "glc x253",
        ],
        "x-trail": [
            "nissan x trail",
            "nissan x-trail",
            "t31",
            "t32",
        ],
        "x trail": [
            "nissan x trail",
            "nissan x-trail",
            "t31",
            "t32",
        ],
        "qashqai": [
            "nissan qashqai",
            "j10",
            "j11",
        ],
        "solaris": [
            "hyundai solaris",
        ],
        "tucson": [
            "hyundai tucson",
            "ix35",
        ],
        "sportage": [
            "kia sportage",
            "ql",
            "nq5",
        ],
        "sorento": [
            "kia sorento",
            "um",
            "mq4",
        ],
        "monjaro": [
            "geely monjaro",
            "xingyue l",
        ],
        "coolray": [
            "geely coolray",
            "binyue",
        ],
        "corolla fielder": [
            "toyota corolla fielder",
        ],
    }

    expansions = []
    seen = set()

    # ✅ FIX: работаем через токены
    tokens = set(q.split())

    for key, values in expansions_map.items():
        key_tokens = set(_norm(key).split())

        if key_tokens.issubset(tokens):
            for v in values:
                nv = _norm(v)
                if nv != q and nv not in seen:
                    seen.add(nv)
                    expansions.append(v)

    return expansions