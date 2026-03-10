def expand_query(query):

    if not query:
        return []

    q = query.lower()

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
            "rav4 xa30",
            "rav4 xa40",
            "rav4 xa50",
        ],
        "rav 4": [
            "toyota rav4",
            "rav4 xa30",
            "rav4 xa40",
            "rav4 xa50",
        ],
        "prado": [
            "toyota prado",
            "land cruiser prado",
            "lc150",
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
            "bmw f30",
            "bmw g20",
            "bmw e90",
        ],
        "e class": [
            "mercedes e class",
            "mercedes w212",
            "mercedes w213",
        ],
        "e-class": [
            "mercedes e class",
            "mercedes w212",
            "mercedes w213",
        ],
        "c class": [
            "mercedes c class",
            "mercedes w204",
            "mercedes w205",
        ],
        "c-class": [
            "mercedes c class",
            "mercedes w204",
            "mercedes w205",
        ],
        "glc": [
            "mercedes glc",
            "glc class",
        ],
        "x-trail": [
            "nissan x trail",
            "nissan x-trail",
        ],
        "x trail": [
            "nissan x trail",
            "nissan x-trail",
        ],
        "qashqai": [
            "nissan qashqai",
        ],
        "solaris": [
            "hyundai solaris",
        ],
        "tucson": [
            "hyundai tucson",
        ],
        "sportage": [
            "kia sportage",
        ],
        "sorento": [
            "kia sorento",
        ],
        "monjaro": [
            "geely monjaro",
        ],
        "corolla fielder": [
            "toyota corolla fielder",
        ],
    }

    expansions = set()

    for key, values in expansions_map.items():
        if key in q:
            for v in values:
                if v != q:
                    expansions.add(v)

    return list(expansions)