def detect_car_intent(query: str):

    if not query:
        return "browse"

    q = query.lower()

    buy_words = [
        "купить",
        "продажа",
        "цена",
        "до",
        "млн",
        "тыс",
        "пробег",
        "дизель",
        "бенз",
        "бензин",
        "гибрид",
        "электро",
        "km",
        "км"
    ]

    brand_tokens = [
        "bmw","toyota","mercedes","audi","nissan","kia","hyundai",
        "lexus","mazda","honda","volkswagen"
    ]

    if any(w in q for w in buy_words):
        return "buy"

    if any(b in q for b in brand_tokens):
        return "buy"

    return "browse"