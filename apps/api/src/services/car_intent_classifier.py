def detect_car_intent(query: str):

    q = query.lower()

    buy_words = [
        "купить",
        "цена",
        "до",
        "млн",
        "тыс",
        "пробег"
    ]

    for w in buy_words:
        if w in q:
            return "buy"

    return "browse"