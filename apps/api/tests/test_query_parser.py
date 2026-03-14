from services.query_parser import parse_query


def test_price_thousands():
    q = parse_query("kia rio до 900 тыс")
    assert q.price_max == 900000
    assert q.mileage_max is None


def test_mileage_explicit():
    q = parse_query("bmw пробег до 120 тыс км")
    assert q.mileage_max == 120000
    assert q.price_max is None


def test_price_millions():
    q = parse_query("camry до 2 млн")
    assert q.price_max == 2000000
    assert q.mileage_max is None


def test_mileage_km():
    q = parse_query("bmw до 200 тыс км")
    assert q.mileage_max == 200000
    assert q.price_max is None