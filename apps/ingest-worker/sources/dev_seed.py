from datetime import datetime, timezone

def fetch_dev_seed(limit: int = 5):
    return [
        {
            "source": "dev_seed",
            "source_url": "https://seed.local/bmw-520i-2018",
            "title": "BMW 520i 2018 1 669 000 ₽ продажа",
            "content": "Продам BMW 520i 2018 год пробег 120 000 км бензин в отличном состоянии. Цена 1 669 000 рублей.",
            "created_at": datetime.now(timezone.utc),
        },
        {
            "source": "dev_seed",
            "source_url": "https://seed.local/mercedes-e200-2019",
            "title": "Mercedes E200 2019 2 150 000 ₽ продажа",
            "content": "Продам Mercedes E-Class E200 2019 пробег 90 000 км бензин один владелец. Цена 2 150 000 руб.",
            "created_at": datetime.now(timezone.utc),
        },
        {
            "source": "dev_seed",
            "source_url": "https://seed.local/bmw-x5-2017",
            "title": "BMW X5 2017 2 400 000 ₽ продажа",
            "content": "Продам BMW X5 xDrive 2017 год пробег 140 000 км дизель полный привод. Цена 2 400 000 ₽.",
            "created_at": datetime.now(timezone.utc),
        },
        {
            "source": "dev_seed",
            "source_url": "https://seed.local/toyota-camry-2020",
            "title": "Toyota Camry 2020 2 350 000 ₽ продажа",
            "content": "Продам Toyota Camry 3.5 2020 год пробег 55 000 км бензин Москва. Цена 2 350 000 руб.",
            "created_at": datetime.now(timezone.utc),
        },
        {
            "source": "dev_seed",
            "source_url": "https://seed.local/kia-sportage-2021",
            "title": "Kia Sportage 2021 1 950 000 ₽ продажа",
            "content": "Продам Kia Sportage 2021 год пробег 40 000 км бензин передний привод. Цена 1 950 000 ₽.",
            "created_at": datetime.now(timezone.utc),
        },
        {
            "source": "dev_seed",
            "source_url": "https://seed.local/hyundai-tucson-2022",
            "title": "Hyundai Tucson 2022 2 100 000 ₽ продажа",
            "content": "Продам Hyundai Tucson 2022 год пробег 25 000 км бензин Санкт-Петербург. Цена 2 100 000 руб.",
            "created_at": datetime.now(timezone.utc),
        },
        {
            "source": "dev_seed",
            "source_url": "https://seed.local/audi-a6-2019",
            "title": "Audi A6 2019 3 200 000 ₽ продажа",
            "content": "Продам Audi A6 55 TFSI 2019 год пробег 75 000 км бензин Москва. Цена 3 200 000 ₽.",
            "created_at": datetime.now(timezone.utc),
        },
        {
            "source": "dev_seed",
            "source_url": "https://seed.local/toyota-land-cruiser-2021",
            "title": "Toyota Land Cruiser 300 2021 11 500 000 ₽ продажа",
            "content": "Продам Toyota Land Cruiser 300 2021 год пробег 30 000 км дизель полный привод Москва. Цена 11 500 000 руб.",
            "created_at": datetime.now(timezone.utc),
        },
        {
            "source": "dev_seed",
            "source_url": "https://seed.local/volkswagen-tiguan-2020",
            "title": "Volkswagen Tiguan 2020 1 890 000 ₽ продажа",
            "content": "Продам Volkswagen Tiguan 2.0 TSI 2020 год пробег 62 000 км бензин. Цена 1 890 000 рублей.",
            "created_at": datetime.now(timezone.utc),
        },
        {
            "source": "dev_seed",
            "source_url": "https://seed.local/nissan-qashqai-2019",
            "title": "Nissan Qashqai 2019 1 450 000 ₽ продажа",
            "content": "Продам Nissan Qashqai 2019 пробег 80 000 км бензин передний привод. Цена 1 450 000 руб.",
            "created_at": datetime.now(timezone.utc),
        },
    ]