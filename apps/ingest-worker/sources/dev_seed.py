from datetime import datetime

def fetch_dev_seed(limit: int = 5):
    return [
        {
            "source": "dev_seed",
            "source_url": "https://seed.local/bmw-520i-2018",
            "title": "BMW 520i 2018 1 669 000 ₽",
            "content": "BMW 520i 2018 год пробег 120 000 км бензин",
            "created_at": datetime.utcnow(),
        },
        {
            "source": "dev_seed",
            "source_url": "https://seed.local/mercedes-e200-2019",
            "title": "Mercedes E200 2019 2 150 000 ₽",
            "content": "Mercedes E200 2019 пробег 90 000 км бензин",
            "created_at": datetime.utcnow(),
        },
        {
            "source": "dev_seed",
            "source_url": "https://seed.local/bmw-x5-2017",
            "title": "BMW X5 2017 2 400 000 ₽",
            "content": "BMW X5 2017 пробег 140 000 км дизель",
            "created_at": datetime.utcnow(),
        },
    ]