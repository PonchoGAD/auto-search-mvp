from typing import List, Dict


def fetch_mock(limit: int = 20) -> List[Dict]:
    """
    MOCK источник для проверки всего пайплайна:
    ingest → normalize → chunk → index → search
    """

    items = []

    for i in range(limit):
        items.append(
            {
                "source": "mock",
                "source_url": f"https://mock.local/car/{i}",
                "title": f"BMW X5 2019 бензин без окрасов {40 + i} 000 км",
                "content": (
                    f"BMW X5 2019 год, бензин, пробег {40 + i} 000 км, "
                    f"без окрасов, цена 3 800 000 руб, Москва"
                ),
            }
        )

    print(f"[MOCK] generated: {len(items)} items")
    return items
