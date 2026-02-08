import requests
import re
from typing import List, Dict
from urllib.parse import quote_plus


HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def fetch_auto_ru(limit: int = 20) -> List[Dict]:
    """
    MVP устойчивый auto.ru ingestion через Google SERP.
    Без JS, без Selenium, стабильно.
    """

    query = quote_plus("site:auto.ru купить автомобиль")
    url = f"https://www.google.com/search?q={query}&num=50"

    resp = requests.get(url, headers=HEADERS, timeout=20)
    resp.raise_for_status()

    html = resp.text

    # вытаскиваем ссылки на auto.ru
    urls = re.findall(
        r"https://auto\.ru/[^\s\"&<>]+",
        html
    )

    seen = set()
    items = []

    for u in urls:
        if u in seen:
            continue
        seen.add(u)

        title = u.split("/")[-1].replace("-", " ")

        items.append(
            {
                "source": "auto.ru",
                "source_url": u,
                "title": title,
                "content": title,
            }
        )

        if len(items) >= limit:
            break

    if not items:
        print("[AUTO.RU][ERROR] no items from SERP")

    print(f"[AUTO.RU] fetched via SERP: {len(items)}")
    return items
