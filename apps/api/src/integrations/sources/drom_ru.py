import requests
from bs4 import BeautifulSoup
from typing import List, Dict

URL = "https://auto.drom.ru/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}


def fetch_drom_ru(limit: int = 20) -> List[Dict]:
    resp = requests.get(URL, headers=HEADERS, timeout=20)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    cards = soup.select("a.css-xb5nz8")  # стабильный селектор Drom
    items = []

    for card in cards[:limit]:
        url = card.get("href")
        title = card.get_text(strip=True)

        if not url:
            continue

        if not url.startswith("http"):
            url = "https://auto.drom.ru" + url

        items.append(
            {
                "source": "drom.ru",
                "source_url": url,
                "title": title,
                "content": title,
            }
        )

    print(f"[DROM] fetched: {len(items)}")
    return items
