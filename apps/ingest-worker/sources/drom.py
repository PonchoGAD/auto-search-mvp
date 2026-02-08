import requests
from bs4 import BeautifulSoup
from typing import List, Dict

DROM_BASE_URL = "https://auto.drom.ru/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}


def fetch_drom_ru(limit: int = 30) -> List[Dict]:
    """
    Stable Drom.ru ingestion (NO Playwright).
    HTML-only, VPS-safe.

    Returns:
    [
      { source, source_url, title, content }
    ]
    """

    resp = requests.get(DROM_BASE_URL, headers=HEADERS, timeout=20)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.text, "html.parser")

    # ⚠️ Drom реально часто меняет классы
    # Этот селектор сейчас самый стабильный для ссылок на объявления
    cards = soup.select("a[href*='auto.drom.ru']")

    items: List[Dict] = []
    seen = set()

    for card in cards:
        url = card.get("href")
        title = card.get_text(strip=True)

        if not url or "auto.drom.ru" not in url:
            continue

        if not url.startswith("http"):
            url = "https://auto.drom.ru" + url

        if url in seen:
            continue

        seen.add(url)

        items.append(
            {
                "source": "drom.ru",
                "source_url": url,
                "title": title or url.split("/")[-1],
                "content": title or url,
            }
        )

        if len(items) >= limit:
            break

    print(f"[DROM] fetched={len(items)}")
    return items
