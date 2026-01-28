from typing import List, Dict
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = "https://www.benzclub.ru"

# Актуальные разделы (ПРОДАЖА / АВТО)
LISTING_PATHS = [
    "/forum/forumdisplay.php?f=37",  # Продажа авто
    "/forum/forumdisplay.php?f=38",  # Купля-продажа
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def _fetch_listing_page(url: str) -> List[Dict]:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    items: List[Dict] = []

    # vBulletin / XenForo — темы
    for a in soup.select("a[href*='showthread.php']"):
        href = a.get("href")
        title = a.get_text(strip=True)

        if not href or not title:
            continue

        full_url = urljoin(BASE_URL, href)

        items.append(
            {
                "source": "benzclub.ru",
                "source_url": full_url,
                "title": title,
                "content": title,  # SERP only
            }
        )

    return items


def fetch_benzclub_listings(limit: int = 30) -> List[Dict]:
    """
    HTTP ingestion benzclub.ru
    Парсит ТОЛЬКО списки тем (продажи авто).
    Без захода в карточки.
    """
    results: List[Dict] = []
    seen = set()

    for path in LISTING_PATHS:
        try:
            url = urljoin(BASE_URL, path)
            items = _fetch_listing_page(url)
        except Exception as e:
            print(f"[BENZCLUB][ERROR] {path}: {e}")
            continue

        for it in items:
            if it["source_url"] in seen:
                continue

            seen.add(it["source_url"])
            results.append(it)

            if len(results) >= limit:
                return results

    return results
