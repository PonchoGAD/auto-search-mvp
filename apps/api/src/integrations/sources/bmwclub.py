from typing import List, Dict
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

BASE_URL = "https://www.bmwclub.ru"

# Актуальные разделы ПРОДАЖИ
LISTING_PATHS = [
    "/forums/avtomobili.94/",        # Продажа авто BMW
    "/find-new/138153914/posts",     # Новые объявления (маркет)
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

    # XenForo — ссылки на темы
    for a in soup.select("a[href*='/threads/']"):
        href = a.get("href")
        title = a.get_text(strip=True)

        if not href or not title:
            continue

        full_url = urljoin(BASE_URL, href)

        items.append(
            {
                "source": "bmwclub.ru",
                "source_url": full_url,
                "title": title,
                "content": title,  # SERP only
            }
        )

    return items


def fetch_bmwclub_listings(limit: int = 30) -> List[Dict]:
    """
    HTTP ingestion bmwclub.ru
    Парсит списки тем (продажа BMW).
    Без карточек.
    """
    results: List[Dict] = []
    seen = set()

    for path in LISTING_PATHS:
        try:
            url = urljoin(BASE_URL, path)
            items = _fetch_listing_page(url)
        except Exception as e:
            print(f"[BMWCLUB][ERROR] {path}: {e}")
            continue

        for it in items:
            if it["source_url"] in seen:
                continue

            seen.add(it["source_url"])
            results.append(it)

            if len(results) >= limit:
                return results

    return results
