import requests
from bs4 import BeautifulSoup
from typing import List, Dict
import random

DROM_BASE_URL = "https://auto.drom.ru/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    )
}

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)",
]


def fetch_drom_ru(limit: int = 50) -> List[Dict]:
    """
    Stable Drom.ru ingestion (NO Playwright).
    HTML-only, VPS-safe.

    Returns:
    [
      { source, source_url, title, content }
    ]
    """

    items: List[Dict] = []
    seen = set()

    # 🔥 PAGINATION: pages 1..5
    for page in range(1, 6):
        if len(items) >= limit:
            break

        url = f"{DROM_BASE_URL}?page={page}"

        headers = {
            "User-Agent": random.choice(USER_AGENTS)
        }

        resp = requests.get(url, headers=headers, timeout=15)
        resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")

        # ⚠️ Drom реально часто меняет классы
        # Этот селектор сейчас самый стабильный для ссылок на объявления
        cards = soup.select("a[href*='auto.drom.ru']")

        for card in cards:
            if len(items) >= limit:
                break

            ad_url = card.get("href")
            title = card.get_text(strip=True)

            if not ad_url or "auto.drom.ru" not in ad_url:
                continue

            if not ad_url.startswith("http"):
                ad_url = "https://auto.drom.ru" + ad_url

            if ad_url in seen:
                continue

            seen.add(ad_url)

            # 🔒 Anti-login / garbage filter
            if "my.drom.ru/sign" in ad_url:
                continue

            if not title:
                continue

            if "вход" in title.lower() or "регистрац" in title.lower():
                continue

            if "/sign?" in ad_url:
                continue

            items.append(
                {
                    "source": "drom.ru",
                    "source_url": ad_url,
                    "title": title or ad_url.split("/")[-1],
                    "content": title or ad_url,
                }
            )

    print(f"[DROM] fetched={len(items)}")
    return items
