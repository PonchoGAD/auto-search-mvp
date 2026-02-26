import requests
from bs4 import BeautifulSoup
from typing import List, Dict
import random
import time

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


def fetch_detail_page(url: str) -> str | None:
    try:
        headers = {
            "User-Agent": random.choice(USER_AGENTS)
        }

        resp = requests.get(url, headers=headers, timeout=15)

        if resp.status_code != 200:
            print(f"[DROM][DETAIL FAIL] {url} status={resp.status_code}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        # УБИРАЕМ скрипты и стили
        for tag in soup(["script", "style"]):
            tag.decompose()

        text = soup.get_text(separator=" ", strip=True)

        if not text or len(text) < 500:
            return None

        return text

    except Exception as e:
        print(f"[DROM][DETAIL ERROR] {url} {e}")
        return None


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
    filtered = 0

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
                filtered += 1
                continue

            if not ad_url.startswith("http"):
                ad_url = "https://auto.drom.ru" + ad_url

            # ---- DENYLIST ----
            deny_patterns = [
                "/addbull/",
                "/rate_car/",
                "/moto/",
                "/spec/",
                "/sign",
                "/my/",
            ]

            if any(p in ad_url for p in deny_patterns):
                filtered += 1
                continue

            # root brand pages like /bmw/
            if ad_url.rstrip("/").count("/") <= 3:
                filtered += 1
                continue

            # ---- ALLOWLIST ----
            is_ad = (
                ad_url.endswith(".html") or
                ad_url.rstrip("/").split("/")[-1].isdigit()
            )

            if not is_ad:
                filtered += 1
                continue

            if ad_url in seen:
                filtered += 1
                continue

            seen.add(ad_url)

            # 🔒 Anti-login / garbage filter
            if "my.drom.ru/sign" in ad_url:
                filtered += 1
                continue

            if not title:
                filtered += 1
                continue

            if "вход" in title.lower() or "регистрац" in title.lower():
                filtered += 1
                continue

            if "/sign?" in ad_url:
                filtered += 1
                continue

            # 🔥 ГРУЗИМ DETAIL СТРАНИЦУ
            detail_text = fetch_detail_page(ad_url)

            if not detail_text:
                continue

            # 🔥 САНИТИ: должна быть цена и км
            if "км" not in detail_text:
                continue

            if "₽" not in detail_text and "руб" not in detail_text:
                continue

            items.append(
                {
                    "source": "drom.ru",
                    "source_url": ad_url,
                    "title": title or ad_url.split("/")[-1],
                    "content": detail_text,
                }
            )

            # маленькая пауза чтобы не ловить 429
            time.sleep(1.2)

    print(f"[DROM] fetched={len(items)} filtered={filtered}")
    return items