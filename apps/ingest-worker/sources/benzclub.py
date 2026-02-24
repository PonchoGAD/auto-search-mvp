from typing import List, Dict
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import re

BASE_URL = "https://www.benzclub.ru"

LISTING_PATHS = [
    "/forum/forumdisplay.php?f=37",  # Продажа авт
    "/forum/forumdisplay.php?f=38",  # Купля-продажа
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def detect_brand(text: str) -> str | None:
    if not text:
        return None

    t = text.lower()

    if "mercedes" in t or "benz" in t:
        return "mercedes"
    if "bmw" in t:
        return "bmw"

    return None


def normalize_benzclub_url(url: str) -> str:
    import re
    from urllib.parse import urlparse, parse_qs

    parsed = urlparse(url)
    qs = parse_qs(parsed.query)

    thread_id = None

    if "t" in qs:
        thread_id = qs["t"][0]

    match = re.search(r"showthread\.php\?t=(\d+)", url)
    if match:
        thread_id = match.group(1)

    if not thread_id:
        return url

    return f"{BASE_URL}/forum/showthread.php?t={thread_id}"


def _clean_post_text(text: str) -> str:
    if not text:
        return ""

    text = re.sub(r"\[quote.*?\].*?\[/quote\]", " ", text, flags=re.S | re.I)
    text = re.sub(r"Sent from.*", " ", text, flags=re.I)
    text = re.sub(r"Отправлено.*", " ", text, flags=re.I)

    text = re.sub(r"\s+", " ", text).strip()
    return text


def _fetch_thread(url: str) -> Dict | None:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    title_el = soup.select_one("h1")
    parsed_title = title_el.get_text(strip=True) if title_el else ""

    first_post = soup.select_one("div.postbody")
    content = first_post.get_text(" ", strip=True) if first_post else ""

    clean_text = _clean_post_text(content)
    parsed_text = f"{parsed_title}\n\n{clean_text}".strip()

    if not parsed_text or len(parsed_text.strip()) < 100:
        return None

    return {
        "source": "benzclub.ru",
        "source_url": url,
        "title": parsed_title.strip(),
        "content": parsed_text.strip(),
    }


def _fetch_listing_page(url: str) -> List[str]:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    links = []

    for a in soup.select("a[href*='showthread.php']"):
        href = a.get("href")
        if not href:
            continue

        full_url = urljoin(BASE_URL, href)
        full_url = normalize_benzclub_url(full_url)

        links.append(full_url)

    return list(set(links))


def fetch_benzclub_listings(limit: int = 30) -> List[Dict]:
    results: List[Dict] = []
    seen = set()

    for path in LISTING_PATHS:
        try:
            url = urljoin(BASE_URL, path)
            threads = _fetch_listing_page(url)
        except Exception as e:
            print(f"[BENZCLUB][ERROR] {path}: {e}")
            continue

        for thread_url in threads:
            thread_url = normalize_benzclub_url(thread_url)

            if thread_url in seen:
                continue
            seen.add(thread_url)

            try:
                data = _fetch_thread(thread_url)
                if not data:
                    continue
            except Exception as e:
                print(f"[BENZCLUB][THREAD_ERROR] {thread_url}: {e}")
                continue

            brand = detect_brand(data["content"])

            results.append(
                {
                    "source": "benzclub.ru",
                    "source_url": thread_url,
                    "title": data["title"],
                    "content": data["content"],
                    "brand": brand,
                    "model": None,
                    "price": None,
                    "mileage": None,
                    "currency": "RUB",
                    "sale_intent": 1,
                    "created_at": datetime.utcnow().isoformat(),
                    "created_at_ts": int(datetime.utcnow().timestamp()),
                }
            )

            if len(results) >= limit:
                return results

    return results