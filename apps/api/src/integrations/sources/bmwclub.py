from typing import List, Dict
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime
import re

BASE_URL = "https://www.bmwclub.ru"

LISTING_PATHS = [
    "/forums/avtomobili.94/",
    "/find-new/138153914/posts",
]

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


def _clean_post_text(text: str) -> str:
    if not text:
        return ""

    text = re.sub(r"\[quote.*?\].*?\[/quote\]", " ", text, flags=re.S | re.I)
    text = re.sub(r"Sent from.*", " ", text, flags=re.I)
    text = re.sub(r"Отправлено.*", " ", text, flags=re.I)

    text = re.sub(r"\s+", " ", text).strip()
    return text


def _fetch_thread(url: str) -> Dict:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    title_el = soup.select_one("h1")
    title = title_el.get_text(strip=True) if title_el else ""

    first_post = soup.select_one("article.message-body")
    content = first_post.get_text(" ", strip=True) if first_post else ""

    clean_text = _clean_post_text(content)

    return {
        "title": title,
        "content": f"{title}\n\n{clean_text}",
        "clean_text": clean_text,
    }


def _fetch_listing_page(url: str) -> List[str]:
    r = requests.get(url, headers=HEADERS, timeout=30)
    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    links = []

    for a in soup.select("a[href*='/threads/']"):
        href = a.get("href")
        if not href:
            continue
        links.append(urljoin(BASE_URL, href))

    return list(set(links))


def fetch_bmwclub_listings(limit: int = 30) -> List[Dict]:
    results: List[Dict] = []
    seen = set()

    for path in LISTING_PATHS:
        try:
            url = urljoin(BASE_URL, path)
            threads = _fetch_listing_page(url)
        except Exception as e:
            print(f"[BMWCLUB][ERROR] {path}: {e}")
            continue

        for thread_url in threads:
            if thread_url in seen:
                continue
            seen.add(thread_url)

            try:
                data = _fetch_thread(thread_url)
            except Exception as e:
                print(f"[BMWCLUB][THREAD_ERROR] {thread_url}: {e}")
                continue

            results.append(
                {
                    "source": "bmwclub",
                    "source_url": thread_url,
                    "title": data["title"],
                    "content": data["content"],
                    "clean_text": data["clean_text"],
                    "fetched_at": datetime.utcnow().isoformat(),
                }
            )

            if len(results) >= limit:
                return results

    return results
