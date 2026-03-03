from typing import List, Dict
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from datetime import datetime, timezone
import re
import os
import time
import random

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

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

BMWCLUB_ENABLED = os.getenv("BMWCLUB_ENABLED", "false").lower() == "true"


# =========================
# SESSION + RETRY
# =========================

def build_session():
    s = requests.Session()
    retry = Retry(
        total=3,
        connect=3,
        read=3,
        backoff_factor=1.2,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )
    adapter = HTTPAdapter(max_retries=retry)
    s.mount("https://", adapter)
    s.mount("http://", adapter)
    s.headers.update(HEADERS)
    return s


SESSION = build_session()


def detect_brand(text: str) -> str | None:
    if not text:
        return None

    t = text.lower()

    if "bmw" in t:
        return "bmw"
    if "mercedes" in t or "benz" in t:
        return "mercedes"

    return None


def _clean_post_text(text: str) -> str:
    if not text:
        return ""

    text = re.sub(r"\[quote.*?\].*?\[/quote\]", " ", text, flags=re.S | re.I)
    text = re.sub(r"Sent from.*", " ", text, flags=re.I)
    text = re.sub(r"Отправлено.*", " ", text, flags=re.I)

    text = re.sub(r"\s+", " ", text).strip()
    return text


def _extract_datetime(soup) -> tuple[str | None, int | None, str]:
    t = soup.select_one("time[datetime]")
    if t and t.get("datetime"):
        try:
            dt = datetime.fromisoformat(t["datetime"].replace("Z", "+00:00"))
            return dt.isoformat(), int(dt.timestamp()), "bmwclub"
        except Exception:
            pass

    now = datetime.now(timezone.utc)
    return now.isoformat(), int(now.timestamp()), "ingested"


def _fetch_thread(url: str) -> Dict:
    r = SESSION.get(url, timeout=30)

    if r.status_code >= 400:
        print(f"[BMWCLUB][HTTP] status={r.status_code} url={url}")

    if r.status_code == 403:
        print("[BMWCLUB] 403 forbidden, disabling source")
        return {"_403": True}

    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")

    title_el = soup.select_one("h1")
    title = title_el.get_text(strip=True) if title_el else ""

    first_post = soup.select_one("article.message-body")
    content = first_post.get_text(" ", strip=True) if first_post else ""

    clean_text = _clean_post_text(content)

    created_at, created_at_ts, created_at_source = _extract_datetime(soup)

    return {
        "title": title,
        "content": f"{title}\n\n{clean_text}",
        "clean_text": clean_text,
        "created_at": created_at,
        "created_at_ts": created_at_ts,
        "created_at_source": created_at_source,
    }


def _fetch_listing_page(url: str) -> tuple[List[str], bool]:
    r = SESSION.get(url, timeout=30)

    if r.status_code >= 400:
        print(f"[BMWCLUB][HTTP] status={r.status_code} url={url}")

    if r.status_code == 403:
        print("[BMWCLUB] 403 forbidden on listing, disabling source")
        return [], True

    r.raise_for_status()

    soup = BeautifulSoup(r.text, "html.parser")
    links = []

    for a in soup.select("a[href*='/threads/']"):
        href = a.get("href")
        if not href:
            continue

        full = urljoin(BASE_URL, href)

        if not re.search(r"/threads/[^/]*\.\d+/?$", full) and not re.search(r"/threads/\d+/?$", full):
            continue

        links.append(full)

    unique_links = list(set(links))
    return unique_links[:100], False


def fetch_bmwclub_listings(limit: int = 30) -> List[Dict]:
    global BMWCLUB_ENABLED

    if not BMWCLUB_ENABLED:
        print("[BMWCLUB] disabled by config")
        return []

    results: List[Dict] = []
    seen = set()

    for path in LISTING_PATHS:
        try:
            url = urljoin(BASE_URL, path)
            threads, forbidden = _fetch_listing_page(url)

            if forbidden:
                BMWCLUB_ENABLED = False
                print("[BMWCLUB] disabled reason=403")
                return []

        except Exception as e:
            print(f"[BMWCLUB][ERROR] {path}: {e}")
            continue

        for thread_url in threads:
            if thread_url in seen:
                continue
            seen.add(thread_url)

            time.sleep(random.uniform(0.7, 1.6))

            try:
                data = _fetch_thread(thread_url)
            except Exception as e:
                print(f"[BMWCLUB][THREAD_ERROR] {thread_url}: {e}")
                continue

            if not data:
                continue

            if data.get("_403"):
                BMWCLUB_ENABLED = False
                print("[BMWCLUB] disabled reason=403")
                return []

            clean_text = (data.get("clean_text") or "").strip()
            if not clean_text or len(clean_text) < 10:
                print(f"[BMWCLUB][SKIP] reason=short_content url={thread_url}")
                continue

            results.append({
                "source": "bmwclub.ru",
                "source_url": thread_url,
                "title": data.get("title") or "",
                "content": data.get("content") or "",
                "created_at": data.get("created_at"),
                "created_at_ts": data.get("created_at_ts"),
                "created_at_source": data.get("created_at_source", "bmwclub"),
            })

            if len(results) >= limit:
                return results

    return results