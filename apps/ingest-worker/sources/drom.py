import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from typing import List, Dict
import random
import time
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

DROM_BASE_URL = "https://auto.drom.ru/"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
}

USER_AGENTS =[
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
]

# 🔐 PROXY SUPPORT
DROM_PROXY = os.getenv("DROM_PROXY")

PROXIES = None
if DROM_PROXY:
    PROXIES = {
        "http": DROM_PROXY,
        "https": DROM_PROXY,
    }
else:
    print("[DROM][WARN] proxy_not_set", flush=True)

# 🔁 RETRY SESSION (PRODUCTION SAFE)
session = requests.Session()

retries = Retry(
    total=3,
    backoff_factor=1,
    status_forcelist=[429, 500, 502, 503, 504],
)

session.mount("http://", HTTPAdapter(max_retries=retries))
session.mount("https://", HTTPAdapter(max_retries=retries))


def fetch_drom_card(url: str) -> str:
    """Извлекает подробное описание и таблицу характеристик прямо из карточки продавца"""
    try:
        resp = session.get(url, headers=HEADERS, timeout=15, proxies=PROXIES)
        resp.raise_for_status()
    except Exception as e:
        print(f"[DROM][WARN] Ошибка доступа к карточке {url}: {e}", flush=True)
        return ""

    if "captcha" in resp.text.lower() or "защита от роботов" in resp.text.lower():
        print(f"[DROM][WARN] Captcha detected on {url}", flush=True)
        return ""

    soup = BeautifulSoup(resp.text, "html.parser")
    text_blocks =[]
    
    # 1. Основное описание продавца
    desc = soup.select_one('[data-ftid="bull_description"]') or soup.select_one('[data-ga-stats-name="description"]')
    if desc:
        text_blocks.append(desc.get_text(" ", strip=True))
        
    # 2. Таблицы характеристик (новые и старые классы Дрома)
    for spec in soup.select('table tr,[data-ftid="component_inline_dict"], [data-ftid="bull_custom_specs"]'):
        text = spec.get_text(" ", strip=True)
        if text and len(text) > 3:
            text_blocks.append(text)
            
    # 3. Если ничего не нашлось, забираем ключевые div'ы
    if not text_blocks:
        for div in soup.select('.css-1j8sk4m, [data-ftid="bull_title"]'):
            text_blocks.append(div.get_text(" ", strip=True))

    return "\n".join(text_blocks).strip()


PROXY = os.getenv("DROM_PROXY")


def create_session() -> requests.Session:
    s = requests.Session()

    retry = Retry(
        total=4,
        backoff_factor=2.0,
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=["GET"],
        raise_on_status=False,
        respect_retry_after_header=True,
    )

    adapter = HTTPAdapter(max_retries=retry, pool_connections=10, pool_maxsize=10)
    s.mount("http://", adapter)
    s.mount("https://", adapter)

    if PROXY:
        s.proxies.update({"http": PROXY, "https": PROXY})
        print("[DROM] proxy enabled")

    return s


def polite_sleep(min_s: float = 2.5, max_s: float = 5.5):
    time.sleep(random.uniform(min_s, max_s))


def fetch_detail_page(session: requests.Session, url: str) -> str | None:
    try:
        headers = {
            "User-Agent": random.choice(USER_AGENTS),
            "Accept-Language": "ru-RU,ru;q=0.9",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,/;q=0.8",
            "Connection": "keep-alive",
        }

        resp = session.get(url, headers=headers, timeout=25)

        if resp.status_code == 429:
            print(f"[DROM][DETAIL 429] {url} -> cooling down")
            time.sleep(60)  # жёсткое охлаждение
            return None

        if resp.status_code != 200:
            print(f"[DROM][DETAIL FAIL] {url} status={resp.status_code}")
            return None

        soup = BeautifulSoup(resp.text, "html.parser")

        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()

        text = soup.get_text(separator=" ", strip=True)

        if not text or len(text) < 600:
            return None

        return text

    except Exception as e:
        print(f"[DROM][DETAIL ERROR] {url} {e}")
        return None


# Drom.ru city slugs for multi-city scraping
_DROM_CITY_SLUGS = [
    "moscow", "spb", "ekb", "novosibirsk", "krasnodar",
    "rostov", "kazan", "chelyabinsk", "omsk", "samara",
    "ufa", "krasnoyarsk", "perm", "voronezh", "volgograd",
    "irkutsk", "tyumen", "khabarovsk", "vladivostok", "barnaul",
    "nnov", "tolyatti", "sochi", "yaroslavl", "ulyanovsk",
]


def _build_drom_urls() -> List[str]:
    """Build list of pages to scrape: global feed + one page per city."""
    city_slugs_env = os.getenv("DROM_CITIES", "").strip()
    slugs = [c.strip() for c in city_slugs_env.split(",") if c.strip()] if city_slugs_env else _DROM_CITY_SLUGS

    urls = []
    # Global feed: 2 pages
    for page in range(1, 3):
        urls.append(f"{DROM_BASE_URL}?page={page}")
    # City-specific: 1 page each (enough diversity, avoids rate-limit)
    for slug in slugs:
        urls.append(f"{DROM_BASE_URL}{slug}/?page=1")
    return urls


def fetch_drom_ru(limit: int = 100) -> List[Dict]:
    """
    Stable Drom.ru ingestion (NO Playwright).
    HTML-only, VPS-safe. Scrapes global feed + all major Russian cities.
    """

    items: List[Dict] =[]
    seen = set()
    filtered = 0

    for url in _build_drom_urls():
        if len(items) >= limit:
            break

        headers = {
            **HEADERS,
            "User-Agent": random.choice(USER_AGENTS)
        }

        try:
            resp = session.get(
                url,
                headers=headers,
                timeout=20,
                proxies=PROXIES,
            )
        except Exception as e:
            print(f"[DROM][ERROR] url={url} err={e}", flush=True)
            continue

        try:
            resp.raise_for_status()
        except Exception as e:
            print(f"[DROM][ERROR] url={url} status_err={e}", flush=True)
            continue

        try:
            soup = BeautifulSoup(resp.text, "html.parser")
        except Exception as e:
            print(f"[DROM][ERROR] url={url} parse_err={e}", flush=True)
            continue

        cards = soup.select("a[href*='auto.drom.ru']")

        for card in cards:
            if len(items) >= limit:
                break

            try:
                ad_url = card.get("href")
                # 🔥 ВАЖНО: разделитель ' | ' предотвращает склеивание слов (2020Москва105тыс). 
                # Так normalize легко найдет пробег и вид топлива!
                title = card.get_text(" | ", strip=True)
                
                # Если текст получился больше 400 знаков, значит он зацепил блок "Похожие", обрезаем.
                if len(title) > 400:
                    title = title[:400]
            except Exception:
                filtered += 1
                continue

            if not ad_url or "auto.drom.ru" not in ad_url:
                filtered += 1
                continue

            if not ad_url or "auto.drom.ru" not in ad_url:
                filtered += 1
                continue

            if not ad_url.startswith("http"):
                ad_url = "https://auto.drom.ru" + ad_url

            # ---- DENYLIST ----
            deny_patterns =[
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
            is_ad = ad_url.endswith(".html")

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

            # Идём внутрь карточки
            full_content = fetch_drom_card(ad_url)

            items.append(
                {
                    "source": "drom.ru",
                    "source_url": ad_url,
                    "title": title,
                    "content": full_content if full_content else title,
                }
            )

    print(f"[DROM] fetched={len(items)} filtered={filtered}", flush=True)
    return items