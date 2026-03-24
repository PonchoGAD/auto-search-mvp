import os
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from typing import List, Dict
import random

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


def fetch_drom_ru(limit: int = 50) -> List[Dict]:
    """
    Stable Drom.ru ingestion (NO Playwright).
    HTML-only, VPS-safe.
    """

    items: List[Dict] =[]
    seen = set()
    filtered = 0

    # 🔥 PAGINATION: pages 1..5
    for page in range(1, 6):
        if len(items) >= limit:
            break

        url = f"{DROM_BASE_URL}?page={page}"

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