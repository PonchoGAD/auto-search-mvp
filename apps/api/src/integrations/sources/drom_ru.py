import os
import random
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from typing import List, Dict

URL = "https://auto.drom.ru/"

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
PROXIES = {"http": DROM_PROXY, "https": DROM_PROXY} if DROM_PROXY else None

# 🔁 RETRY SESSION
session = requests.Session()
retries = Retry(total=3, backoff_factor=1, status_forcelist=[429, 500, 502, 503, 504])
session.mount("http://", HTTPAdapter(max_retries=retries))
session.mount("https://", HTTPAdapter(max_retries=retries))


def fetch_drom_card(url: str) -> str:
    """Извлекает подробное описание и таблицу характеристик прямо из карточки продавца"""
    try:
        resp = session.get(url, headers=HEADERS, timeout=15, proxies=PROXIES)
        resp.raise_for_status()
    except Exception as e:
        print(f"[DROM][WARN] Ошибка загрузки карточки {url}: {e}", flush=True)
        return ""

    soup = BeautifulSoup(resp.text, "html.parser")

    # 🔥 описание
    description = ""
    desc_block = soup.select_one('[data-ftid="bull_description"]') or soup.select_one('[data-ga-stats-name="description"]')
    if desc_block:
        description = desc_block.get_text(" ", strip=True)

    # 🔥 характеристики (парсим таблицу)
    specs =[]
    for tr in soup.select("table tr"):
        th = tr.select_one("th")
        td = tr.select_one("td")
        if th and td:
            specs.append(f"{th.get_text(' ', strip=True)} {td.get_text(' ', strip=True)}")
        else:
            text = tr.get_text(" ", strip=True)
            if text:
                specs.append(text)

    specs_text = " ".join(specs)

    return f"{description}\n{specs_text}".strip()


def fetch_drom_ru_serp(limit: int = 20) -> List[Dict]:
    """Основная функция, вызываемая из пайплайна (ingest.py)"""
    items =[]
    seen = set()

    for page in range(1, 4):  # Пагинация для надежности сбора лимита
        if len(items) >= limit:
            break

        page_url = f"{URL}?page={page}" if page > 1 else URL
        headers = {**HEADERS, "User-Agent": random.choice(USER_AGENTS)}

        try:
            resp = session.get(page_url, headers=headers, timeout=20, proxies=PROXIES)
            resp.raise_for_status()
        except Exception as e:
            print(f"[DROM][ERROR] fetch SERP error {page_url}: {e}", flush=True)
            continue

        soup = BeautifulSoup(resp.text, "html.parser")
        
        # Drom часто меняет классы, используем несколько селекторов
        cards = soup.select("a.css-xb5nz8") or soup.select("a[data-ftid='bulls-list_bull']") or soup.select("a[href*='auto.drom.ru']")

        for card in cards:
            if len(items) >= limit:
                break

            try:
                ad_url = card.get("href")
                title = card.get_text(" ", strip=True)
            except Exception:
                continue

            if not ad_url or "auto.drom.ru" not in ad_url:
                continue

            if not ad_url.startswith("http"):
                ad_url = "https://auto.drom.ru" + ad_url

            # Отсекаем мусор и служебные страницы
            if any(p in ad_url for p in["/addbull/", "/rate_car/", "/moto/", "/spec/", "/sign", "/my/"]):
                continue
                
            is_ad = ad_url.endswith(".html") or ad_url.rstrip("/").split("/")[-1].isdigit()
            if not is_ad:
                continue

            if ad_url in seen:
                continue
            seen.add(ad_url)

            if "вход" in title.lower() or "регистрац" in title.lower():
                continue

            # 🔥 Идём внутрь карточки за полным текстом
            full_content = fetch_drom_card(ad_url)

            items.append(
                {
                    "source": "drom.ru",
                    "source_url": ad_url,
                    "title": title,
                    "content": full_content if full_content else title,
                }
            )

    print(f"[DROM] fetched: {len(items)} from SERP")
    return items

# Alias для совместимости со старыми вызовами, если такие остались
fetch_drom_ru = fetch_drom_ru_serp