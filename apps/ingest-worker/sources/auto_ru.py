from typing import List, Dict
from playwright_base import PlaywrightBase
import random

AUTO_RU_BASE_URL = "https://auto.ru/cars/all/"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)",
]


async def fetch_auto_ru_serp(limit: int = 50) -> List[Dict]:
    base = PlaywrightBase(headless=True)
    await base.launch()
    page = await base.new_page()

    # ✔ Random User-Agent через Playwright
    await page.set_extra_http_headers({
        "User-Agent": random.choice(USER_AGENTS)
    })

    try:
        await base.safe_goto(page, AUTO_RU_BASE_URL)
        await page.wait_for_timeout(4000)
        await page.mouse.wheel(0, 4000)
        await page.wait_for_timeout(3000)

        links = await page.eval_on_selector_all(
            "a[href*='/cars/']",
            "els => els.map(e => e.href)"
        )

        uniq = []
        seen = set()

        for url in links:
            if not url or url in seen:
                continue
            seen.add(url)
            uniq.append(url)
            if len(uniq) >= limit:
                break

        return [
            {
                "source": "auto.ru",
                "source_url": url,
                "title": url.split("/")[-1],
                "content": url,
            }
            for url in uniq
        ]
    finally:
        await base.close()
