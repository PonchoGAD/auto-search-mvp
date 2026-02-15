from typing import List, Dict
from playwright_base import PlaywrightBase
import random

AVITO_BASE_URL = "https://www.avito.ru/all/avtomobili"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)",
]


async def fetch_avito_serp(limit: int = 50) -> List[Dict]:
    base = PlaywrightBase(headless=True)
    await base.launch()
    page = await base.new_page()

    # ✔ Random User-Agent через Playwright
    await page.set_extra_http_headers({
        "User-Agent": random.choice(USER_AGENTS)
    })

    try:
        await base.safe_goto(page, AVITO_BASE_URL)
        await page.wait_for_timeout(4000)
        await page.mouse.wheel(0, 5000)
        await page.wait_for_timeout(3000)

        links = await page.eval_on_selector_all(
            "a[href*='/avtomobili/']",
            "els => els.map(e => e.href)"
        )

        uniq = []
        seen = set()

        for url in links:
            if not url or "/avtomobili/" not in url or url in seen:
                continue
            seen.add(url)
            uniq.append(url)
            if len(uniq) >= limit:
                break

        return [
            {
                "source": "avito.ru",
                "source_url": url,
                "title": url.split("/")[-1],
                "content": url,
            }
            for url in uniq
        ]
    finally:
        await base.close()
