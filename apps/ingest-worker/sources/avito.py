from typing import List, Dict
from playwright_base import PlaywrightBase

AVITO_BASE_URL = "https://www.avito.ru/all/avtomobili"


async def fetch_avito_serp(limit: int = 30) -> List[Dict]:
    base = PlaywrightBase(headless=True)
    await base.launch()
    page = await base.new_page()

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
