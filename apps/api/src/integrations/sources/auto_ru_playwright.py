from typing import List, Dict

from .playwright_base import PlaywrightBase


AUTO_RU_URL = "https://auto.ru/cars/all/"


async def fetch_auto_ru_serp(limit: int = 30) -> List[Dict]:
    base = PlaywrightBase()
    await base.launch()
    page = await base.new_page()

    try:
        await base.safe_goto(page, AUTO_RU_URL)
        await page.wait_for_timeout(3000)

        links = await page.eval_on_selector_all(
            "a.ListingItemTitle__link",
            "els => els.map(e => e.href)",
        )

        items = [
            {
                "source": "auto.ru",
                "source_url": url,
                "title": url.split("/")[-1],
                "content": url,
            }
            for url in links[:limit]
        ]

        return items

    finally:
        await base.close()
