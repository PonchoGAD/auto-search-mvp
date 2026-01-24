from typing import List, Dict

from .playwright_base import PlaywrightBase


DROM_RU_URL = "https://auto.drom.ru/"


async def fetch_drom_ru_serp(limit: int = 30) -> List[Dict]:
    base = PlaywrightBase()
    await base.launch()
    page = await base.new_page()

    try:
        await base.safe_goto(page, DROM_RU_URL)
        await page.wait_for_timeout(4000)

        # Drom SERP: ссылки на объявления
        links = await page.eval_on_selector_all(
            "a.css-xb5nz8, a.css-1f68fiz",
            "els => els.map(e => e.href).filter(h => h && h.includes('auto.drom.ru'))",
        )

        uniq = []
        seen = set()
        for url in links:
            if url not in seen:
                uniq.append(url)
                seen.add(url)

        items = [
            {
                "source": "drom.ru",
                "source_url": url,
                "title": url.split("/")[-1],
                "content": url,
            }
            for url in uniq[:limit]
        ]

        return items

    finally:
        await base.close()
