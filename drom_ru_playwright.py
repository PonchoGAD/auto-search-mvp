# drom_ru_playwright.py
from .playwright_base import PlaywrightBase

DROM_RU_URL = "https://auto.drom.ru/"

async def fetch_drom_ru_serp(limit: int = 30):
    """
    MVP ingestion с drom.ru:
    - только SERP (список объявлений)
    - без захода в карточки
    - минимальный риск антибота
    """

    base = PlaywrightBase()
    await base.launch()
    page = await base.new_page()

    await page.goto(DROM_RU_URL, timeout=60000)
    await page.wait_for_timeout(4000)

    # На drom ссылки обычно выглядят как <a href="https://auto.drom.ru/...">
    links = await page.eval_on_selector_all(
        "a[href^='https://auto.drom.ru/']",
        "els => els.map(e => e.href)"
    )

    await base.close()

    # дедуп на уровне SERP
    uniq = []
    seen = set()
    for url in links:
        if url in seen:
            continue
        seen.add(url)
        uniq.append(url)

    return [
        {
            "source": "drom.ru",
            "source_url": url,
            "title": url.split("/")[-1],
            "content": url,  # на этапе SERP достаточно
        }
        for url in uniq[:limit]
    ]
