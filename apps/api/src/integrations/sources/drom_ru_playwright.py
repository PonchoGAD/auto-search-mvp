from typing import List, Dict

from .playwright_base import PlaywrightBase

DROM_BASE_URL = "https://auto.drom.ru/"


async def fetch_drom_ru_serp(limit: int = 30) -> List[Dict]:
    """
    Fetch SERP links from drom.ru (list pages only).
    Без захода в карточки объявлений.

    Возвращает:
    [
        {
            "source": "drom.ru",
            "source_url": "...",
            "title": "...",
            "content": "..."
        }
    ]
    """

    base = PlaywrightBase()
    await base.launch()

    page = await base.new_page()

    try:
        # 1️⃣ Безопасный переход
        await base.safe_goto(page, DROM_BASE_URL)
        await page.wait_for_timeout(3000)

        links: List[str] = []

        # ==================================================
        # PRIMARY SELECTOR (часто меняется, но пробуем первым)
        # ==================================================
        try:
            links = await page.eval_on_selector_all(
                "a.css-5l099z.e1huvdhj1",
                "els => els.map(e => e.href)"
            )
        except Exception:
            links = []

        # ==================================================
        # FALLBACK #1 — стабильный data-ftid (если доступен)
        # ==================================================
        if not links:
            try:
                links = await page.eval_on_selector_all(
                    "a[data-ftid='bulls-list_bull']",
                    "els => els.map(e => e.href)"
                )
            except Exception:
                links = []

        # ==================================================
        # FALLBACK #2 — очень общий, но надёжный
        # ==================================================
        if not links:
            try:
                links = await page.eval_on_selector_all(
                    "a[href*='auto.drom.ru']",
                    "els => els.map(e => e.href)"
                )
            except Exception:
                links = []

        # ==================================================
        # CLEANUP + DEDUP + LIMIT
        # ==================================================
        uniq_links: List[str] = []
        seen = set()

        for url in links:
            if not url:
                continue
            if "auto.drom.ru" not in url:
                continue
            if url in seen:
                continue

            seen.add(url)
            uniq_links.append(url)

            if len(uniq_links) >= limit:
                break

        # ==================================================
        # RESULT FORMAT
        # ==================================================
        return [
            {
                "source": "drom.ru",
                "source_url": url,
                "title": url.split("/")[-1],
                "content": url,  # SERP only, без захода в карточки
            }
            for url in uniq_links
        ]

    finally:
        await base.close()
