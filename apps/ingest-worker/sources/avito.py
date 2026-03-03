from typing import List, Dict
from playwright_base import PlaywrightBase
import asyncio
import random

AVITO_BASE_URL = "https://www.avito.ru/all/avtomobili"


async def fetch_avito_serp(limit: int = 50) -> List[Dict]:
    base = PlaywrightBase(headless=True)

    try:
        await base.launch()
        page = await base.new_page()

        await base.safe_goto(page, AVITO_BASE_URL)

        for _ in range(3):
            await page.mouse.wheel(0, random.randint(3000, 5000))
            await asyncio.sleep(random.uniform(1.0, 2.0))

        content = await page.content()
        if "captcha" in content.lower():
            print("[AVITO] captcha detected — skipping")
            return []

        links = await page.eval_on_selector_all(
            "a[href*='/avtomobili/']",
            "els => els.map(e => e.href)"
        )

        uniq = []
        seen = set()

        for url in links:
            if not url:
                continue
            if "/avtomobili/" not in url:
                continue
            if url in seen:
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

    except Exception as e:
        print(f"[AVITO] failed: {e}")
        return []

    finally:
        await base.close()