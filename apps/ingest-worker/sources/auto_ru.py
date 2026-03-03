from typing import List, Dict
from playwright_base import PlaywrightBase
import asyncio
import random

AUTO_RU_BASE_URL = "https://auto.ru/cars/all/"


async def fetch_auto_ru_serp(limit: int = 50) -> List[Dict]:
    base = PlaywrightBase(headless=True)

    try:
        await base.launch()
        page = await base.new_page()

        await base.safe_goto(page, AUTO_RU_BASE_URL)

        # scroll simulation
        for _ in range(3):
            await page.mouse.wheel(0, random.randint(2000, 4000))
            await asyncio.sleep(random.uniform(1.0, 2.0))

        # detect captcha / block
        content = await page.content()
        if "captcha" in content.lower():
            print("[AUTO.RU] captcha detected — skipping")
            return []

        links = await page.eval_on_selector_all(
            "a[href*='/cars/']",
            "els => els.map(e => e.href)"
        )

        uniq = []
        seen = set()

        for url in links:
            if not url:
                continue
            if "/cars/" not in url:
                continue
            if url in seen:
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

    except Exception as e:
        print(f"[AUTO.RU] failed: {e}")
        return []

    finally:
        await base.close()