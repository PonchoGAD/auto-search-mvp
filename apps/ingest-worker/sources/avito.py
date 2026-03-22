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

        # Собираем не только ссылки, но и весь текст сниппета Avito
        items_data = await page.evaluate(
            """() => {
                let els = Array.from(document.querySelectorAll("a[href*='/avtomobili/']"));
                return els.map(e => {
                    let container = e.closest('[data-marker="item"]') || e.parentElement.parentElement;
                    return { url: e.href, text: container ? container.innerText : e.innerText };
                });
            }"""
        )

        uniq =[]
        seen = set()

        for ad in items_data:
            url = ad.get("url", "")
            text = ad.get("text", "")
            if not url or "/avtomobili/" not in url or url in seen:
                continue

            seen.add(url)
            title = text[:80].replace("\n", " ").strip() if text else url.split("/")[-1]
            content = text.replace("\n", " ").strip() if text else url
            
            uniq.append({
                "source": "avito.ru",
                "source_url": url,
                "title": title,
                "content": content,
            })

            if len(uniq) >= limit:
                break

        return uniq

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