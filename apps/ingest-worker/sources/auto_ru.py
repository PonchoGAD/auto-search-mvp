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

        # Собираем не только ссылки, но и весь текст карточки (пробег, год, двигатель)
        items_data = await page.evaluate(
            """() => {
                let els = Array.from(document.querySelectorAll("a[href*='/cars/']"));
                return els.map(e => {
                    let container = e.closest('div[class*="ListingItem"]') || e.closest('div[data-stat-card]') || e.parentElement.parentElement;
                    return { url: e.href, text: container ? container.innerText : e.innerText };
                });
            }"""
        )

        uniq =[]
        seen = set()

        for ad in items_data:
            url = ad.get("url", "")
            text = ad.get("text", "")
            if not url or "/cars/" not in url or url in seen:
                continue

            seen.add(url)
            title = text[:80].replace("\n", " ").strip() if text else url.split("/")[-1]
            content = text.replace("\n", " ").strip() if text else url
            
            uniq.append({
                "source": "auto.ru",
                "source_url": url,
                "title": title,
                "content": content,
            })

            if len(uniq) >= limit:
                break

        return uniq

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