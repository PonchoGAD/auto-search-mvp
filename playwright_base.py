# playwright_base.py
from playwright.async_api import async_playwright
import random
import asyncio

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)",
]

class PlaywrightBase:
    def __init__(self, proxy: str | None = None):
        self.proxy = proxy

    async def launch(self):
        self.pw = await async_playwright().start()
        self.browser = await self.pw.chromium.launch(
            headless=True,
            proxy={"server": self.proxy} if self.proxy else None,
        )

    async def new_page(self):
        context = await self.browser.new_context(
            user_agent=random.choice(USER_AGENTS),
            viewport={"width": 1280, "height": 800},
        )
        return await context.new_page()

    async def close(self):
        await self.browser.close()
        await self.pw.stop()

    async def with_retry(self, fn, retries: int = 3, delay: float = 2.0):
        for attempt in range(retries):
            try:
                return await fn()
            except Exception:
                if attempt == retries - 1:
                    raise
                await asyncio.sleep(delay * (attempt + 1))
