import random
import asyncio
from typing import Optional, List

from playwright.async_api import async_playwright, Browser, Page


DEFAULT_UA_LIST = [
    # Desktop
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
]


class PlaywrightBase:
    def __init__(
        self,
        proxies: Optional[List[str]] = None,
        user_agents: Optional[List[str]] = None,
        retries: int = 3,
        backoff_sec: float = 2.0,
        headless: bool = True,
    ):
        self.proxies = proxies or []
        self.user_agents = user_agents or DEFAULT_UA_LIST
        self.retries = retries
        self.backoff_sec = backoff_sec
        self.headless = headless

        self._pw = None
        self._browser: Optional[Browser] = None

    async def launch(self):
        self._pw = await async_playwright().start()

        proxy_cfg = None
        if self.proxies:
            proxy = random.choice(self.proxies)
            proxy_cfg = {"server": proxy}

        self._browser = await self._pw.chromium.launch(
            headless=self.headless,
            proxy=proxy_cfg,
        )

    async def new_page(self) -> Page:
        if not self._browser:
            raise RuntimeError("Browser not launched")

        ua = random.choice(self.user_agents)

        context = await self._browser.new_context(
            user_agent=ua,
            viewport={"width": 1366, "height": 768},
        )

        page = await context.new_page()
        return page

    async def close(self):
        if self._browser:
            await self._browser.close()
        if self._pw:
            await self._pw.stop()

    async def safe_goto(
        self,
        page: Page,
        url: str,
        timeout_ms: int = 60000,
    ):
        last_error = None

        for attempt in range(1, self.retries + 1):
            try:
                await page.goto(url, timeout=timeout_ms)
                return
            except Exception as e:
                last_error = e
                await asyncio.sleep(self.backoff_sec * attempt)

        raise RuntimeError(f"Failed to load {url}: {last_error}")
