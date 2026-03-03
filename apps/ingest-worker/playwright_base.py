import random
import asyncio
from typing import Optional, List

from playwright.async_api import async_playwright, Browser, Page, BrowserContext


DEFAULT_UA_LIST = [
    # Desktop realistic
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 "
    "(KHTML, like Gecko) Version/17.1 Safari/605.1.15",
]


class PlaywrightBase:
    """
    Production-safe Playwright wrapper:
    - Single browser instance
    - Retry with exponential backoff + jitter
    - Randomized context per page
    - Safe shutdown
    """

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
        if self._browser:
            return  # already launched

        self._pw = await async_playwright().start()

        proxy_cfg = None
        if self.proxies:
            proxy = random.choice(self.proxies)
            proxy_cfg = {"server": proxy}

        self._browser = await self._pw.chromium.launch(
            headless=self.headless,
            proxy=proxy_cfg,
            args=[
                "--no-sandbox",
                "--disable-blink-features=AutomationControlled",
            ],
        )

    async def new_page(self) -> Page:
        if not self._browser:
            raise RuntimeError("Browser not launched")

        ua = random.choice(self.user_agents)

        context: BrowserContext = await self._browser.new_context(
            user_agent=ua,
            viewport={"width": 1366, "height": 768},
            java_script_enabled=True,
        )

        page = await context.new_page()

        # small random delay to mimic human
        await asyncio.sleep(random.uniform(0.5, 1.5))

        return page

    async def close(self):
        try:
            if self._browser:
                await self._browser.close()
        finally:
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
                await page.goto(url, timeout=timeout_ms, wait_until="domcontentloaded")
                await asyncio.sleep(random.uniform(1.0, 2.5))
                return
            except Exception as e:
                last_error = e
                sleep_time = self.backoff_sec * attempt + random.uniform(0.5, 1.5)
                await asyncio.sleep(sleep_time)

        raise RuntimeError(f"[PLAYWRIGHT] Failed to load {url}: {last_error}")