import random
import asyncio
import typing as t
from uuid import uuid4
from pathlib import Path
from urllib.parse import urlparse, urljoin, ParseResult

from playwright.async_api import Playwright, ElementHandle, TimeoutError

try:
    from loggers.logger import logger
    from utils.decorators import utils
    from settings.config import config
    from database.database import db
    from utils.exceptions import BrowserExceptions
    from base.playwright_async import PlaywrightAsync
except ImportError as ie:
    exit(f"{ie} :: {Path(__file__).resolve()}")


class AmazonAds(PlaywrightAsync):
    service_name: str = "amazon_ads"

    def __init__(self, user_id: str, **kwargs):
        super().__init__(user_id=user_id, port=config.USERS[user_id]["port"])
        self.base_domain: t.Optional[str] = None

    @utils.async_exception
    async def random_scroll(self, with_click: bool = True) -> None:
        logger.info("random scrolling")

        for _ in range(random.randint(30, 50)):
            direction = bool(random.randint(0, 2))
            logger.info(f"scrolling direction :: {'DOWN' if direction else 'UP'}")

            for _ in range(random.randint(5, 15)):
                step: int = random.randint(30, 100)
                await self.page.mouse.wheel(0, step if direction else -step)
                await asyncio.sleep(random.uniform(0.2, 0.4))

            if with_click:
                if bool(random.randint(0, 1)):
                    await self.random_click()
                    await asyncio.sleep(random.randint(5, 10))

                    if self.page.url.strip("https://") != self.base_domain:
                        try:
                            await self.page.go_back(timeout=10000, wait_until="networkidle")
                        except TimeoutError:
                            pass

            await asyncio.sleep(random.randint(10, 30))

        for page in self.context.pages:
            if page != self.page:
                try:
                    await page.close()
                    logger.info(f"page closed :: {page.url}")
                except Exception as e:
                    logger.error(e)

    @utils.async_exception
    async def random_click(self) -> None:
        logger.info("random clicking")

        viewport: dict = await self.page.evaluate("() => ({ width: window.innerWidth, height: window.innerHeight })")
        margin_x: int = viewport["width"] * 0.05
        margin_y: int = viewport["height"] * 0.05
        x: int = random.uniform(margin_x, viewport["width"] - margin_x)
        y: int = random.uniform(margin_y, viewport["height"] - margin_y)

        await self.page.mouse.move(x, y)
        await asyncio.sleep(random.uniform(0.1, 0.3))
        await self.page.mouse.click(x, y)

        # elements: list = await self.page.query_selector_all('span:not(a span), p:not(a p)')
        # visible_elements: list = [
        #     el for el in elements if await el.is_visible() and await self.run_js("element_viewport.js", el)
        # ]
        #
        # if visible_elements:
        #     await self.click(element=random.choice(visible_elements), focus=False, offset=False)
        # else:
        #     logger.warning("not found visible elements")

    @utils.async_exception
    async def navigate_internal_links(self) -> None:
        logger.info("navigating internal links")

        elements: list = await self.page.query_selector_all('a[href]')
        visible_elements = list()

        for el in elements:
            is_visible: bool = await el.is_visible() and await self.run_js("link_viewport.js", el)
            if not is_visible:
                continue

            href: str = await el.get_attribute("href")
            if not href:
                continue

            if (
                    href.startswith("#") or
                    href.startswith("mailto:") or
                    href.startswith("tel:") or
                    href.endswith((".pdf", ".jpg", ".png", ".zip"))
            ):
                continue

            full_url: str = href if href.startswith("http") else urljoin(self.page.url, href)
            parsed_link: ParseResult = urlparse(full_url)

            if parsed_link.netloc == self.base_domain:
                visible_elements.append(el)

        if visible_elements:
            target_link: ElementHandle = random.choice(visible_elements)
            try:
                await self.click(element=target_link)
                await self.page.wait_for_load_state(timeout=60000)
                await self.random_scroll(with_click=False)

                if self.page.url != self.base_domain:
                    await self.page.go_back()
            except Exception as e:
                logger.error(e)
            finally:
                visible_elements.remove(target_link)
        else:
            logger.info("not found visible elements")

    @utils.playwright_initiator
    async def execute(self, playwright: Playwright) -> None:
        task: dict = {
            "task_id": str(uuid4()),
            "user_id": self.user_id,
            "service": self.service_name,
            "status": "started"
        }
        await db.update_task(task=task)

        try:
            if not await self.connect_cdp_session(playwright=playwright):
                task["status"] = "failed"
                raise BrowserExceptions.ConnectionError()

            await asyncio.sleep(5)

            is_opened: bool = False
            for _ in range(3):
                url: str = config.USERS[self.user_id]["ads"]
                try:
                    await self.page.goto(url=url, timeout=30000)
                    is_opened: bool = True
                    logger.info(f"page is opened :: {url}")
                    break
                except (BrowserExceptions.PageError, TimeoutError):
                    logger.warning(f"page is not opened :: {url}")
                    continue

            if not is_opened:
                task["status"] = "failed"
                raise BrowserExceptions.PageError()

            parsed_url: ParseResult = urlparse(self.page.url)
            self.base_domain: str = parsed_url.netloc

            await asyncio.sleep(random.randint(5, 10))
            await self.random_scroll()
        finally:
            if task["status"] == "started":
                task["status"] = "stopped"
            await db.update_task(task=task)

    @utils.exception
    def run(self) -> None:
        msg: str = f"service {{status}} :: {self.service_name} :: {self.user_id}"

        try:
            logger.info(msg.format(status="running"))
            asyncio.run(self.execute())
            logger.info(msg.format(status="finished"))
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info(msg.format(status="stopped"))
