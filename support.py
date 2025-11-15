import os
import asyncio
import typing as t
from uuid import uuid4
from pathlib import Path
from datetime import datetime

from playwright.async_api import Playwright, ElementHandle, Download, TimeoutError

try:
    from loggers.logger import logger
    from utils.decorators import utils
    from settings.config import config
    from utils.exceptions import BrowserExceptions
    from base.playwright_async import PlaywrightAsync
    from database.database import db
    from database.big_query import big_query
    from database.postgres_db import postgres_db
except ImportError as ie:
    exit(f"{ie} :: {Path(__file__).resolve()}")


class Support(PlaywrightAsync):
    service_name: str = "support"

    def __init__(self, user_id: str, **kwargs):
        super().__init__(user_id=user_id, port=config.USERS[user_id]["port"])
        # self.dataset: str = "logist"
        self.dataset: str = "csv"
        self.url: str = config.URL[self.service_name]
        self.task: t.Optional[dict] = None

    @utils.async_exception
    async def download_report(self, report_name: str) -> bool:
        download_button: ElementHandle = await self.wait_for_selector(
            selector="//kat-button[@class='export-report-button']"
        )
        if not download_button:
            logger.error("not found download button")
            return False

        if not await self.scroll_to_element(element=download_button):
            await download_button.scroll_into_view_if_needed(timeout=10000)

        await asyncio.sleep(5)

        try:
            async with self.page.expect_download() as download_info:
                await download_button.click()

            download: Download = await download_info.value
            # report_path: str = os.path.join(config.reports_path, category, download.suggested_filename)
            report_path: str = os.path.join(config.reports_path, self.service_name, f"{report_name}.xlsx")
            await download.save_as(path=report_path)
            await asyncio.sleep(10)
        except Exception as e:
            logger.error(e)

        return True

    @utils.async_exception
    async def get_report(self) -> bool:
        results_range: ElementHandle = await self.wait_for_selector(selector="//kat-dropdown[@value='10']")
        if results_range:
            if not await self.scroll_to_element(element=results_range):
                await results_range.scroll_into_view_if_needed(timeout=10000)

            await asyncio.sleep(5)
            await self.run_js("set_results_range.js", "10")

        current_month, year = await self.get_date(period="current_month", month_name=True)
        previous_month, year = await self.get_date(period="previous_month", month_name=True)

        await asyncio.sleep(10)

        for page in range(10):
            is_current = is_previous = None

            for element in await self.page.query_selector_all("tr"):
                text: str = await element.text_content()
                if current_month[:3] in text:
                    is_current: bool = True
                elif previous_month[:3] in text:
                    is_previous: bool = True

            if is_previous:
                report_name: str = f"{self.service_name}_{datetime.now().strftime('%d_%m_%Y')}_page{page+1}"

                if not await self.download_report(report_name=report_name):
                    return False

                report_path: str = os.path.join(config.reports_path, self.service_name, f"{report_name}.xlsx")
                if not os.path.isfile(report_path):
                    logger.critical(f"report not found :: {report_path}")
                    return False

                # if not big_query.add_report(
                #     file_path=report_path,
                #     dataset=self.dataset,
                #     table=self.service_name
                # ):
                #     return False
                if not postgres_db.add_report(
                    file_path=report_path,
                    dataset=self.dataset,
                    table=self.service_name
                ):
                    continue

                logger.info(f"report completed :: {self.service_name} :: {report_name}")

                await asyncio.sleep(5)

            if is_current or is_previous:
                next_button: ElementHandle = await self.wait_for_selector(
                    # selector="(//kat-pagination)[2]"
                    selector="//kat-pagination"
                )
                if not next_button:
                    logger.error("not found pagination")
                    break

                if not await self.scroll_to_element(element=next_button):
                    await next_button.scroll_into_view_if_needed(timeout=10000)

                await asyncio.sleep(5)

                # if not await self.run_js(js_file="pagination.js"):
                #     logger.info("all pages were processing")
                #     break
                pagination: list = await next_button.query_selector_all("li")
                for p in pagination:
                    if str(page+2) in await p.text_content():
                        await p.click()
                        break
                    # logger.info(await p.text_content())

                await asyncio.sleep(5)
            else:
                logger.info("all pages were processing")
                break

        return True

    @utils.playwright_initiator
    async def execute(self, playwright: Playwright) -> None:
        self.task: dict = {
            "task_id": str(uuid4()),
            "user_id": self.user_id,
            "service": self.service_name,
            "status": "started"
        }
        await db.update_task(task=self.task)

        try:
            if not await self.connect_cdp_session(playwright=playwright):
                self.task["status"] = "failed"
                raise BrowserExceptions.ConnectionError()

            is_logged: bool = await self.is_logged()
            if is_logged is None:
                raise BrowserExceptions.PageError()
            elif not is_logged:
                if await self.login():
                    is_logged: bool = True

            if is_logged:
                is_opened: bool = False
                for _ in range(3):
                    try:
                        await self.page.goto(url=self.url, timeout=60000)
                        is_opened: bool = True
                        logger.info(f"page is opened :: {self.url}")
                        break
                    except (BrowserExceptions.PageError, TimeoutError):
                        logger.warning(f"page is not opened :: {self.url}")
                        continue

                if not is_opened:
                    self.task["status"] = "failed"
                    raise BrowserExceptions.PageError()

                if not await self.get_report():
                    self.task["status"] = "failed"
            else:
                self.task["status"] = "failed"
                logger.warning("login failed")
        finally:
            if self.task["status"] == "started":
                self.task["status"] = "stopped"
            await db.update_task(task=self.task)

    @utils.exception
    def run(self) -> None:
        msg: str = f"service {{status}} :: {self.service_name} :: {self.user_id}"

        try:
            logger.info(msg.format(status="running"))
            asyncio.run(self.execute())
            logger.info(msg.format(status="finished"))
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info(msg.format(status="stopped"))
