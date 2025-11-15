import os
import asyncio
import typing as t
from uuid import uuid4
from pathlib import Path
from datetime import datetime

from playwright.async_api import Playwright, ElementHandle, Locator, Download, TimeoutError

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


class Shipments(PlaywrightAsync):
    service_name: str = "shipments"

    def __init__(self, user_id: str, **kwargs):
        super().__init__(user_id=user_id, port=config.USERS[user_id]["port"])
        # self.dataset: str = "logist"
        self.dataset: str = "csv"
        self.url: str = config.URL[self.service_name]
        self.task: t.Optional[dict] = None

    @utils.async_exception
    async def download_report(self, report_name: str) -> bool:
        download_button: ElementHandle = await self.page.wait_for_selector(
            selector="//kat-link[@class='export-table-link']"
        )
        if not download_button:
            logger.error("not found download button")
            return False

        try:
            async with self.page.expect_download() as download_info:
                await download_button.click()

            download: Download = await download_info.value
            # report_path: str = os.path.join(config.reports_path, category, download.suggested_filename)
            report_path: str = os.path.join(config.reports_path, self.service_name, f"{report_name}.csv")
            await download.save_as(path=report_path)
            await asyncio.sleep(10)
        except Exception as e:
            logger.error(e)

        return True

    @utils.async_exception
    async def get_report(self) -> bool:
        period: str = "previous_month"

        await self.run_js("pick_date_range.js")
        await asyncio.sleep(5)

        date_picker: Locator = self.page.locator("kat-date-range-picker[start-label='From Date']").nth(index=1)
        await self.set_date(element=date_picker, period=period, service_name=self.service_name)
        await asyncio.sleep(5)

        apply_button: ElementHandle = await self.wait_for_selector(
            selector="kat-button[class='date-range-apply-button']"
        )
        if not apply_button:
            logger.error("not found apply button")
            return False

        await apply_button.click()
        await asyncio.sleep(5)

        results_range: ElementHandle = await self.wait_for_selector(selector="//kat-dropdown[@value='25']")
        if results_range:
            if not await self.scroll_to_element(element=results_range):
                await results_range.scroll_into_view_if_needed(timeout=10000)

            await asyncio.sleep(5)
            await self.run_js("set_results_range.js", "25")

        await asyncio.sleep(10)

        for page in range(10):
            report_name: str = f"{self.service_name}_{datetime.now().strftime('%d_%m_%Y')}_page{page+1}"
            if not await self.download_report(report_name=report_name):
                return False

            await asyncio.sleep(5)

            report_path: str = os.path.join(config.reports_path, self.service_name, f"{report_name}.csv")
            if not os.path.isfile(report_path):
                logger.critical(f"report not found :: {report_path}")
                return False

            # if not big_query.add_report(
            #     file_path=report_path,
            #     dataset=self.dataset,
            #     table=self.service_name,
            #     add_date=True
            # ):
            #     return False
            if not postgres_db.add_report(
                file_path=report_path,
                dataset=self.dataset,
                table=self.service_name,
                add_date=True
            ):
                continue

            logger.info(f"report completed :: {self.service_name} :: {report_name}")

            await asyncio.sleep(5)

            next_button = await self.wait_for_selector(selector="//a[text()='Next']", timeout=10000)
            if next_button:
                if not await self.scroll_to_element(element=next_button):
                    await next_button.scroll_into_view_if_needed(timeout=10000)

                await asyncio.sleep(5)
                await next_button.click()
            else:
                logger.info("all pages were processing")
                break

            await asyncio.sleep(10)

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
