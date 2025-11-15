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


class Payments(PlaywrightAsync):
    service_name: str = "payments"

    def __init__(self, user_id: str, category: str, **kwargs):
        super().__init__(user_id=user_id, port=config.USERS[user_id]["port"])
        self.category: str = category
        # self.dataset: str = "finance"
        self.dataset: str = "csv"
        self.url: str = config.URL[self.service_name]
        self.task: t.Optional[dict] = None

    @utils.async_exception
    async def download_report(self, report_name: str) -> bool:
        report_rowgroup: ElementHandle = await self.wait_for_selector(
            selector="//kat-table-body[@role='rowgroup']"
        )
        if not report_rowgroup:
            logger.error("not found report rowgroup")
            return False

        report_row: ElementHandle = await report_rowgroup.wait_for_selector(
            selector="kat-table-row[role='row']:first-child"
        )
        if not report_row:
            logger.error("not found report row")
            return False

        download_button: t.Optional[ElementHandle] = None

        for _ in range(180):
            tasks: list = [
                asyncio.create_task(report_row.text_content()),
                asyncio.create_task(
                    report_row.wait_for_selector(selector="kat-button[label='Download CSV']", timeout=10000)
                ),
                asyncio.create_task(
                    report_row.wait_for_selector(selector="//kat-button[@label='Refresh']", timeout=10000)
                ),
                asyncio.create_task(
                    report_row.wait_for_selector(selector="//kat-button[@label='Request Again']", timeout=10000)
                )
            ]

            text_content, download_button, refresh_button, request_button = \
                await asyncio.gather(*tasks, return_exceptions=True)

            if "No Data Available" in text_content:
                download_button = None
                logger.warning("No Data Available")
            elif "Canceled" in text_content:
                download_button = None
                logger.warning("Canceled")
            elif "Failed" in text_content:
                download_button = None
                logger.warning("Failed")

            if isinstance(download_button, ElementHandle):
                logger.info("report was generated")
                break

            if isinstance(refresh_button, ElementHandle):
                logger.warning("trying to refresh")
                await refresh_button.click()

            if isinstance(request_button, ElementHandle):
                logger.warning("trying to request again")
                await request_button.click()

            logger.warning("report is not ready")
            await asyncio.sleep(50)

        if not download_button:
            logger.error("not found report row")
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
    async def get_report(self, period: t.Optional[str] = None, report_name: t.Optional[str] = None) -> bool:
        await asyncio.sleep(5)
        await self.set_date(period=period, service_name=self.service_name)
        await asyncio.sleep(5)

        request_report_button: ElementHandle = await self.wait_for_selector(
            selector="//kat-button[@label='Request Report']"
        )
        if not request_report_button:
            logger.error("not found request report button")
            return False

        await request_report_button.click()
        await asyncio.sleep(5)

        if not await self.download_report(report_name=report_name):
            return False

        return True

    @utils.async_exception
    async def get_monthly_report(self) -> bool:
        periods: list = ["current_month"]
        today: int = datetime.now().day

        if today <= 3:
            periods.append("previous_month")

        for period in periods:
            month, year = await self.get_date(period=period)

            report_name: str = f"{self.category}_{month}_{year}"

            if not await self.get_report(period=period, report_name=report_name):
                continue

            report_path: str = os.path.join(config.reports_path, self.service_name, f"{report_name}.csv")
            if not os.path.isfile(report_path):
                logger.critical(f"report not found :: {report_path}")
                return False

            postgres_db.add_report(
                file_path=report_path,
                dataset=self.dataset,
                table=self.category,
                skip_rows=7
            )

            logger.info(f"report completed :: {self.service_name} :: {report_name}")
            await asyncio.sleep(30)

            if not await self.is_logged(reload=True):
                return False

        return True

    @utils.playwright_initiator
    async def execute(self, playwright: Playwright) -> None:
        self.task: dict = {
            "task_id": str(uuid4()),
            "user_id": self.user_id,
            "service": self.service_name,
            "category": self.category,
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

                if not await self.get_monthly_report():
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
        msg: str = f"service {{status}} :: {self.service_name} :: {self.category} :: {self.user_id}"

        try:
            logger.info(msg.format(status="running"))
            asyncio.run(self.execute())
            logger.info(msg.format(status="finished"))
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info(msg.format(status="stopped"))
