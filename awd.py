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


class Awd(PlaywrightAsync):
    service_name: str = "awd"

    def __init__(self, user_id: str, category: str, **kwargs):
        super().__init__(user_id=user_id, port=config.USERS[user_id]["port"])
        self.category: str = category
        # self.dataset: str = "logist"
        self.dataset: str = "csv"
        self.url: str = config.URL[self.service_name].get(self.category, config.URL[self.service_name]["monthly"])
        self.task: t.Optional[dict] = None
        self.file_type: str = "csv" if self.category in ["inventory", "shipment_awd_inbound"] else "xlsx"

    @utils.async_exception
    async def download_report(self, report_name: str) -> bool:
        if self.category == "inventory":
            dropdown_button: ElementHandle = await self.wait_for_selector(
                selector="//kat-dropdown-button[@id='downloadDropdown']"
            )
            if not dropdown_button:
                logger.error("not found dropdown button")
                return False

            await asyncio.sleep(5)

            indicator: ElementHandle = await dropdown_button.wait_for_selector(
                selector="div[class='button-group-header'] button[class='indicator']"
            )
            if not indicator:
                logger.error("not found indicator button")
                return False

            await indicator.click()
            await asyncio.sleep(5)

            download_button: ElementHandle = await self.page.wait_for_selector(
                selector="button[data-action='DOWNLOAD_INVENTORY_DATA']"
            )
            if not download_button:
                logger.error("not found download button")
                return False
        else:
            is_generated: bool = False
            for _ in range(36):
                request_button: ElementHandle = await self.wait_for_selector(
                    selector="//kat-button[@label='Request .csv Download']" if self.category == "shipment_awd_inbound"
                    else "//kat-button[@label='Request Download']"
                )
                if not request_button:
                    logger.error("not found request button")
                    await asyncio.sleep(10)
                    continue

                await asyncio.sleep(5)

                if not await self.scroll_to_element(element=request_button):
                    await request_button.scroll_into_view_if_needed(timeout=10000)

                await request_button.click()

                modal_button = await self.wait_for_selector(
                    selector="kat-modal[visible='true'] >>> div.container > div.dialog > div.header > button.close"
                )

                if modal_button:
                    logger.warning('found modal window')
                    await modal_button.click()
                    await asyncio.sleep(300)
                else:
                    logger.info("not found modal window")
                    is_generated: bool = True
                    break

            if not is_generated:
                logger.error("report was not generated")
                return False

            await asyncio.sleep(10)

            report_row: ElementHandle = await self.wait_for_selector(
                selector="(//kat-table-row[@role='row'][1])[last()]"
            )
            if not report_row:
                logger.error("not found report row")
                return False

            download_button: t.Optional[ElementHandle] = None

            for _ in range(180):
                tasks: list = [
                    asyncio.create_task(report_row.text_content()),
                    asyncio.create_task(
                        report_row.wait_for_selector(selector="kat-button[label='Download']", timeout=10000)
                    )
                ]

                text_content, download_button = await asyncio.gather(*tasks, return_exceptions=True)

                if "No Data Available" in text_content or "No Shipment Found" in text_content:
                    download_button = None
                    logger.warning("no data available")
                    break

                if isinstance(download_button, ElementHandle):
                    logger.info("report was generated")
                    break

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
            report_path: str = os.path.join(config.reports_path, self.service_name, f"{report_name}.{self.file_type}")
            await download.save_as(path=report_path)
            await asyncio.sleep(10)
        except Exception as e:
            logger.error(e)

        return True

    @utils.async_exception
    async def get_report(self) -> bool:
        await asyncio.sleep(5)

        if self.category == "shipment_awd_inbound":
            await self.run_js("set_exact_date.js", "dateRangeDropDown")
            await asyncio.sleep(5)

            date_picker: Locator = self.page.locator("kat-date-range-picker[start-label='Start Date']")
            await self.set_date(element=date_picker, period="current_month", category=self.category)
            await asyncio.sleep(5)
        elif self.category != "inventory":
            categories: dict = {
                "storage": "//kat-link[@label='AWD Monthly Storage Fee Report']",
                "processing": "//kat-link[@label='AWD Monthly Processing Fee Report']",
                "transportation": "//kat-link[@label='AWD Monthly Transportation Fee Report']"
            }

            category_button: ElementHandle = await self.wait_for_selector(selector=categories[self.category])
            if not category_button:
                logger.error("not found category button")
                return False

            await category_button.click()
            await asyncio.sleep(10)

            period: str = "previous_month"
            await self.set_date(period=period, service_name=self.service_name)

        report_name: str = f"{self.category}_{datetime.now().strftime('%d_%m_%Y')}"

        if not await self.download_report(report_name=report_name):
            return False

        report_path: str = os.path.join(config.reports_path, self.service_name, f"{report_name}.{self.file_type}")
        if not os.path.isfile(report_path):
            logger.critical(f"report not found :: {report_path}")
            return False

        # if not big_query.add_report(
        #     file_path=report_path,
        #     dataset=f"{self.dataset}_{self.category}",
        #     table=report_name,
        #     skip_rows=3 if self.category == "inventory" else 0,
        #     add_date=True if self.category == "storage" else False,
        # ):
        #     return False
        if not postgres_db.add_report(
            file_path=report_path,
            dataset=self.dataset,
            table=self.category,
            skip_rows=3 if self.category == "inventory" else 0,
            add_date=True
        ):
            return False

        logger.info(f"report completed :: {self.service_name} :: {report_name}")
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
        msg: str = f"service {{status}} :: {self.service_name} :: {self.category} :: {self.user_id}"

        try:
            logger.info(msg.format(status="running"))
            asyncio.run(self.execute())
            logger.info(msg.format(status="finished"))
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info(msg.format(status="stopped"))
