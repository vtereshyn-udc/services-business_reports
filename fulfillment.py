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


class Fulfillment(PlaywrightAsync):
    service_name: str = "fulfillment"

    def __init__(self, user_id: str, category: str, **kwargs):
        super().__init__(user_id=user_id, port=config.USERS[user_id]["port"])
        self.category: str = category
        # self.dataset: str = "logist"
        self.dataset: str = "csv"
        self.url: str = config.URL[self.service_name]
        self.task: t.Optional[dict] = None

    @utils.async_exception
    async def download_report(self, report_name: str) -> bool:
        is_generated: bool = False
        for _ in range(36):
            request_button: ElementHandle = await self.wait_for_selector(
                selector="//kat-button[@class='download-report-page-kat-button-primary']"
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
                    report_row.wait_for_selector(selector="kat-button[label='Download']", timeout=10000)
                )
            ]

            text_content, download_button = await asyncio.gather(*tasks, return_exceptions=True)

            if "No Data Available" in text_content or "Canceled" in text_content:
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
            report_path: str = os.path.join(config.reports_path, self.service_name, f"{report_name}.csv")
            await download.save_as(path=report_path)
            await asyncio.sleep(10)
        except Exception as e:
            logger.error(e)

        return True

    @utils.async_exception
    async def get_report(self, period: t.Optional[str] = None, report_name: t.Optional[str] = None) -> bool:
        if self.category in [
            "fba_inventory", "manage_fba_inventory", "reimbursements", "inventory_surcharge", "promotions"
        ]:
            selector: t.Optional[str] = None
            if self.category in ["reimbursements", "inventory_surcharge"]:
                selector: str = "(//label[@class='reports-nav-show-link'])[3]"
            elif self.category in ["fba_inventory", "manage_fba_inventory"]:
                selector: str = "//label[@class='reports-nav-show-link']"
            elif self.category == "promotions":
                selector: str = "(//label[@class='reports-nav-show-link'])[2]"


            show_button: ElementHandle = await self.wait_for_selector(selector=selector)
            if not show_button:
                logger.error("not found show button")
                return False

            # if not await self.scroll_to_element(element=show_button):
            try:
                await show_button.scroll_into_view_if_needed(timeout=10000)
            except Exception:
                await self.save_screenshot(selector="scroll_into_view")

            await asyncio.sleep(5)
            await show_button.click()
            await asyncio.sleep(5)

        categories: dict = {
            "fulfilled_shipments": "//span[text()='Amazon Fulfilled Shipments']",
            "fba_inventory": "//span[text()='FBA Inventory']",
            "manage_fba_inventory": "//span[text()='Manage FBA Inventory']",
            "replacements": "//span[text()='Replacements']",
            "reimbursements": "//span[text()='Reimbursements']",
            "order_detail": "//span[text()='Removal Order Detail']",
            "shipment_detail": "//span[text()='Removal Shipment Detail']",
            "storage_fees": "//span[text()='Monthly Storage Fees']",
            "inventory_surcharge": "//span[text()='Aged Inventory Surcharge report']",
            "promotions": "//span[text()='Promotions']",
            "fba_customer_returns": "//span[text()='FBA customer returns']",
        }

        category_button: ElementHandle = await self.wait_for_selector(selector=categories[self.category])
        if not category_button:
            logger.error("not found category button")
            return False

        # if not await self.scroll_to_element(element=category_button):
        await asyncio.sleep(10)
        await category_button.scroll_into_view_if_needed(timeout=10000)

        await asyncio.sleep(5)
        await category_button.click()
        await asyncio.sleep(5)

        if self.category in ["fulfilled_shipments", "replacements", "reimbursements", "order_detail", "shipment_detail"]:
            download_button: ElementHandle = await self.wait_for_selector(selector="//a[@id='reportpage_download_tab']")
            if not download_button:
                logger.error("not found download button")
                return False

            if not await self.scroll_to_element(element=download_button):
                await download_button.scroll_into_view_if_needed(timeout=10000)

            await asyncio.sleep(5)
            await download_button.click()
            await asyncio.sleep(5)

            # await self.run_js("set_exact_date.js")

            await self.page.click("kat-dropdown.daily-time-picker-kat-dropdown-normal >> div[part='dropdown-header']")
            await self.page.wait_for_selector("kat-dropdown.daily-time-picker-kat-dropdown-normal >> kat-option")

            options = await self.page.query_selector_all("kat-dropdown.daily-time-picker-kat-dropdown-normal >> kat-option")
            for opt in options:
                text = (await opt.inner_text()).strip()
                if text == "Exact dates":
                    await opt.click()
                    break

            await asyncio.sleep(5)

            date_picker: ElementHandle = await self.wait_for_selector(
                selector="//kat-date-range-picker[@id='daily-time-picker-kat-date-range-picker']"
            )
            if not date_picker:
                logger.error("not found date picker")
                return False

            await asyncio.sleep(5)
            await self.set_date(element=date_picker, period=period)
            await asyncio.sleep(5)

        if self.category in ["storage_fees", "inventory_surcharge"]:
            download_button: ElementHandle = await self.wait_for_selector(selector="//a[@id='reportpage_download_tab']")
            if not download_button:
                logger.error("not found download button")
                return False

            if not await self.scroll_to_element(element=download_button):
                await download_button.scroll_into_view_if_needed(timeout=10000)

            await asyncio.sleep(5)
            await download_button.click()
            await asyncio.sleep(5)

            await self.run_js("set_fulfillment_month.js")
            await asyncio.sleep(5)

            await self.run_js("set_fulfillment_year.js")
            await asyncio.sleep(5)

        if self.category in ["promotions", "fba_customer_returns"]:
            download_button: ElementHandle = await self.wait_for_selector(selector="//a[@id='reportpage_download_tab']")
            if not download_button:
                logger.error("not found download button")
                return False

            if not await self.scroll_to_element(element=download_button):
                await download_button.scroll_into_view_if_needed(timeout=10000)

            await asyncio.sleep(5)
            await download_button.click()

        if not report_name:
            report_name: str = self.category

        if not await self.download_report(report_name=report_name):
            return False

        return True

    @utils.async_exception
    async def get_daily_report(self) -> bool:
        if not await self.get_report():
            return False

        report_path: str = os.path.join(config.reports_path, self.service_name, f"{self.category}.csv")
        if not os.path.isfile(report_path):
            logger.critical(f"report not found :: {report_path}")
            return False

        # if not big_query.add_report(
        #     file_path=report_path,
        #     dataset=self.dataset,
        #     table=self.category,
        #     add_date=True if self.category == "manage_fba_inventory" else False
        # ):
        #     return False
        if not postgres_db.add_report(
            file_path=report_path,
            dataset=self.dataset,
            table=self.category,
            add_date=True if self.category == "manage_fba_inventory" else False
        ):
            return False

        logger.info(f"report completed :: {self.service_name} :: {self.category}")
        return True

    @utils.async_exception
    async def get_monthly_report(self) -> bool:
        previous_month_categories: list = ["reimbursements", "storage_fees", "inventory_surcharge"]

        periods: list = ["current_month" if self.category not in previous_month_categories else "previous_month"]
        today: int = datetime.now().day

        if today <= 3 and self.category not in previous_month_categories:
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

            # if not big_query.add_report(
            #     file_path=report_path,
            #     dataset=f"{self.dataset}_{self.category}",
            #     table=report_name,
            # ):
            #     return False
            date = datetime.now().date().isoformat()
            postgres_db.add_report(
                file_path=report_path,
                dataset=self.dataset,
                table=self.category,
                add_date=date if self.category == "storage_fees" else False
            )

            logger.info(f"report completed :: {self.service_name} :: {report_name}")
            await asyncio.sleep(30)

            if not await self.is_logged(reload=True):
                return False

        return True

    @utils.async_exception
    async def get_yearly_report(self) -> bool:
        period: str = "full_year"
        month = year = None
        now: datetime = datetime.now()

        if now.month > 1:
            year: int = now.year
            month: int = now.month - 1
        else:
            year: int = now.year - 1
            month: int = 12

        report_name: str = f"{self.category}_{month}_{year}"

        if not await self.get_report(period=period, report_name=report_name):
            return False

        report_path: str = os.path.join(config.reports_path, self.service_name, f"{report_name}.csv")
        if not os.path.isfile(report_path):
            logger.critical(f"report not found :: {report_path}")
            return False

        # if not big_query.add_report(
        #     file_path=report_path,
        #     dataset=self.dataset,
        #     table=self.category
        # ):
        #     return False
        if not postgres_db.add_report(
            file_path=report_path,
            dataset=self.dataset,
            table=self.category
        ):
            return False

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

                if self.category in ["manage_fba_inventory", "fba_inventory", "promotions", "fba_customer_returns"]:
                    if not await self.get_daily_report():
                        self.task["status"] = "failed"
                elif self.category in [
                "replacements", "fulfilled_shipments", "reimbursements", "storage_fees", "inventory_surcharge"
                ]:
                    if not await self.get_monthly_report():
                        self.task["status"] = "failed"
                if self.category in ["order_detail", "shipment_detail"]:
                    if not await self.get_yearly_report():
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
