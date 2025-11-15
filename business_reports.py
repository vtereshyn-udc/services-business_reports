import os
import asyncio
import typing as t
from uuid import uuid4
from pathlib import Path
from datetime import datetime, timedelta

import pandas as pd
from playwright.async_api import Playwright, ElementHandle, Download, TimeoutError

try:
    from loggers.logger import logger
    from utils.decorators import utils
    from settings.config import config
    from utils.exceptions import BrowserExceptions
    from base.playwright_async import PlaywrightAsync
    from database.database import db
    from utils.google_sheets import gs
    from loggers.cleaner import clean_logs
    from database.big_query import big_query
    from database.postgres_db import postgres_db
except ImportError as ie:
    exit(f"{ie} :: {Path(__file__).resolve()}")


class BusinessReports(PlaywrightAsync):
    service_name: str = "business_reports"

    def __init__(self, user_id: str, category: str, **kwargs):
        super().__init__(user_id=user_id, port=config.USERS[user_id]["port"])
        self.category: str = category
        self.url: str = config.URL[self.service_name]
        self.task: t.Optional[dict] = None

    @utils.async_exception
    async def download_report(self, report_name: str) -> bool:
        download_button: ElementHandle = await self.page.wait_for_selector(
            selector="//kat-button[@data-testid='BR_TABLE_BAR_DOWNLOAD_BUTTON']"
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
        categories: dict = {
            "brand_performance": "//span[text()='Brand Performance']",
            "sales_traffic_daily": "(//span[text()='Detail Page Sales and Traffic'])[2]",
            "sales_traffic_weekly": "(//span[text()='Detail Page Sales and Traffic'])[2]",
        }

        category_button: ElementHandle = await self.wait_for_selector(selector=categories[self.category])
        if not category_button:
            logger.error("not found category button")
            return False

        # //kat-alert[contains(@description, 'your data could not be loaded')]
        await category_button.click()
        await asyncio.sleep(10)

        await self.run_js("set_date_range.js")
        await asyncio.sleep(10)

        if "sales_traffic" in self.category:
            await self.run_js("set_dashboard_view.js")
            await asyncio.sleep(10)

        for day in ["1_days_ago", "3_days_ago"]:
            period: str = day if self.category == "sales_traffic_daily" else "last_week"
            date_period: str = await self.set_date(period=period, service_name=self.service_name)

            report_name: str = f"{self.category}_{datetime.now().strftime('%d_%m_%Y')}"
            if self.category == "sales_traffic_daily":
                report_name += day

            if not await self.download_report(report_name=report_name):
                return False

            report_path: str = os.path.join(config.reports_path, self.service_name, f"{report_name}.csv")
            if not os.path.isfile(report_path):
                logger.critical(f"report not found :: {report_path}")
                return False

            # if not big_query.add_report(
            #     file_path=report_path,
            #     dataset=self.service_name,
            #     table=self.category,
            #     period=date_period if self.category != "sales_traffic_daily" else None,
            #     # add_date=True if self.category == "sales_traffic_daily" else False,
            #     custom_date=date_period.split("-")[0] if self.category == "sales_traffic_daily" else None
            # ):
            #     return False

            custom_date: str = datetime.strptime(date_period.split("-")[0], "%m/%d/%Y").strftime("%Y-%m-%d")
            postgres_db.add_report(
                file_path=report_path,
                dataset=self.service_name,
                table=self.category,
                period=date_period if self.category != "sales_traffic_daily" else None,
                # add_date=True if self.category == "sales_traffic_daily" else False,
                custom_date=custom_date if self.category == "sales_traffic_daily" else None
            )

            logger.info(f"report completed :: {self.service_name} :: {report_name}")

            if self.category != "sales_traffic_daily":
                break

            await asyncio.sleep(15)

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
            if self.category == "competitors":
                df: pd.DataFrame = gs.worksheet_to_dataframe(category=self.category)
                if df.empty:
                    logger.error("not found competitors report")
                    return False

                df: pd.DataFrame = df.loc[:, ~df.columns.duplicated()]

                now: datetime = datetime.now()
                start_date: str = (now - timedelta(days=7)).strftime("%m/%d/%Y")
                end_date: str = now.strftime("%m/%d/%Y")
                period: str = f"{start_date}-{end_date}"

                # big_query.add_column(df=df, period=period)
                postgres_db.add_column(df=df, period=period)

                # if big_query.update_data(df=df, dataset=self.service_name, table=self.category):
                #     logger.info(f"report was added :: {self.service_name}.{self.category}")
                if postgres_db.update_data(df=df, dataset=self.service_name, table=self.category):
                    logger.info(f"report was added :: {self.service_name}.{self.category}")

            else:
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
            clean_logs()

    @utils.exception
    def run(self) -> None:
        msg: str = f"service {{status}} :: {self.service_name} :: {self.category} :: {self.user_id}"

        try:
            logger.info(msg.format(status="running"))
            asyncio.run(self.execute())
            logger.info(msg.format(status="finished"))
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info(msg.format(status="stopped"))
