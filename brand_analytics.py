import os
import random
import re
import asyncio
import typing as t
import pandas as pd
from uuid import uuid4
from pathlib import Path

from playwright.async_api import Playwright, Page, ElementHandle, Locator, Download, TimeoutError

try:
    from loggers.logger import logger
    from utils.decorators import utils
    from settings.config import config
    from utils.exceptions import BrowserExceptions
    from base.playwright_async import PlaywrightAsync
    from database.database import db
    from utils.google_sheets import gs
    from database.big_query import big_query
    from database.postgres_db import postgres_db
except ImportError as ie:
    exit(f"{ie} :: {Path(__file__).resolve()}")


class BrandAnalytics(PlaywrightAsync):
    service_name: str = "brand_analytics"

    def __init__(self, user_id: str, category: str, **kwargs):
        super().__init__(user_id=user_id, port=config.USERS[user_id]["port"])
        self.user_id: str = user_id
        self.category: str = category
        self.url: str = config.URL[self.service_name]
        self.task: t.Optional[dict] = None
        self.week_num: t.Optional[int] = None

    @utils.async_exception
    async def is_new_week(self) -> bool:
        await self.page.locator("#weekly-week").wait_for(timeout=10000)
        title: str = await self.run_js(js_file="get_title.js")

        if not title:
            logger.error("not found title")
            return False

        pattern: str = r"Week\s+(\d+)"
        match: re.Match = re.search(pattern, title)

        if not match:
            logger.error("not matched title")
            return False

        try:
            self.week_num = int(match.group(1))
            logger.info(f"current week :: {self.week_num}")
            return True
        except Exception:
            logger.error("week number error")
            return False

        # last_week: int = await db.get_task(task=self.task, description=True)
        # logger.info(f"last week :: {last_week}")
        #
        # if isinstance(self.week_num, int) and self.week_num > last_week:
        #     return True
        #
        # return False

    @utils.async_exception
    async def download_report(self, report_name: str) -> bool:
        for _ in range(2):
            download_button: ElementHandle = await self.wait_for_selector(
                selector="//kat-button[@id='downloadModalGenerateDownloadButton']"
            )
            if not download_button:
                logger.error("not found download button")
                return False

            await download_button.click()
            # await self.click(element=download_button)
            await asyncio.sleep(10)

        page: t.Optional[Page] = None
        for tab in self.context.pages:
            if "download-manager" in tab.url:
                page: Page = tab
                break

        await asyncio.sleep(10)

        if not page:
            logger.error("not found page")
            return False

        for _ in range(180):
            try:
                await page.wait_for_selector(selector="//kat-badge[@label='In Progress']", timeout=10000)
                logger.warning("report is not ready")
            except TimeoutError:
                logger.info("report was generated")
                break

            await asyncio.sleep(50)

        try:
            async with page.expect_download() as download_info:
                download_button: ElementHandle = await page.wait_for_selector(
                    selector="//kat-icon[@name='file_download']"
                )
                if not download_button:
                    logger.error("not found download button")
                    return False

                await download_button.click()
                # await self.click(element=download_button)

            download: Download = await download_info.value
            # report_path: str = os.path.join(config.reports_path, category, download.suggested_filename)
            report_path: str = os.path.join(config.reports_path, self.service_name, f"{report_name}.csv")
            await download.save_as(path=report_path)
            await asyncio.sleep(10)
        except Exception as e:
            logger.error(e)
        finally:
            await page.close()

        return True

    @utils.async_exception
    async def get_brand_report(self, brand: str) -> bool:
        await self.run_js("set_brand.js", brand)
        await asyncio.sleep(5)

        apply_button: ElementHandle = await self.wait_for_selector(
            selector="//kat-button[@data-test-id='RequiredFilterApplyButton']"
        )
        if not apply_button:
            logger.error("not found apply button")
            return False

        await apply_button.click()
        # await self.click(element=apply_button)
        await asyncio.sleep(5)

        download_button: ElementHandle = await self.wait_for_selector(
            selector="//kat-button[@id='GenerateDownloadButton']"
        )
        if not download_button:
            logger.error("not found download button")
            return False

        await download_button.click()
        # await self.click(element=download_button)
        await asyncio.sleep(5)

        await self.run_js("set_report_view.js")
        await asyncio.sleep(5)

        if not await self.download_report(report_name=brand):
            return False

        return True

    @utils.async_exception
    async def get_brand(self) -> bool:
        is_new_week: bool = await self.is_new_week()

        if is_new_week is None:
            raise BrowserExceptions.ElementNotFoundError(element="is_new_week")
        elif not is_new_week:
            logger.info("the new week has not come")
            return False
        elif is_new_week:
            await asyncio.sleep(5)
            options: list = await self.run_js("get_all_brands.js")

            if not options:
                logger.error("not found options")
                return False

            # current_brand: str = await self.run_js("get_current_brand.js")

            for brand in options:
                logger.info(f"processing :: {brand}")
                await asyncio.sleep(5)

                if not await self.get_brand_report(brand=brand):
                    return False

                await asyncio.sleep(15)

                report_path: str = os.path.join(config.reports_path, self.service_name, f"{brand}.csv")
                if not os.path.isfile(report_path):
                    logger.critical(f"report not found :: {report_path}")
                    return False

                # if brand != big_query.get_report_name(file_path=report_path, category="brand"):
                #     logger.error("incorrect file")
                #     continue

                if not big_query.add_report(
                    file_path=report_path,
                    dataset="amzudc",
                    # table=brand.lower().replace(" ", "_"),
                    table="share_test",
                    skip_rows=1,
                    # table="share_test",
                    # table=f"{self.service_name}_{brand.lower().replace(' ', '_')}",
                ):
                    return False

                logger.info(f"report completed :: {brand}")
                await asyncio.sleep(30)

                if not await self.is_logged(reload=True):
                    return False

        return True

    @utils.async_exception
    async def get_asin_report(self, asin: str) -> bool:
        asin_button: ElementHandle = await self.wait_for_selector(
            selector="//kat-tab[@tab-id='query-performance-asin-view']"
        )
        if not asin_button:
            logger.error("not found asin button")
            return False

        await asin_button.click()
        # await self.click(element=asin_button)
        await asyncio.sleep(5)

        asin_input: ElementHandle = await self.wait_for_selector(
            selector="//kat-input[@placeholder='Search for 1 ASIN']"
        )
        if not asin_input:
            logger.error("not found asin input")
            return False

        input_element: ElementHandle = await asin_input.evaluate_handle(
            "el => el.shadowRoot.querySelector('input[part=\"input\"]')"
        )
        if not input_element:
            logger.error("not found input element")
            return False

        await input_element.fill(asin)
        await asyncio.sleep(5)

        await self.run_js("set_asin_range.js")
        await asyncio.sleep(5)

        # await self.run_js("set_asin_year.js")
        # await asyncio.sleep(5)
        #
        # await self.run_js("set_asin_month.js")
        # await asyncio.sleep(5)

        await self.run_js(js_file="set_week.js")
        await asyncio.sleep(5)

        try:
            buttons: Locator = self.page.locator("//kat-button[@data-test-id='RequiredFilterApplyButton']")
            apply_button = buttons.nth(-1)
            await apply_button.click()
            # await self.click(element=apply_button)
        except Exception as e:
            logger.error(e)
            return False

        await asyncio.sleep(5)

        try:
            buttons: Locator = self.page.locator("//kat-button[@id='GenerateDownloadButton']")
            download_button = buttons.nth(-1)
            await download_button.click()
            # await self.click(element=download_button)
        except Exception as e:
            logger.error(e)
            return False

        await asyncio.sleep(5)

        if not await self.download_report(report_name=asin):
            return False

        return True

    @utils.async_exception
    async def process_asin(self, sku: str, asin: str) -> bool:
        logger.info(f"processing :: {asin} :: {sku}")
        await asyncio.sleep(5)

        # await self.is_new_week()

        if not await self.get_asin_report(asin=asin):
            return False

        await asyncio.sleep(15)

        report_path: str = os.path.join(config.reports_path, self.service_name, f"{asin}.csv")
        if not os.path.isfile(report_path):
            logger.critical(f"report not found :: {report_path}")
            return False

        # if asin != big_query.get_report_name(file_path=report_path, category="asin"):
        #     logger.error("incorrect file")
        #     return False

        # if not big_query.add_report(
        #     file_path=report_path,
        #     dataset=self.service_name,
        #     table=self.category,
        #     skip_rows=1,
        #     asin=sku
        # ):
        #     return False
        if not postgres_db.add_report(
            file_path=report_path,
            dataset=self.service_name,
            table=self.category,
            skip_rows=1,
            asin=sku
        ):
            return False


        logger.info(f"report completed :: {asin} :: {sku}")
        await asyncio.sleep(random.randint(30, 60))

        return True

    @utils.async_exception
    async def get_asin(self) -> bool:
        df: pd.DataFrame = gs.worksheet_to_dataframe(category=self.category)
        if df.empty:
            logger.error("not found asin report")
            return False

        df_loc: pd.DataFrame = df.loc[:99, ["sku", "ASIN"]] if self.user_id == "1" else df.loc[98:, ["sku", "ASIN"]]

        for value in df_loc.itertuples(index=False):
            for _ in range(3):
                if not await self.process_asin(sku=value.sku, asin=value.ASIN):
                    continue

                if not await self.is_logged(reload=True):
                    await asyncio.sleep(300)

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

                break

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

                if self.category == "brand":
                    if not await self.get_brand():
                        self.task["status"] = "failed"
                elif self.category == "asin":
                    if not await self.get_asin():
                        self.task["status"] = "failed"
            else:
                self.task["status"] = "failed"
                logger.warning("login failed")
        finally:
            if self.task["status"] == "started":
                self.task["status"] = "stopped"

            self.task["description"] = f"week: {self.week_num}" if self.week_num else 1
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

    async def lost_reports(self):
        import datetime
        import copy
        import json

        df: pd.DataFrame = gs.worksheet_to_dataframe(category=self.category)

        if df.empty:
            logger.error("not found asin report")
            return False

        df.columns = df.iloc[1]
        df = df.iloc[2:].reset_index(drop=True)

        input_models = dict()
        for row in df[["Status Amazon", "SKU", "Asin"]].itertuples(index=False):
            status = row[0]
            sku = row[1]
            asin = row[2]
            if status == "Active":
                input_models[sku] = asin

        lost_periods: list = [
            datetime.datetime(2025, 9, 27, 0, 0),
            datetime.datetime(2025, 9, 20, 0, 0),
            datetime.datetime(2025, 9, 13, 0, 0),
            datetime.datetime(2025, 9, 6, 0, 0)
        ]

        db_models: list = postgres_db.get_all_from_table(schema_name=self.service_name, table_name="asin")
        lost_models = dict()

        for input_model in input_models.keys():
            model_periods: list = copy.deepcopy(lost_periods)
            for db_model in db_models:
                if input_model == db_model["sku"]:
                    if db_model["reporting_date"] not in model_periods:
                        continue

                    model_periods.remove(db_model["reporting_date"])

            if model_periods:
                lost_models[input_model] = {
                    "asin": input_models[input_model],
                    "periods": [m.isoformat() for m in model_periods]
                }

        print(json.dumps(lost_models))

        # ba = BrandAnalytics(user_id="1", category="asin")
        # asyncio.run(ba.lost_reports())
