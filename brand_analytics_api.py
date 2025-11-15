import os
import ast
import time
import copy
import asyncio
import calendar
import pandas as pd
from uuid import uuid4
from pathlib import Path
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

try:
    from loggers.logger import logger
    from utils.decorators import utils
    from settings.config import config
    from database.database import db
    from utils.google_sheets import gs
    from services.api_sp import AmazonSP
    from database.postgres_db import postgres_db
except ImportError as ie:
    exit(f"{ie} :: {Path(__file__).resolve()}")


class BrandAnalyticsAPI(AmazonSP):
    category: str = "GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT"

    def __init__(self, *args, **kwargs):
        super().__init__(self.category)
        self.column_mapping: dict = self.column_mapping()
        self.api_columns: list = self.api_columns()

    @utils.exception
    def open_report(self) -> pd.DataFrame:
        return pd.read_csv(self.report_path)

    @utils.exception
    def processing_dataframe(self, model, start_date, sku=None) -> list:
        df: pd.DataFrame = self.open_report()

        for col in self.api_columns:
            df[col] = df[col].apply(lambda x: ast.literal_eval(x) if pd.notna(x) else None)

        processed_rows = list()

        for _, row in df.iterrows():
            processed_row = dict()
            for col in self.api_columns:
                for key, value in row[col].items():
                    if isinstance(value, dict):
                        value = value.get("amount")

                    processed_row[key] = value

            processed_row["sku"] = sku if sku else config.SKU.get(model, {}).get("sku")
            processed_rows.append(processed_row)
            processed_row["reporting_date"] = start_date
            processed_rows.append(processed_row)

        return processed_rows

    @staticmethod
    def get_period() -> list:
        start = datetime(2025, 8, 1)
        end = datetime(2025, 8, 1)

        months = list()
        current = start

        while current <= end:
            year, month = current.year, current.month
            start_date = datetime(year, month, 1)
            last_day = calendar.monthrange(year, month)[1]
            end_date = datetime(year, month, last_day)

            months.append({
                "start_date": start_date.strftime("%Y-%m-%d"),
                "end_date": end_date.strftime("%Y-%m-%d")
            })

            current += relativedelta(months=1)

        return months

    @staticmethod
    def api_columns() -> list:
        return [
            "searchQueryData",
            "impressionData",
            "clickData",
            "cartAddData",
            "purchaseData"
        ]

    @staticmethod
    def column_mapping() -> dict:
        return {
            "searchQuery": "search_query",
            "searchQueryScore": "search_query_score",
            "searchQueryVolume": "search_query_volume",
            "totalQueryImpressionCount": "impressions_total_count",
            "asinImpressionCount": "impressions_asin_count",
            "asinImpressionShare": "impressions_asin_share_percent",
            "totalClickCount": "clicks_total_count",
            "totalClickRate": "clicks_click_rate_percent",
            "asinClickCount": "clicks_asin_count",
            "asinClickShare": "clicks_asin_share_percent",
            "totalMedianClickPrice": "clicks_price_median",
            "asinMedianClickPrice": "clicks_asin_price_median",
            "totalSameDayShippingClickCount": "clicks_same_day_shipping_speed",
            "totalOneDayShippingClickCount": "clicks_1d_shipping_speed",
            "totalTwoDayShippingClickCount": "clicks_2d_shipping_speed",
            "totalCartAddCount": "cart_adds_total_count",
            "totalCartAddRate": "cart_adds_cart_add_rate_percent",
            "asinCartAddCount": "cart_adds_asin_count",
            "asinCartAddShare": "cart_adds_asin_share_percent",
            "totalMedianCartAddPrice": "cart_adds_price_median",
            "asinMedianCartAddPrice": "cart_adds_asin_price_median",
            "totalSameDayShippingCartAddCount": "cart_adds_same_day_shipping_speed",
            "totalOneDayShippingCartAddCount": "cart_adds_1d_shipping_speed",
            "totalTwoDayShippingCartAddCount": "cart_adds_2d_shipping_speed",
            "totalPurchaseCount": "purchases_total_count",
            "totalPurchaseRate": "purchases_purchase_rate_percent",
            "asinPurchaseCount": "purchases_asin_count",
            "asinPurchaseShare": "purchases_asin_share_percent",
            "totalMedianPurchasePrice": "purchases_price_median",
            "asinMedianPurchasePrice": "purchases_asin_price_median",
            "totalSameDayShippingPurchaseCount": "purchases_same_day_shipping_speed",
            "totalOneDayShippingPurchaseCount": "purchases_1d_shipping_speed",
            "totalTwoDayShippingPurchaseCount": "purchases_2d_shipping_speed"
        }

    @utils.exception
    def run_lost_models(self):
        now: datetime = datetime.now()
        if now.weekday() not in [0, 1, 2]:
            return True

        days_back: int = (now.weekday() + 2) % 7 or 7
        last_saturday: datetime = (now - timedelta(days=days_back)).replace(hour=0, minute=0, second=0, microsecond=0)
        periods: list = [last_saturday]

        df: pd.DataFrame = gs.worksheet_to_dataframe(category="asin")

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

        # lost_periods: list = [
        #     datetime(2025, 10, 11, 0, 0),
        #     # datetime(2025, 10, 4, 0, 0),
        #     # datetime.datetime(2025, 9, 27, 0, 0),
        #     # datetime.datetime(2025, 9, 20, 0, 0),
        #     # datetime.datetime(2025, 9, 13, 0, 0),
        #     # datetime.datetime(2025, 9, 6, 0, 0)
        # ]

        db_models: list = postgres_db.get_all_from_table(schema_name="brand_analytics", table_name="asin")
        lost_models = dict()

        for input_model in input_models.keys():
            model_periods: list = copy.deepcopy(periods)
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

        # print(json.dumps(lost_models, indent=2))
        # for sku, values in config.BA_LOST_REPORTS.items():
        for sku, values in lost_models.items():
            asin: str = values["asin"]
            periods: list = values["periods"]

            for period in periods:
                logger.info(f"Processing :: {sku} :: {asin} :: {period}")
                period_dt: datetime = datetime.fromisoformat(period)
                start_date: str = (period_dt - timedelta(days=6)).date().isoformat()
                end_date: str = period_dt.date().isoformat()

                # report_name: str = f"{self.category}_{asin}_{end_date}.csv"
                # self.report_path: str = os.path.join(config.reports_path, self.category, report_name)
                # os.makedirs(os.path.dirname(self.report_path), exist_ok=True)

                if not self.get_report(
                        reportOptions={
                            "reportPeriod": "WEEK",
                            "asin": asin
                        },
                        dataStartTime=start_date,
                        dataEndTime=end_date,
                ):
                    logger.critical(f"Failed to process period {period} model {sku}")
                    continue

                try:
                    df: pd.DataFrame = pd.DataFrame(self.processing_dataframe("", end_date, sku))
                except Exception:
                    continue

                df: pd.DataFrame = df.rename(columns=self.column_mapping)

                postgres_db.update_data(
                    df=df,
                    dataset="brand_analytics",
                    table="asin",
                )

                time.sleep(30)

        return True

    # @utils.exception
    # def run(self) -> None:
    #     for period in self.get_period():
    #         # if period["start_date"] <= "2024-07-01" or period["start_date"] >= "2024-10-01":
    #         #     continue
    #         for model in list(config.SKU.keys())[10:]:
    #             logger.info(f"Processing period {period} model {model}")
    #
    #             asin_restarts: dict = config.ASIN_RESTARTS.get(model, {})
    #
    #             asins_to_process = list()
    #             for key, value in asin_restarts.items():
    #                 restart_date: datetime = datetime.strptime(key, "%d.%m.%Y").date()
    #                 start_date_dt: datetime = datetime.fromisoformat(period["start_date"]).date()
    #                 end_date_dt: datetime = datetime.fromisoformat(period["end_date"]).date()
    #
    #                 if start_date_dt <= restart_date <= end_date_dt:
    #                     asins_to_process = [value["old_asin"], value["new_asin"]]
    #                     break
    #
    #                 elif start_date_dt <= restart_date:
    #                     asins_to_process = [value["old_asin"]]
    #                     break
    #
    #                 elif restart_date <= start_date_dt:
    #                     asins_to_process = [value["new_asin"]]
    #
    #             if not asins_to_process:
    #                 asins_to_process.append(config.SKU[model]["current_asin"])
    #
    #             for asin in asins_to_process:
    #                 report_name: str = f"{self.category}_{asin}_{period['start_date']}.csv"
    #                 self.report_path: str = os.path.join(config.reports_path, self.category, report_name)
    #                 os.makedirs(os.path.dirname(self.report_path), exist_ok=True)
    #
    #                 if not self.get_report(
    #                     reportOptions={
    #                         "reportPeriod": "MONTH",
    #                         "asin": asin
    #                     },
    #                     dataStartTime=period["start_date"],
    #                     dataEndTime=period["end_date"]
    #                 ):
    #                     logger.critical(f"Failed to process period {period} model {model}")
    #                     continue
    #
    #                 try:
    #                     df: pd.DataFrame = pd.DataFrame(self.processing_dataframe(model, period["start_date"]))
    #                 except Exception:
    #                     continue
    #
    #                 df: pd.DataFrame = df.rename(columns=self.column_mapping)
    #
    #                 postgres_db.update_data(
    #                     df=df,
    #                     dataset="brand_analytics",
    #                     table="asin_mounth",
    #                 )
    #
    #                 time.sleep(30)

    @utils.async_exception
    async def execute(self) -> None:
        self.task: dict = {
            "task_id": str(uuid4()),
            "user_id": "0",
            "service": self.service_name,
            "status": "started"
        }
        await db.update_task(task=self.task)

        try:
            if not await asyncio.to_thread(self.run_lost_models):
                self.task["status"] = "failed"
        finally:
            if self.task["status"] == "started":
                self.task["status"] = "stopped"
            await db.update_task(task=self.task)

    @utils.exception
    def run(self) -> None:
        msg: str = f"service {{status}} :: {self.service_name}"

        try:
            logger.info(msg.format(status="running"))
            asyncio.run(self.execute())
            logger.info(msg.format(status="finished"))
        except KeyboardInterrupt:
            logger.info(msg.format(status="stopped"))
