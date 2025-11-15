import os
import re
import json
import time
import gzip
import asyncio
import requests
import pandas as pd
from uuid import uuid4
from pathlib import Path
from datetime import datetime, timedelta

from ad_api.base import ApiResponse
from ad_api.base.marketplaces import Marketplaces
from ad_api.api.reports import Reports

try:
    from loggers.logger import logger
    from utils.decorators import utils
    from settings.config import config
    from database.database import db
    from database.postgres_db import postgres_db
except ImportError as ie:
    exit(f"{ie} :: {Path(__file__).resolve()}")


class AmazonAD:
    service_name: str = "api_ad"

    def __init__(self, **kwargs):
        # self.period = int(period)
        self.client: Reports = self._init()

    @staticmethod
    def camel_to_snake(name: str) -> str:
        name = re.sub(r'([a-zA-Z])(\d)', r'\1_\2', name)
        name = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)
        return name.lower()

    @staticmethod
    def _init() -> Reports:
        credentials: dict = {
            "refresh_token": config.AD_REFRESH_TOKEN,
            "client_id": config.AD_CLIENT_ID,
            "client_secret": config.AD_CLIENT_SECRET,
            "profile_id": config.AD_PROFILE_ID
        }
        return Reports(credentials=credentials, marketplace=Marketplaces.US)

    @utils.exception
    def create_report(self, data: dict) -> str:
        response: ApiResponse = self.client.post_report(body=data)
        return response.payload.get("reportId")

    @utils.exception
    def report_status(self, report_id: str) -> str:
        while True:
            payload: dict = self.client.get_report(report_id).payload
            status: str = payload.get("status")
            logger.info(f"status :: {status} :: {report_id}")

            if status == "COMPLETED":
                return payload.get("url")
            elif status in ["FATAL", "CANCELLED"]:
                break

            time.sleep(120)

    @utils.exception
    def download_report(self, report_name: str, period: str, url: str) -> bool:
        response: requests.Response = requests.get(url)
        if not response.ok:
            logger.error(f"status :: {response.status_code} :: {report_name}")
            return False

        data = json.loads(gzip.decompress(response.content).decode("utf-8"))

        if not data:
            return False

        try:
            data = [
                {self.camel_to_snake(k): v for k, v in d.items()}
                for d in data
            ]

            df: pd.DataFrame = pd.DataFrame(data)

            report_dir = os.path.join(config.reports_path, self.service_name)
            os.makedirs(report_dir, exist_ok=True)

            report_path = os.path.join(report_dir, f"{report_name}_{period}.csv")
            df.to_csv(report_path, index=False, encoding="utf-8")
        except Exception as e:
            logger.error(e)
            return False

        postgres_db.update_data(
            df=df,
            dataset=self.service_name,
            table=report_name
        )

        return True

    # @utils.exception
    # def get_report(self) -> bool:
    #     # period_days: list = [2, 5, 30]
    #     # period_days: list = [1]
    #     # period_days: list = [datetime(2025, 9, day).date() for day in range(27, 31)]
    #     # period_days: list = [datetime(2025, 10, 12).date()]
    #     period_days: list = [datetime.now().date() - timedelta(days=1)]
    #     for days in period_days:
    #         logger.info(f"getting report for {days} days")
    #         # end_date: datetime = datetime.now().date()
    #         # start_date: datetime = end_date - timedelta(days=days)
    #         ###
    #         # end_date: datetime = datetime.now().date() - timedelta(days=days)
    #         # start_date: datetime = end_date
    #         ###
    #         end_date = start_date = days
    #         period: str = f"{start_date.isoformat()}_{end_date.isoformat()}"
    #
    #         for category, report_config in config.API_AD.items():
    #             report_ids = dict()
    #             for data in report_config:
    #                 data["startDate"] = start_date.isoformat()
    #                 data["endDate"] = end_date.isoformat()
    #
    #                 name: str = data["name"]
    #                 logger.info(f"processing :: {category} :: {name} :: {period}")
    #
    #                 while True:
    #                     try:
    #                         report_id: str = self.create_report(data=data)
    #                         if report_id:
    #                             break
    #                     except Exception as e:
    #                         logger.error(e)
    #
    #                     time.sleep(180)
    #
    #                 if not report_id:
    #                     logger.error("failed to create report")
    #                     return False
    #
    #                 logger.info(f"report was successfully created :: {category} :: {name} :: {period} :: {report_id}")
    #
    #                 report_ids[name] = report_id
    #                 time.sleep(180)
    #
    #             for report_name, report_id in report_ids.items():
    #                 url: str = self.report_status(report_id=report_id)
    #
    #                 if not url:
    #                     logger.error("url not found")
    #                     continue
    #
    #                 logger.info(f"document was successfully created :: {report_name} :: {period}")
    #
    #                 if not self.download_report(report_name=report_name, period=period, url=url):
    #                     logger.error("report download failed")
    #                     continue
    #
    #     return True

    # @utils.exception
    # def get_report(self) -> bool:
    #     period_days: list = [2, 5, 30]
    #     for days in period_days:
    #         logger.info(f"getting report for {days} days")
    #         end_date: datetime = datetime.now().date() - timedelta(days=1)
    #         start_date: datetime = end_date - timedelta(days=days)
    #         period: str = f"{start_date.isoformat()}_{end_date.isoformat()}"
    #
    #         for category, report_config in config.API_AD.items():
    #             report_ids = dict()
    #             for data in report_config:
    #                 # if "_campaign" not in data["name"]:
    #                 #     continue
    #
    #                 data["startDate"] = start_date.isoformat()
    #                 data["endDate"] = end_date.isoformat()
    #
    #                 name: str = data["name"]
    #                 logger.info(f"processing :: {category} :: {name} :: {period}")
    #
    #                 while True:
    #                     try:
    #                         report_id: str = self.create_report(data=data)
    #                         if report_id:
    #                             break
    #                     except Exception as e:
    #                         logger.error(e)
    #
    #                     time.sleep(180)
    #
    #                 if not report_id:
    #                     logger.error("failed to create report")
    #                     return False
    #
    #                 logger.info(f"report was successfully created :: {category} :: {name} :: {period} :: {report_id}")
    #
    #                 report_ids[name] = report_id
    #                 time.sleep(180)
    #
    #             for report_name, report_id in report_ids.items():
    #                 url: str = self.report_status(report_id=report_id)
    #
    #                 if not url:
    #                     logger.error("url not found")
    #                     continue
    #
    #                 logger.info(f"document was successfully created :: {report_name} :: {period}")
    #
    #                 if not self.download_report(report_name=report_name, period=period, url=url):
    #                     logger.error("report download failed")
    #                     continue
    #
    #     return True

    @utils.exception
    def get_report(self) -> bool:
        period_days: int = 7
        for days in range(1, period_days + 1):
            logger.info(f"getting report for {days} days")

            end_date: datetime = datetime.now().date() - timedelta(days=days)
            start_date: datetime = end_date
            period: str = f"{start_date.isoformat()}_{end_date.isoformat()}"

            report_ids = dict()

            for category, report_config in config.API_AD.items():
                for data in report_config:
                    if "_campaign" not in data["name"]:
                        continue

                    data["startDate"] = start_date.isoformat()
                    data["endDate"] = end_date.isoformat()

                    name: str = data["name"]
                    logger.info(f"processing :: {category} :: {name} :: {period}")

                    while True:
                        try:
                            report_id: str = self.create_report(data=data)
                            if report_id:
                                break
                        except Exception as e:
                            logger.error(e)

                        time.sleep(180)

                    if not report_id:
                        logger.error("failed to create report")
                        return False

                    logger.info(f"report was successfully created :: {category} :: {name} :: {period} :: {report_id}")

                    report_ids[name] = report_id
                    time.sleep(180)

            for report_name, report_id in report_ids.items():
                url: str = self.report_status(report_id=report_id)

                if not url:
                    logger.error("url not found")
                    continue

                logger.info(f"document was successfully created :: {report_name} :: {period}")

                if not self.download_report(report_name=report_name, period=period, url=url):
                    logger.error("report download failed")
                    continue

        return True

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
            if not await asyncio.to_thread(self.get_report):
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
