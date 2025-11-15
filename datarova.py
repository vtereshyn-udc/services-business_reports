import time
import json
import asyncio
import typing as t
from uuid import uuid4
from io import StringIO
from pathlib import Path
from datetime import datetime, timedelta, timezone

import requests
import pandas as pd

try:
    from loggers.logger import logger
    from utils.decorators import utils
    from settings.config import config
    from database.database import db
    from database.big_query import big_query
    from database.postgres_db import postgres_db
except ImportError as ie:
    exit(f"{ie} :: {Path(__file__).resolve()}")


class Datarova:
    service_name: str = "datarova"

    def __init__(self, **kwargs):
        self.date: t.Optional[str] = None
        self.access_token: t.Optional[str] = None
        self.x_plan_token: t.Optional[str] = None

    @utils.exception
    def login(self) -> None:
        data: str = f'{{"AuthFlow":"USER_PASSWORD_AUTH","ClientId":"5dcti49pggi19uqs3df8iae3o1","AuthParameters":{{"USERNAME":"{config.DATAROVA_USER}","PASSWORD":"{config.DATAROVA_PASS}"}}, "ClientMetadata":{{}}}}'

        response: requests.Response = requests.post(
            url="https://cognito-idp.us-west-2.amazonaws.com/",
            headers=config.HEADERS["login"],
            data=data
        )

        if not response.ok:
            logger.error(response.text)
            raise

        try:
            data: dict = response.json()
            self.access_token: str = data.get("AuthenticationResult", {}).get("AccessToken")
            logger.info("login was successfully")
        except json.decoder.JSONDecodeError:
            logger.error(response.text)
            raise ValueError()

    @utils.exception
    def get_customer(self) -> None:
        headers: dict = config.HEADERS["customer"]
        headers["Authorization"] = f"Bearer {self.access_token}"

        response: requests.Response = requests.get(
            url="https://api.datarova.com/get-customer",
            headers=headers
        )

        if not response.ok:
            logger.error(response.status_code, response.text)
            raise

        self.x_plan_token = response.headers.get("X-Plan")
        logger.info("customer was received successfully")

    @utils.exception
    def download_report(self) -> list:
        headers: dict = config.HEADERS["downloads"]
        headers["Authorization"] = f"Bearer {self.access_token}"
        headers["x-plan"] = self.x_plan_token

        response: requests.Response = requests.get(
            url="https://api.datarova.com/downloads",
            headers=headers
        )

        if not response.ok:
            logger.error(response.status_code, response.text)
            raise

        try:
            data: dict = response.json()
            return data.get("results")
        except json.decoder.JSONDecodeError:
            logger.error(response.text)
            raise ValueError()

    @utils.exception
    def process_asin(self, asin: str, project_id: str) -> None:
        logger.info(f"processing asin :: {asin}")

        headers: dict = config.HEADERS["add"]
        headers["Authorization"] = f"Bearer {self.access_token}"
        headers["x-plan"] = self.x_plan_token

        data = {
            "reportType": "16",
            "keywordASIN": asin,
            "from": self.date,
            "to": self.date,
            "exactAsinOnly": "true",
            "projectId": project_id,
            "marketplace": "US",
        }

        response: requests.Response = requests.post(
            url='https://api.datarova.com/download/add',
            headers=headers,
            data=data
        )

        if not response.ok:
            logger.error(response.status_code, response.text)
            raise

    @utils.exception
    def get_report(self) -> bool:
        self.date: str = (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")

        self.login()
        time.sleep(5)

        if not self.access_token:
            logger.error("access token is missing")
            raise

        self.get_customer()
        time.sleep(5)

        if not self.x_plan_token:
            logger.error("x_plan token is missing")
            raise

        asin_list = list()
        for asin in config.ASIN:
            self.process_asin(asin=asin["asin"], project_id=asin["project_id"])
            asin_list.append(asin["asin"])
            time.sleep(5)

        time.sleep(5)
        results: dict = self.download_report()
        # {'id': 163452, 'userId': '26cedeea-b803-47a3-8a5a-dbc70c8a1cc9', 'reportType': 13, 'rangeType': None, 'keywordASIN': 'B0DQLG1YPB', 'from': '2025-05-30', 'to': '2025-05-30', 'fileName': 'ASIN-Tab-B0DQLG1YPB-250531-084053.csv', 'status': 2, 'readyOn': '1748680855880', 'link': 'https://ecomanalytics-download-report.s3.us-east-2.amazonaws.com/files/ASIN-Tab-B0DQLG1YPB-250531-084053.csv', 'marketplace': 'US', 'createdAt': '2025-05-31T08:40:53.485Z', 'updatedAt': '2025-05-31T08:40:55.880Z', 'reportTypeText': 'ASIN Tab'}
        for result in results:
            if result.get("reportType") == 16 and result.get("from") == self.date and result.get("to") == self.date and result.get("keywordASIN") in asin_list:
                if datetime.fromisoformat(result.get("createdAt").rstrip("Z")).date() == datetime.now(timezone.utc).date():
                    url: str = result.get("link")

                    try:
                        response: requests.Response = requests.get(url=url)

                        if not response.ok:
                            logger.error(response.status_code, response.text)
                            raise
                    except Exception as e:
                        logger.error(e)
                        continue

                    csv_data: StringIO = StringIO(response.text)
                    df: pd.DataFrame = pd.read_csv(csv_data)
                    df.columns = [col.replace(" ", "_").lower() for col in df.columns]
                    df['date'] = pd.to_datetime(df['date']).dt.date
                    df['amazon_choice_badge'] = df['amazon_choice_badge'].map({'Yes': True, 'No': False}).astype('boolean')

                    # if not big_query.update_data(
                    #     df=df,
                    #     dataset="amzudc",
                    #     table="rank_tracker",
                    #     deduplicate=False
                    # ):
                    #     return False
                    if not postgres_db.update_data(
                        df=df,
                        dataset="amzudc",
                        table="rank_tracker",
                        write_disposition="WRITE_APPEND",
                        deduplicate=True
                    ):
                        return False

                    asin_list.remove(result.get("keywordASIN"))

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


print(config.DATAROVA_PASS)