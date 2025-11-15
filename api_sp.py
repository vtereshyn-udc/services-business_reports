import os
import re
import io
import ast
import sys
import time
import json
import asyncio
import pandas as pd
from uuid import uuid4
from pathlib import Path
from datetime import datetime, timedelta

import xml.etree.ElementTree as ET
from sp_api.api import Reports, Orders
from sp_api.base import ApiResponse, ReportType
from sp_api.base.marketplaces import Marketplaces
from sp_api.base.exceptions import SellingApiRequestThrottledException

sys.path.append(str(Path(__file__).resolve().parent.parent))

try:
    from loggers.logger import logger
    from utils.decorators import utils
    from settings.config import config
    from database.database import db
    from database.big_query import big_query
    from database.postgres_db import postgres_db
except ImportError as ie:
    exit(f"{ie} :: {Path(__file__).resolve()}")


class AmazonSP:
    service_name: str = "api_sp"

    def __init__(self, category: str, **kwargs):
        self.category: str = category
        self.client: Reports | Orders = self._init(
            client_type="orders" if category and "order" in category.lower() else "reports"
        )
        self.report_path = None
        self.current_date = None
        self.report_config = None

    @staticmethod
    def _init(client_type: str) -> Reports | Orders:
        credentials: dict = {
            "refresh_token": config.SP_REFRESH_TOKEN,
            "lwa_app_id": config.SP_LWA_APP_ID,
            "lwa_client_secret": config.SP_LWA_CLIENT_SECRET
        }
        if client_type == "orders":
            return Orders(credentials=credentials, marketplace=Marketplaces.US)
        else:
            return Reports(credentials=credentials, marketplace=Marketplaces.US)

    @utils.exception
    def get_reports(self) -> str:
        reports: ApiResponse = self.client.get_reports(reportTypes=self.category, marketplaceIds=[Marketplaces.US.marketplace_id])
        reports_list: dict = reports.payload.get("reports", [])
        for report in reports_list:
            if report["processingStatus"] == "DONE":
                return report.get("reportDocumentId")

    @utils.exception
    def get_orders(self, report_path: str) -> list:
        # args: dict = report_config.get("args")
        now: datetime = datetime.now().date()

        reports: ApiResponse = self.client.get_orders(
            LastUpdatedAfter=(now - timedelta(days=30)).isoformat() + "T00:00:00Z",
            MarketplaceIds=[Marketplaces.US.marketplace_id]
        )

        data: dict = reports.payload.get("Orders", [])
        df: pd.DataFrame = pd.DataFrame(data)

        df.to_csv(report_path, index=False, encoding="utf-8")
        return True

    # @utils.exception
    def create_report(self, **kwargs) -> str:
        response: ApiResponse = self.client.create_report(reportType=self.category, **kwargs)
        # {'errors': None,
        #  'headers': {'Server': 'Server', 'Date': 'Mon, 01 Sep 2025 09:19:09 GMT', 'Content-Type': 'application/json', 'Content-Length': '28', 'Connection': 'keep-alive', 'x-amz-rid': 'PZ8SZQ3KA660HBYA3XBP', 'x-amzn-RateLimit-Limit': '0.0167', 'x-amzn-RequestId': '1fcfe756-f056-4524-85fd-f15669be61c3', 'x-amz-apigw-id': 'OPF1fcfe756f056', 'X-Amzn-Trace-Id': 'Root=1-68b5650d-1fcfe756f0564524', 'Vary': 'Content-Type,Accept-Encoding,User-Agent', 'Strict-Transport-Security': 'max-age=47474747; includeSubDomains; preload'},
        #  'next_token': None,
        #  'pagination': None,
        #  'payload': {'reportId': '1213280020332'},
        #  'rate_limit': '0.0167'}
        return response.payload.get("reportId")

    @utils.exception
    def report_status(self, report_id: str) -> str:
        while True:
            status: str = self.client.get_report(report_id).payload.get("processingStatus")
            logger.info(f"status :: {status} :: {report_id}")

            if status == "DONE":
                return self.client.get_report(report_id).payload.get("reportDocumentId")
            elif status in ["FATAL", "CANCELLED"]:
                break

            time.sleep(30)

    def _xml_element_to_dict(self, element: ET.Element, parent_key: str = '') -> dict:
        result = {}

        for attr_key, attr_value in element.attrib.items():
            key = f"{parent_key}_{attr_key}" if parent_key else attr_key
            result[key] = attr_value

        if element.text and element.text.strip() and len(element) == 0:
            return {parent_key or element.tag: element.text.strip()}

        for child in element:
            child_key = child.tag
            child_dict = self._xml_element_to_dict(child, child_key)
            result.update(child_dict)

        return result

    def _parse_xml_document(self, document: str) -> pd.DataFrame:
        xml_content = document.replace('""', '"').strip()

        root_tag_match = re.search(r'<(\w+)[>\s]', xml_content)
        if root_tag_match:
            root_tag = root_tag_match.group(1)
            closing_tag = f'</{root_tag}>'
            closing_pos = xml_content.rfind(closing_tag)
            if closing_pos != -1:
                xml_content = xml_content[:closing_pos + len(closing_tag)]

        try:
            root = ET.fromstring(xml_content)

            data = []

            for possible_tag in ['Node', 'Record', 'Row', 'Item']:
                nodes = root.findall(f'.//{possible_tag}')
                if nodes:
                    for node in nodes:
                        row = self._xml_element_to_dict(node)
                        data.append(row)
                    break

            if not data:
                for record in root:
                    row = self._xml_element_to_dict(record)
                    data.append(row)

            if not data:
                data = [self._xml_element_to_dict(root)]

            return pd.DataFrame(data)

        except ET.ParseError as e:
            logger.error(f"XML parsing error: {e}")
            raise

    @staticmethod
    def _parse_column_value(x):
        if pd.isna(x):
            return None

        if isinstance(x, dict):
            return x

        if isinstance(x, str):
            try:
                return json.loads(x)
            except (json.JSONDecodeError, ValueError):
                try:
                    return ast.literal_eval(x)
                except (ValueError, SyntaxError):
                    return None

        return None

    def _flatten_dict(self, data: dict) -> dict:
        result = {}

        for key, value in data.items():
            if isinstance(value, dict):
                if 'amount' in value and 'currencyCode' in value:
                    result[key] = value['amount']
                else:
                    result.update(self._flatten_dict(value))
            else:
                result[key] = value

        return result

    @utils.exception
    def processing_dataframe(self, df: pd.DataFrame, columns: list) -> pd.DataFrame:
        for col in columns:
            df[col] = df[col].apply(self._parse_column_value)

        processed_rows = list()

        for _, row in df.iterrows():
            processed_row = dict()

            for col in df.columns:
                if col not in columns:
                    processed_row[col] = row[col]

            for col in columns:
                if pd.notna(row[col]) and isinstance(row[col], dict):
                    flattened = self._flatten_dict(row[col])
                    processed_row.update(flattened)

            processed_rows.append(processed_row)

        return pd.DataFrame(processed_rows)

    @utils.exception
    def download_report(self, document_id: str) -> bool:
        response: ApiResponse = self.client.get_report_document(document_id, download=True)
        document: str = response.payload.get("document")

        if not document:
            return False

        if self.category in [
            "GET_BRAND_ANALYTICS_MARKET_BASKET_REPORT",
            "GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT",
            "GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT"
        ]:
            data: dict = json.loads(document)
            records: list = data.get('dataByAsin', [])
            df: pd.DataFrame = pd.DataFrame(records)
        elif self.category == "GET_SALES_AND_TRAFFIC_REPORT":
            data: dict = json.loads(document)
            records: list = data.get("salesAndTrafficByDate", [])
            df: pd.DataFrame = pd.DataFrame(records)
        elif self.report_config.get("format") == "xml":
            df: pd.DataFrame = self._parse_xml_document(document)
        elif self.report_config.get("format") == "json":
            try:
                data = json.loads(document)
                if isinstance(data, list):
                    df: pd.DataFrame = pd.DataFrame(data)
                elif isinstance(data, dict):
                    for value in data.values():
                        if isinstance(value, list):
                            df: pd.DataFrame = pd.DataFrame(value)
                            break
                    else:
                        df: pd.DataFrame = pd.DataFrame([data])
                else:
                    df: pd.DataFrame = pd.DataFrame([data])
            except json.JSONDecodeError:
                df: pd.DataFrame = pd.read_csv(io.StringIO(document), sep="\t", encoding="utf-8")
        else:
            try:
                df: pd.DataFrame = pd.read_csv(io.StringIO(document), sep="\t", encoding="utf-8")
            except pd.errors.ParserError as e:
                logger.error(e)
                return False

        json_columns = self.report_config.get("json_columns")
        if json_columns:
            df: pd.DataFrame = self.processing_dataframe(df=df, columns=json_columns)

        if self.category == "GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT":
            df.rename(columns={"amount": "repeat_purchase_revenue"}, inplace=True)

        df.to_csv(self.report_path, index=False, encoding="utf-8")
        return True

    @utils.exception
    def get_report(self, **kwargs) -> bool:
        report_dir = os.path.join(config.reports_path, self.service_name)
        os.makedirs(report_dir, exist_ok=True)

        self.report_path = os.path.join(report_dir, f"{self.category}.csv")

        # if "order" in self.category.lower():
        #     # self.get_orders(report_config=report_config)
        #     self.get_orders(report_path=self.report_path)
        #     logger.info("order report has been created")
        # else:
        #     if create_report:

        logger.info(self.category)

        while True:
            try:
                report_id = self.create_report(**kwargs)
                if report_id:
                    break
            # except SellingApiRequestThrottledException as e:
            except Exception as e:
                logger.error(e)

            time.sleep(1800)

        if not report_id:
            logger.error("failed to create report")
            return False

        logger.info(f"report was successfully created :: {report_id}")

        document_id: str = self.report_status(report_id=report_id)
            # else:
            #     document_id: str = self.get_reports()

        if not document_id:
            logger.error("document id not found")
            return False

        logger.info(f"document was successfully created :: {document_id}")

        if not self.download_report(document_id=document_id):
            logger.error("report download failed")
            return False

        postgres_db.add_report(
            file_path=self.report_path,
            dataset=self.service_name,
            table=self.category.lower(),
            is_camel=True,
            custom_date=self.current_date
        )

        return True

    @utils.exception
    def collect_reports(self):
        now: datetime = datetime.now().date()
        self.current_date: datetime = now - timedelta(days=1)
        kwargs: dict = {
            "dataStartTime": self.current_date.isoformat() + "T00:00:00Z",
            "dataEndTime": self.current_date.isoformat() + "T23:59:59Z",
            "reportOptions": {
                "reportPeriod": "DAY"
            }
        }

        for category, report_config in config.API_SP.items():
            self.category = category
            self.report_config = report_config

            try:
                self.get_report(**kwargs)
            except Exception as e:
                logger.error(e)

            # time.sleep(60)

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
            if not await asyncio.to_thread(self.collect_reports):
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


# 10 GET_FBA_FULFILLMENT_INBOUND_NONCOMPLIANCE_DATA
# 11 GET_FBA_FULFILLMENT_LONGTERM_STORAGE_FEE_CHARGES_DATA
# 13 GET_FBA_FULFILLMENT_REMOVAL_SHIPMENT_DETAIL_DATA
# 19 GET_FBA_SNS_FORECAST_DATA - EMPTY
# 20 GET_FBA_SNS_PERFORMANCE_DATA
# 21 GET_FBA_STORAGE_FEE_CHARGES_DATA
# 24 GET_FLAT_FILE_MFN_SKU_RETURN_ATTRIBUTES_REPORT - EMPTY
# 26 GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE - EMPTY
# 28 GET_MERCHANT_CANCELLED_LISTINGS_DATA - EMPTY
# 40 GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE - Request for report type 1117 is not allowed at this time

# GET_SALES_AND_TRAFFIC_REPORT
# GET_B2B_PRODUCT_OPPORTUNITIES_RECOMMENDED_FOR_YOU
# GET_V1_SELLER_PERFORMANCE_REPORT
# GET_XML_BROWSE_TREE_DATA
# GET_B2B_PRODUCT_OPPORTUNITIES_NOT_YET_ON_AMAZON
# GET_BRAND_ANALYTICS_SEARCH_QUERY_PERFORMANCE_REPORT - ALREADY IN USE
# GET_V2_SELLER_PERFORMANCE_REPORT - HARD STRUCTURE
# GET_ORDERS - UNKNOWN
# GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT
