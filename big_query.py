import os
import re
import sys
import json
import string
import typing as t
from pathlib import Path
from datetime import datetime

import pandas as pd
from google.cloud import bigquery
from google.api_core import exceptions

sys.path.append(str(Path(__file__).resolve().parent.parent))

try:
    from loggers.logger import logger
    from utils.decorators import utils
    from settings.config import config
except ImportError as ie:
    exit(f"{ie} :: {Path(__file__).resolve()}")


class BigQuery:
    def __init__(self):
        self.set_credentials()
        self.client: bigquery.Client = bigquery.Client()

    @staticmethod
    def set_credentials() -> None:
        if not os.path.isfile(config.service_account_path):
            logger.error("service account file not found.")

        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config.service_account_path

    @staticmethod
    def read_file(file_path: str, skip_rows: int = 0) -> pd.DataFrame:
        if file_path.endswith('.csv'):
            return pd.read_csv(file_path, skiprows=skip_rows)
        elif file_path.endswith('.xlsx'):
            return pd.read_excel(file_path, skiprows=skip_rows)

    @staticmethod
    def add_column(
            df: pd.DataFrame,
            period: t.Optional[str] = None,
            custom_date: t.Optional[str] = None,
            asin: t.Optional[str] = None
    ) -> pd.DataFrame:
        if period:
            if "period" not in df.columns:
                df.insert(loc=0, column="period", value=period)
        elif custom_date:
            if "date" not in df.columns:
                df.insert(loc=0, column="date", value=custom_date)
        elif asin:
            if "sku" not in df.columns:
                df.insert(loc=0, column="sku", value=asin)
        else:
            if "date" not in df.columns:
                df.insert(loc=0, column="date", value=datetime.now().strftime("%Y-%m-%d"))

        return df

    @staticmethod
    def clean_column_name(col: str) -> str:
        cleaned_name: str = col

        for char in string.punctuation:
            if char == "%":
                cleaned_name: str = cleaned_name.replace(char, "percent")
            else:
                cleaned_name: str = cleaned_name.replace(char, "_")

        cleaned_name: str = re.sub(r"_+", "_", cleaned_name)
        cleaned_name: str = cleaned_name.strip("_")

        if cleaned_name and cleaned_name[0].isdigit():
            cleaned_name: str = f"col_{cleaned_name}"

        cleaned_name: str = cleaned_name[:128]
        return cleaned_name

    @staticmethod
    def get_report_name(file_path: str, category: str) -> str:
        with open(file=file_path, mode="r", encoding="utf-8") as file:
            first_line: str = file.readline().strip()

        if category == "brand":
            category: str = category.capitalize()
        elif category == "asin":
            category: str = category.upper()

        try:
            return first_line.split(f"{category}=[")[1].split("]")[0].strip('"')
        except Exception as e:
            logger.error(e)

    @utils.exception
    def create_dataset(self, dataset: str) -> str:
        dataset_ref: str = f"{self.client.project}.{dataset}"

        try:
            self.client.get_dataset(dataset_ref)
            logger.info(f"dataset already exists :: {dataset_ref}")
        except exceptions.NotFound:
            logger.warning(f"dataset not found :: {dataset_ref}")

            dataset: bigquery.Dataset = bigquery.Dataset(dataset_ref=dataset_ref)
            self.client.create_dataset(dataset)
            logger.info(f"dataset was created :: {dataset_ref}")
        finally:
            return dataset_ref

    @utils.exception
    def create_table(self, df: pd.DataFrame, dataset: str, table: str) -> str:
        dataset_ref: str = self.create_dataset(dataset=dataset)
        table_ref: str = f"{dataset_ref}.{table}"

        try:
            self.client.get_table(table_ref)
            logger.info(f"table already exists :: {table_ref}")
        except exceptions.NotFound:
            logger.warning(f"table not found :: {table_ref}")

            schema = list()
            for column_name, dtype in df.dtypes.items():
                if dtype == 'int64':
                    field_type = 'INTEGER'
                elif dtype == 'float64':
                    field_type = 'FLOAT'
                elif dtype == 'boolean':
                    field_type = 'BOOLEAN'
                elif dtype == 'object':
                    field_type = 'STRING'
                else:
                    field_type = 'STRING'

                schema.append(bigquery.SchemaField(name=column_name, field_type=field_type))

            table: bigquery.Table = bigquery.Table(table_ref=table_ref, schema=schema)
            self.client.create_table(table)
            logger.info(f"table was created :: {table_ref}")
        finally:
            return table_ref

    @utils.exception
    def deduplicate_data(self, table_ref: str) -> None:
        table: bigquery.Table = self.client.get_table(table_ref)
        has_date_column: bool = any(field.name in ["date", "period"] for field in table.schema)

        if has_date_column:
            columns_to_deduplicate: list = [
                f"CAST(`{field.name}` AS STRING)" if field.field_type == "FLOAT64" else f"`{field.name}`"
                for field in table.schema if field.name not in ["date", "period"]
            ]
            group_by_clause: str = ", ".join(columns_to_deduplicate)
            select_columns: list = [
                f"MIN(`date`) AS `date`" if field.name == "date"
                else "ANY_VALUE(`period`) AS `period`" if field.name == "period"
                else f"`{field.name}`"
                for field in table.schema
            ]
            select_clause: str = ", ".join(select_columns)
            deduplicate_query: str = f"""
                        CREATE OR REPLACE TABLE `{table_ref}` AS
                        SELECT {select_clause}
                        FROM `{table_ref}`
                        GROUP BY {group_by_clause};
                    """
        else:
            deduplicate_query: str = f"""
                    CREATE OR REPLACE TABLE `{table_ref}` AS
                    SELECT DISTINCT *
                    FROM `{table_ref}`;
                """

        query_job: bigquery.QueryJob = self.client.query(deduplicate_query)
        query_job.result()

        logger.info(f"table was deduplicated :: {table_ref}")

    @utils.exception
    def update_data(
            self,
            df: pd.DataFrame,
            dataset: str,
            table: str,
            write_disposition: str,
            deduplicate: bool = True
    ) -> bool:
        table_ref: str = self.create_table(df=df, dataset=dataset, table=table)

        job_config: bigquery.LoadJobConfig = bigquery.LoadJobConfig(
            write_disposition=write_disposition,
            # source_format=bigquery.SourceFormat.CSV,
            # autodetect=True
        )

        job: bigquery.LoadJob = self.client.load_table_from_dataframe(
            dataframe=df,
            destination=table_ref,
            job_config=job_config
        )
        job.result()
        logger.info(f"table was updated :: {table_ref}")

        if deduplicate:
            self.deduplicate_data(table_ref=table_ref)

        return True

    @utils.exception
    def validate_schema(self, df: pd.DataFrame, table: str) -> pd.DataFrame:
        if "Sessions _ Browser" in df.columns:
            df["Sessions _ Browser"] = df["Sessions _ Browser"].astype(str)

        if table == "sales_traffic_daily":
            df["Sessions _ Browser"] = pd.to_numeric(df["Sessions _ Browser"]).astype('Int64')

        if table == "share_test":
            df.columns = [col.replace(" ", "_").replace("__", "_").lower() for col in df.columns]
            df["reporting_date"] = pd.to_datetime(df["reporting_date"])

        for col in [
            "is_business_order",
            "signature_confirmation_recommended",
            "IsPremiumOrder",
            "IsISPU",
            "IsPrime",
            "IsSoldByAB",
            "IsBusinessOrder",
            "HasRegulatedItems",
            "IsAccessPointOrder",
            "IsGlobalExpressEnabled",
            "IsReplacementOrder"
        ]:
            if col in df.columns:
                df[col] = df[col].astype("boolean")

        return df

    @utils.exception
    def add_report(
            self,
            file_path: str,
            dataset: str,
            table: str,
            skip_rows: int = 0,
            add_date: bool = False,
            use_api: bool = False,
            custom_date: t.Optional[str] = None,
            period: t.Optional[str] = None,
            asin: t.Optional[str] = None,
            write_disposition: str = "WRITE_APPEND"
    ) -> bool:
        df: pd.DataFrame = self.read_file(file_path=file_path, skip_rows=skip_rows)

        if len(df) == 0:
            logger.warning("dataframe is empty")
            return False

        df.columns = [self.clean_column_name(col) for col in df.columns]
        df: pd.DataFrame = self.validate_schema(df=df, table=table)

        if add_date:
            df: pd.DataFrame = self.add_column(df=df)

        if custom_date:
            df: pd.DataFrame = self.add_column(df=df, custom_date=custom_date)

        if period:
            df: pd.DataFrame = self.add_column(df=df, period=period)

        if asin:
            df: pd.DataFrame = self.add_column(df=df, asin=asin)

        if not self.update_data(df=df, dataset=dataset, table=table, write_disposition=write_disposition):
            return False

        logger.info(f"report was added :: {dataset}.{table}")
        return True

    @utils.exception
    def get_columns(self, dataset: str, table: str) -> list:
        table_ref: str = f"{self.client.project}.{dataset}.{table}"
        table_obj: bigquery.Table = self.client.get_table(table_ref)
        return [field.name for field in table_obj.schema]

    @utils.exception
    def get_data(self, dataset: str, table: str, count: bool = False, file: t.Optional[str] = None) -> list:
        table_ref: str = f"{self.client.project}.{dataset}.{table}"
        select: str = "count(*)" if count else "*"
        query: str = f"SELECT {select} FROM `{table_ref}`"
        query_job: bigquery.QueryJob = self.client.query(query)

        result: list = [dict(row) for row in query_job.result()]

        if not file:
            return result

        file_path: str = os.path.join(config.reports_path, file)
        with open(file=f"{file_path}.json", mode="w", encoding="utf-8") as f:
            f.write(json.dumps(result, indent=2, default=str))

    @utils.exception
    def copy_table(self, source_dataset: str, source_table: str, target_dataset: str, target_table: str) -> None:
        source_table_id: str = f"{self.client.project}.{source_dataset}.{source_table}"
        destination_table_id: str = f"{self.client.project}.{target_dataset}.{target_table}"

        job: bigquery.CopyJob = self.client.copy_table(
            sources=source_table_id,
            destination=destination_table_id,
        )

        job.result()
        logger.info(f"table was copied :: {destination_table_id}")

    @utils.exception
    def delete_table(self, dataset: str, table: str) -> None:
        table_ref: str = f"{self.client.project}.{dataset}.{table}"

        try:
            self.client.delete_table(table=table_ref, not_found_ok=True)
            logger.info(f"table deleted :: {table_ref}")
        except Exception as e:
            logger.error(e)


big_query: BigQuery = BigQuery()
# big_query.add_report(
#     file_path="C:\\Users\\User1\\Documents\\amazon\\reports\\awd\\inventory_04_06_2025.csv",
#     # file_path="/home/user/projects/PycharmProjects/amazon_reports/reports/inventory_04_06_2025.csv",
#     dataset="logist_inventory",
#     table=f"inventory_{datetime.now().strftime('%d_%m_%Y')}",
#     # period="06/01/2025-06/01/2025"
#     # custom_date="06/02/2025"
#     # add_date=True
#     skip_rows=3,
#     # asin="abc_test",
#     # add_date=True
# )

# x = big_query.get_data(dataset="amzudc", table="rank_tracker", count=True)
# print(x)
# [{'f0_': 931026}]
# for i in big_query.get_data(dataset="business_reports", table="competitors"):
#     print(i)
# big_query.deduplicate_data(table_ref="dashboard-udc-parts.test.test_table")

# big_query.copy_table(source_dataset="amzudc", source_table="share_test", target_dataset="test", target_table="share_test")

# big_query.delete_table(dataset="business_reports", table="competitors")
# big_query.delete_table(dataset="business_reports", table="sales_traffic_daily")

# report_path: str = os.path.join(config.reports_path, "asin", "asin.csv")
# big_query.get_report_name(file_path=report_path, category="ASIN")