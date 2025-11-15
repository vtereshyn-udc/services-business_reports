import re
import sys
import string
import typing as t
import numpy as np
from pathlib import Path
from datetime import datetime

import pandas as pd
from sqlalchemy.sql import text
from sqlalchemy.exc import ProgrammingError
from sqlalchemy import create_engine, inspect, MetaData, Table, Column,Integer, Float, Boolean, DateTime, String, BigInteger

sys.path.append(str(Path(__file__).resolve().parent.parent))

try:
    from loggers.logger import logger
    from utils.decorators import utils
    from settings.config import config
except ImportError as ie:
    exit(f"{ie} :: {Path(__file__).resolve()}")


class PostgresDB:
    def __init__(self):
        self.engine = create_engine(config.POSTGRES_URI)
        self.metadata = MetaData()

    def __del__(self):
        if hasattr(self, "engine"):
            self.engine.dispose()

    def _create_schema(self, schema):
        with self.engine.connect() as conn:
            conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {schema}"))
            conn.commit()

    def _create_table(self, df, schema, table):
        if f"{schema}.{table}" in self.metadata.tables:
            return

        columns = [
            Column(col, self._map_dtype_to_sqlalchemy(dtype))
            for col, dtype in df.dtypes.items()
        ]

        table = Table(
            table,
            self.metadata,
            *columns,
            schema=schema
        )

        try:
            table.create(self.engine)
            logger.info(f"Created table {schema}.{table}")
        except ProgrammingError:
            logger.info(f"Table {schema}.{table} already exists")

    def _get_table_schema(self, schema, table):
        inspector = inspect(self.engine)
        full_table_name = f"{schema}.{table}"
        if full_table_name not in [f"{sch}.{tbl}" for sch in inspector.get_schema_names() for tbl in
                                   inspector.get_table_names(schema=sch)]:
            raise ValueError(f"Table {full_table_name} doesn't exist")

        columns = inspector.get_columns(table, schema=schema)
        schema_dict = {col['name']: col['type'] for col in columns}
        return schema_dict

    def _adjust_dataframe_to_schema(self, df, schema, table):
        schema_dict = self._get_table_schema(schema, table)
        df_adjusted = df.copy()

        for col_name, col_type in schema_dict.items():
            col_type_str = str(col_type).lower()

            if col_name not in df_adjusted.columns:
                if 'int' in col_type_str:
                    df_adjusted[col_name] = pd.Series([pd.NA] * len(df_adjusted), dtype='Int64')
                elif any(t in col_type_str for t in ['float', 'double', 'numeric']):
                    df_adjusted[col_name] = pd.Series([np.nan] * len(df_adjusted), dtype='float64')
                elif 'boolean' in col_type_str:
                    df_adjusted[col_name] = pd.Series([pd.NA] * len(df_adjusted), dtype='boolean')
                elif 'timestamp' in col_type_str or 'date' in col_type_str:
                    df_adjusted[col_name] = pd.Series([pd.NaT] * len(df_adjusted), dtype='datetime64[ns]')
                else:
                    df_adjusted[col_name] = pd.Series([None] * len(df_adjusted), dtype='object')

                continue

            try:
                series = df_adjusted[col_name]

                if 'int' in col_type_str:
                    df_adjusted[col_name] = pd.to_numeric(series, errors='coerce').astype('Int64')

                elif any(t in col_type_str for t in ['float', 'double', 'numeric']):
                    series = series.astype(str).str.strip()
                    non_null_series = series.dropna()

                    if not non_null_series.empty and non_null_series.str.contains('%').any():
                        cleaned = series.str.replace('%', '', regex=False)
                        df_adjusted[col_name] = pd.to_numeric(cleaned, errors='coerce')

                    elif not non_null_series.empty and non_null_series.str.contains('$').any():
                        cleaned = (
                            series
                            .str.replace('$', '', regex=False)
                            .str.replace(',', '', regex=False)
                        )
                        df_adjusted[col_name] = pd.to_numeric(cleaned, errors='coerce')

                    else:
                        cleaned = series.str.replace(r'[^0-9.\-]+', '', regex=True)
                        df_adjusted[col_name] = pd.to_numeric(cleaned, errors='coerce')

                    df_adjusted[col_name] = df_adjusted[col_name].astype('float64')

                elif any(t in col_type_str for t in ['varchar', 'text', 'char']):
                    df_adjusted[col_name] = series.astype(str).replace({'nan': None, 'None': None})

                elif 'boolean' in col_type_str:
                    df_adjusted[col_name] = series.map({
                        'True': True, 'False': False,
                        True: True, False: False,
                        '1': True, '0': False,
                        1: True, 0: False
                    }).astype('boolean')

                elif 'timestamp' in col_type_str or 'date' in col_type_str:
                    df_adjusted[col_name] = pd.to_datetime(series, errors='coerce')
            except Exception as e:
                logger.error(e)

        df_adjusted = df_adjusted[[col for col in df_adjusted.columns if col in schema_dict]]
        return df_adjusted

    # def _adjust_dataframe_to_schema(self, df, schema, table):
    #     schema_dict = self._get_table_schema(schema, table)
    #     df_adjusted = df.copy()
    #
    #     # logger.info(f"Исходные типы данных в DataFrame: {df.dtypes.to_dict()}")
    #     # logger.info(f"Схема таблицы {schema}.{table}: {schema_dict}")
    #
    #     for col_name, col_type in schema_dict.items():
    #         if col_name not in df_adjusted.columns:
    #             # logger.warning(f"Столбец {col_name} отсутствует в DataFrame, добавляем с None")
    #             df_adjusted[col_name] = None
    #         else:
    #             try:
    #                 col_type_str = str(col_type).lower()
    #                 # logger.debug(f"Обработка столбца {col_name} с типом {col_type_str}")
    #
    #                 if 'int' in col_type_str:
    #                     df_adjusted[col_name] = pd.to_numeric(df_adjusted[col_name], errors='coerce').astype('Int64')
    #                 elif 'float' in col_type_str or 'double' in col_type_str or 'numeric' in col_type_str:
    #                     df_adjusted[col_name] = pd.to_numeric(df_adjusted[col_name], errors='coerce')
    #                 elif 'varchar' in col_type_str or 'text' in col_type_str or 'char' in col_type_str:
    #                     df_adjusted[col_name] = df_adjusted[col_name].astype(str).replace('nan', None)
    #                 elif 'boolean' in col_type_str:
    #                     df_adjusted[col_name] = df_adjusted[col_name].map(
    #                         {'True': True, 'False': False, True: True, False: False, '1': True, '0': False, 1: True,
    #                          0: False}
    #                     ).astype('boolean')
    #                 elif 'timestamp' in col_type_str or 'date' in col_type_str:
    #                     df_adjusted[col_name] = pd.to_datetime(df_adjusted[col_name], errors='coerce')
    #                 else:
    #                     logger.warning(f"Type {col_type} for column {col_name} don't processed")
    #             except Exception as e:
    #                 logger.error(f"Column processing error {col_name} ({col_type}): {e}")
    #
    #     df_adjusted = df_adjusted[[col for col in df_adjusted.columns if col in schema_dict.keys()]]
    #
    #     # logger.info(f"Итоговые типы данных в DataFrame: {df_adjusted.dtypes.to_dict()}")
    #     return df_adjusted

    @staticmethod
    def _map_dtype_to_sqlalchemy(col_type_str):
        t = str(col_type_str).lower()
        if 'bigint' in t or t in ('int8',):
            return BigInteger
        if 'int' in t or any(x in t for x in ('smallint', 'int4', 'int2')):
            # return Integer
            return BigInteger
        if 'double' in t or 'float' in t or 'numeric' in t or 'decimal' in t:
            return Float
        if 'bool' in t:
            return Boolean
        if 'timestamp' in t or 'date' in t:
            return DateTime
        if 'text' in t or 'char' in t or 'varchar' in t:
            return String
        return String
        # dtype = str(dtype).lower()
        # if "int" in dtype:
        #     return c.BIGINT
        # elif "float" in dtype:
        #     return c.FLOAT
        # elif "object" in dtype or "string" in dtype:
        #     return c.TEXT
        # elif "datetime" in dtype:
        #     return c.TIMESTAMP
        # elif "bool" in dtype:
        #     return c.BOOLEAN
        # else:
        #     logger.warning(f"Unknown dtype {dtype}, defaulting to TEXT")
        #     return c.TEXT

    @staticmethod
    def _convert_datetime_columns(df):
        for col in df.columns:
            sample = df[col].dropna().astype(str).iloc[0] if not df[col].dropna().empty else ''
            if isinstance(sample, str) and sample.startswith(datetime.now().year.__str__()):
                try:
                    df[col] = pd.to_datetime(df[col])
                    logger.info(f"Auto-converted column '{col}' to datetime")
                except Exception:
                    pass
        return df

    @staticmethod
    def read_file(file_path: str, skip_rows: int = 0) -> pd.DataFrame:
        if file_path.endswith(".csv"):
            try:
                return pd.read_csv(file_path, skiprows=skip_rows)
            except UnicodeDecodeError:
                logger.warning(f"UTF-8 decode failed for {file_path}, trying cp1252...")
                try:
                    return pd.read_csv(file_path, skiprows=skip_rows, encoding="cp1252")
                except UnicodeDecodeError:
                    logger.warning(f"cp1252 decode failed for {file_path}, falling back to latin1...")
                    return pd.read_csv(file_path, skiprows=skip_rows, encoding="latin1")
        elif file_path.endswith(".xlsx"):
            return pd.read_excel(file_path, skiprows=skip_rows)

    @staticmethod
    def camel_to_snake(name: str) -> str:
        name = re.sub(r'([a-zA-Z])(\d)', r'\1_\2', name)
        name = re.sub(r'([a-z])([A-Z])', r'\1_\2', name)
        return name.lower()

    def clean_column_name(self, col: str, is_camel: bool) -> str:
        if is_camel:
            col = self.camel_to_snake(col)

        cleaned_name: str = col.lower().replace(" ", "_")

        if "percentage" in cleaned_name:
            cleaned_name = cleaned_name.replace("percentage", "pct")

        for char in string.punctuation:
            if char == "%":
                cleaned_name: str = cleaned_name.replace(char, "percent")
            else:
                cleaned_name: str = cleaned_name.replace(char, "_")

        cleaned_name: str = re.sub(r"_+", "_", cleaned_name)
        cleaned_name: str = cleaned_name.strip("_")

        if cleaned_name and cleaned_name[0].isdigit():
            cleaned_name: str = f"col_{cleaned_name}"

        cleaned_name: str = cleaned_name[:63]
        return cleaned_name

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

    @utils.exception
    def update_data(
            self,
            df: pd.DataFrame,
            dataset: str,
            table: str,
            write_disposition: str = "WRITE_APPEND",
            deduplicate: bool = True
    ) -> bool:
        schema = dataset.lower()
        table = table.lower()
        temp_table = f"{table}_temp"

        # self._create_schema(schema)
        self._create_table(df, schema, table)

        schema_dict = self._get_table_schema(schema, table)
        dtype_mapping = {
            col: self._map_dtype_to_sqlalchemy(str(col_type))
            for col, col_type in schema_dict.items()
        }
        df = self._adjust_dataframe_to_schema(df, schema, table)

        if write_disposition == "WRITE_TRUNCATE":
            df.to_sql(
                table,
                self.engine,
                schema=schema,
                if_exists="replace",
                index=False,
                method="multi",
                dtype=dtype_mapping
            )
        elif write_disposition == "WRITE_APPEND" and not deduplicate:
            df.to_sql(
                table,
                self.engine,
                schema=schema,
                if_exists="append",
                index=False,
                method="multi",
                dtype=dtype_mapping
            )
        else:
            df = df.drop_duplicates()

            delete_sql = None

            if table == "transaction":
                delete_sql = text(f"""
                             DELETE FROM {schema}."{table}"
                             WHERE EXTRACT(YEAR FROM date_time) = EXTRACT(YEAR FROM CURRENT_DATE)
                               AND EXTRACT(MONTH FROM date_time) = EXTRACT(MONTH FROM CURRENT_DATE);
                         """)

                if "order_postal" in df.columns:
                    df["order_postal"] = df["order_postal"].apply(
                        lambda x: re.sub(r"\.\d+$", "", str(x))
                        if str(x).replace(".", "", 1).replace("-", "", 1).isdigit()
                        else str(x)
                    )

            df.to_sql(
                temp_table,
                self.engine,
                schema=schema,
                if_exists="replace",
                index=False,
                method="multi",
                dtype=dtype_mapping
            )

            all_cols = ', '.join(f'"{col}"' for col in df.columns)

            if table in ["fba_inventory", "manage_fba_inventory"] or table.endswith("_campaign"):
                unique_cols = {
                    "fba_inventory": ["snapshot_date", "sku"],
                    "manage_fba_inventory": ["date", "sku"],
                    "_campaign": ["campaign_id", "start_date", "end_date"]
                }

                table_unique_cols = unique_cols["_campaign"] if table.endswith("_campaign") else unique_cols[table]

                conditions = []
                for col in table_unique_cols:
                    col_type = str(schema_dict.get(col, ''))
                    if 'timestamp' in col_type.lower() or 'date' in col_type.lower():
                        conditions.append(f'DATE(target."{col}") = DATE(source."{col}")')
                    else:
                        conditions.append(f'target."{col}" = source."{col}"')

                conditions_str = ' AND '.join(conditions)

                delete_sql = text(f"""
                                    DELETE FROM {schema}."{table}" AS target
                                    WHERE EXISTS (
                                        SELECT 1 
                                        FROM {schema}."{temp_table}" AS source
                                        WHERE {conditions_str}
                                    );
                                """)

                insert_sql = text(f"""
                                    INSERT INTO {schema}."{table}" ({all_cols})
                                    SELECT {all_cols}
                                    FROM {schema}."{temp_table}";
                                """)
            else:
                conditions = []
                for col in df.columns:
                    col_type = str(schema_dict.get(col, ''))
                    if 'timestamp' in col_type.lower() or 'date' in col_type.lower():
                        conditions.append(f'(DATE(target."{col}") IS NOT DISTINCT FROM DATE(source."{col}"))')
                    else:
                        conditions.append(f'(target."{col}" IS NOT DISTINCT FROM source."{col}")')

                conditions_str = ' AND '.join(conditions)

                insert_sql = text(f"""
                            INSERT INTO {schema}."{table}" ({all_cols})
                            SELECT {all_cols}
                            FROM {schema}."{temp_table}" AS source
                            WHERE NOT EXISTS (
                                SELECT 1 FROM {schema}."{table}" AS target
                                WHERE {conditions_str}
                            );
                        """)

            with self.engine.begin() as conn:
                if delete_sql is not None:
                    conn.execute(delete_sql)

                conn.execute(insert_sql)
                conn.execute(text(f'DROP TABLE IF EXISTS {schema}."{temp_table}"'))

            logger.info(f"Inserted rows into {schema}.{table}")
            return True

    def add_report(
            self,
            file_path: str,
            dataset: str,
            table: str,
            skip_rows: int = 0,
            add_date: bool = False,
            is_camel: bool = False,
            custom_date: t.Optional[str] = None,
            period: t.Optional[str] = None,
            asin: t.Optional[str] = None,
            write_disposition: str = "WRITE_APPEND"
    ) -> bool:
        df: pd.DataFrame = self.read_file(file_path=file_path, skip_rows=skip_rows)

        if len(df) == 0:
            logger.warning("dataframe is empty")
            return False

        df.columns = [self.clean_column_name(col, is_camel) for col in df.columns]

        if add_date:
            df: pd.DataFrame = self.add_column(df=df)

        if custom_date:
            df: pd.DataFrame = self.add_column(df=df, custom_date=custom_date)

        if period:
            df: pd.DataFrame = self.add_column(df=df, period=period)

        if asin:
            df: pd.DataFrame = self.add_column(df=df, asin=asin)

        df = self._convert_datetime_columns(df)
        print(df.columns)
        if not self.update_data(df=df, dataset=dataset, table=table, write_disposition=write_disposition):
            return False

        logger.info(f"report was added :: {dataset}.{table}")
        return True

    def get_all_from_table(self, schema_name: str, table_name: str) -> list:
        with self.engine.connect() as connection:
            result = connection.execute(text(f"SELECT * FROM {schema_name}.{table_name}"))
            columns = result.keys()
            return [dict(zip(columns, row)) for row in result.fetchall()]


postgres_db: PostgresDB = PostgresDB()
# postgres_db.add_report(
#     file_path="/home/user/projects/PycharmProjects/amazon_reports/reports/transaction_8_2025.csv",
#     dataset="csv",
#     table="transaction_test",
#     skip_rows=7
# )
