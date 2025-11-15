from pathlib import Path

import gspread
import pandas as pd
from google.auth.credentials import Credentials
from oauth2client.service_account import ServiceAccountCredentials

try:
    from loggers.logger import logger
    from utils.decorators import utils
    from settings.config import config
    from database.big_query import big_query
except ImportError as ie:
    exit(f"{ie} :: {Path(__file__).resolve()}")


class GoogleSheets:
    @utils.exception
    def get_worksheet(self, category: str) -> gspread.Worksheet:
        credentials: Credentials = ServiceAccountCredentials.from_json_keyfile_name(
            filename=config.service_account_path,
            scopes=config.GOOGLE_SHEETS["scopes"],
        )
        client: gspread.Client = gspread.authorize(credentials=credentials)

        worksheet: gspread.Worksheet = client.open_by_url(
            url=config.GOOGLE_SHEETS["sheet_url"][category]).worksheet(
            title=config.GOOGLE_SHEETS["sheet_name"][category]
        )
        return worksheet

    @utils.exception
    def worksheet_to_dataframe(self, category: str) -> pd.DataFrame:
        worksheet: gspread.Worksheet = self.get_worksheet(category=category)
        data: list = worksheet.get_all_values()

        if not data:
            logger.error("no data found")
            return

        headers: list = data[0]
        rows: list = data[1:]
        return pd.DataFrame(rows, columns=headers)


gs: GoogleSheets = GoogleSheets()
