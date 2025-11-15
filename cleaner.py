import os
import re
from pathlib import Path
from datetime import datetime, timedelta

try:
    from loggers.logger import logger
    from utils.decorators import utils
    from settings.config import config
except ImportError as ie:
    exit(f"{ie} :: {Path(__file__).resolve()}")


@utils.exception
def clean_logs():
    date_patterns: list = [
        (re.compile(r"(\d{2}_\d{2}_\d{4})"), "%d_%m_%Y"),
        (re.compile(r"(\d{4}-\d{2}-\d{2})"), "%Y-%m-%d")
    ]
    cutoff_date: datetime = datetime.now() - timedelta(days=5)

    for filename in os.listdir(config.logs_path):
        file_date = None

        for pattern, date_format in date_patterns:
            match: re.Match = pattern.search(filename)
            if match:
                file_date_str: str = match.group(1)
                try:
                    file_date = datetime.strptime(file_date_str, date_format)
                    break
                except ValueError:
                    continue

        if not file_date:
            continue

        if file_date > cutoff_date:
            continue

        file_path: str = os.path.join(config.logs_path, filename)
        os.remove(file_path)
        logger.info(f"removed :: {file_path}")
