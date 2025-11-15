import os
import sys
import logging
from pathlib import Path
from datetime import datetime
from logging.handlers import TimedRotatingFileHandler

name: str = sys.argv[0].split("/")[-1].replace(".py", "")
user: str = sys.argv[1].strip("--").replace("=", "_") if len(sys.argv) > 1 else None
service: str = sys.argv[2].split("=")[-1] if len(sys.argv) > 2 else None
category: str = sys.argv[3].split("=")[-1] if len(sys.argv) > 3 else None


class CustomTimedRotatingFileHandler(TimedRotatingFileHandler):
    def __init__(self, filename, when, interval, backupCount):
        self.base_filename = filename.rsplit(".", 1)[0]
        self.suffix = "%d_%m_%Y"
        current_date = datetime.now().strftime(self.suffix)
        filename_with_date = f"{self.base_filename}_{current_date}.log"
        super().__init__(filename_with_date, when, interval, backupCount)
        self.namer = self.custom_namer

    def custom_namer(self, default_name):
        current_date = datetime.now().strftime(self.suffix)
        return f"{self.base_filename}_{current_date}.log"

    def doRollover(self):
        if self.stream:
            self.stream.close()
            self.stream = None

        current_date = datetime.now().strftime(self.suffix)
        new_filename = f"{self.base_filename}_{current_date}.log"

        if new_filename != self.baseFilename:
            self.baseFilename = new_filename
            super().doRollover()

        if not self.stream:
            self.stream = self._open()


class StreamHandler(logging.StreamHandler):
    def __init__(self):
        super().__init__()

    def emit(self, record):
        if user and service:
            msg = self.format(record)
            self.stream.write(f"[{user}][{service}]{f'[{category}]' if category else ''} {msg}\n")
            self.flush()
        else:
            super().emit(record)


def init_logger(file_log: bool = True, stream_log: bool = True) -> logging.Logger:
    log_directory: str = os.path.join(Path(__file__).parent.parent, "logs")
    if not os.path.exists(log_directory):
        try:
            os.makedirs(log_directory)
        except Exception as e:
            exit(f"failed to create log directory on a path :: {log_directory} :: {e}")

    log_name: str = f"{user}-{service}" if user and service else name
    if category:
        log_name += f"-{category}"
    log_filename: str = f"{log_name}.log"
    log_filepath: str = os.path.join(log_directory, log_filename)

    logger: logging.Logger = logging.getLogger(f"{log_name}.error")
    logger.setLevel(logging.DEBUG)
    formatter: logging.Formatter = logging.Formatter(
        fmt=u"%(filename)s[LINE:%(lineno)d]# %(levelname)-8s [%(asctime)s]  %(message)s"
    )

    if file_log:
        file_handler: CustomTimedRotatingFileHandler = CustomTimedRotatingFileHandler(
            filename=log_filepath,
            when="midnight",
            interval=1,
            backupCount=30
        )

        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    if stream_log:
        stream_handler: StreamHandler = StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)

    return logger


logger: logging.Logger = init_logger()
