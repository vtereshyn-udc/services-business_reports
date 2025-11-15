from pathlib import Path

from twocaptcha import TwoCaptcha

try:
    from loggers.logger import logger
    from utils.decorators import utils
    from settings.config import config
except ImportError as ie:
    exit(f"{ie} :: {Path(__file__).resolve()}")


solver: TwoCaptcha = TwoCaptcha(apiKey=config.TWO_CAPTCHA)


@utils.async_exception
async def solve_captcha(file: str) -> str:
    try:
      result: dict = solver.normal(file=file)
      logger.info(result)
      return result.get("code")
    except Exception as e:
      logger.error(e)
