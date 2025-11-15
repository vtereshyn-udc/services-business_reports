import functools
from pathlib import Path

from playwright.async_api import async_playwright, Playwright

try:
    from loggers.logger import logger
except ImportError as ie:
    exit(f"{ie} :: {Path(__file__).resolve()}")


class Utils:
    @staticmethod
    def exception(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                logger.error(f"exception in '{func.__name__}' => {e}")
                return False

        return wrapper

    @staticmethod
    def async_exception(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                return await func(*args, **kwargs)
            except Exception as e:
                logger.error(f"exception in '{func.__name__}' => {e}")
                return False
        return wrapper

    @staticmethod
    def playwright_initiator(func):
        @functools.wraps(func)
        async def wrapper(*args, **kwargs):
            playwright_context: Playwright = await async_playwright().start()
            try:
                kwargs["playwright"] = playwright_context
                return await func(*args, **kwargs)
            except Exception as e:
                logger.exception(f"exception in '{func.__name__}' => {e}")
                return False
            finally:
                await playwright_context.stop()
        return wrapper


utils: Utils = Utils()
