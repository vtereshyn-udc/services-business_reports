import asyncio
import time
from pathlib import Path

import pyotp

try:
    from loggers.logger import logger
    from utils.decorators import utils
    from settings.config import config
except ImportError as ie:
    exit(f"{ie} :: {Path(__file__).resolve()}")


@utils.async_exception
async def generate_otp(user_id: str) -> str:
    secret_key = config.get_auth(user_id=user_id)
    totp = pyotp.TOTP(secret_key)

    while True:
        remaining_time = int(totp.interval - (time.time() % totp.interval))

        if remaining_time >= 5:
            break

        logger.info(f"Remaining time: {remaining_time}s")
        await asyncio.sleep(1)

    current_otp = totp.now()
    logger.info(f"OTP: {current_otp}")

    return current_otp
