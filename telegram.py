import asyncio
from pathlib import Path

from aiogram import Bot

try:
    from loggers.logger import logger
    from settings.config import config
    from database.database import db
except ImportError as ie:
    exit(f"{ie} :: {Path(__file__).resolve()}")


bot: Bot = Bot(token=config.BOT_TOKEN)


async def send_notification(message: str):
    await bot.send_message(chat_id=config.CHAT_ID, text=message)


async def bot_task():
    while True:
        failed_tasks: list = await db.get_today_tasks()
        if failed_tasks:
            for task in failed_tasks:
                message: str = (f"task_id: {task.get('task_id')}\n"
                                f"user_id: {task.get('user_id')}\n"
                                f"service: {task.get('service')}\n"
                                f"category: {task.get('category')}\n"
                                f"status: {task.get('status')}\n"
                                f"created_at: {task.get('created_at')}")

                await send_notification(message=message)
        else:
            logger.info("not found failed tasks")

        await asyncio.sleep(3600)
