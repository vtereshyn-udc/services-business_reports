import re
import random
import asyncio
import platform
import typing as t
from pathlib import Path
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta

from apscheduler.triggers.cron import CronTrigger
from apscheduler.schedulers.asyncio import AsyncIOScheduler

try:
    import win32api
    import win32con
except ImportError as ie:
    win32api = win32con = None if platform.system() != "Windows" else exit(f"{ie} :: {Path(__file__).resolve()}")

try:
    from loggers.logger import logger
    from utils.decorators import utils
    from settings.config import config
    from database.database import db
    from notifications.telegram import bot_task
except ImportError as ie:
    exit(f"{ie} :: {Path(__file__).resolve()}")


class Scheduler:
    def __init__(self):
        self.scheduler: AsyncIOScheduler = AsyncIOScheduler()
        self.lock = dict()

    @utils.async_exception
    async def keep_alive(self) -> None:
        if platform.system() == "Windows" and win32api and win32con:
            while True:
                try:
                    win32api.keybd_event(win32con.VK_SHIFT, 0, 0, 0)
                    win32api.keybd_event(win32con.VK_SHIFT, 0, win32con.KEYEVENTF_KEYUP, 0)
                    logger.debug("keep-alive event")
                except Exception as e:
                    logger.error(e)

                await asyncio.sleep(300)

    @utils.async_exception
    async def get_random_time(self, time_range: dict, start_time: t.Optional[int] = None) -> dict:
        start_hour, end_hour = map(int, time_range["hour"].split("-"))

        random_hour: int = random.randint(start_time if start_time else start_hour, end_hour - 1)
        random_minute: int = random.randint(0, 59)

        return {"hour": random_hour, "minute": random_minute}

    @utils.async_exception
    async def active_process(self, user_id: str) -> bool:
        logger.info(f"checking active process :: user_id={user_id}")

        try:
            process: asyncio.subprocess.Process = await asyncio.create_subprocess_exec(
                "wmic", "process", "where", "name='python.exe'", "get", "commandline",
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )
            stdout, stderr = await process.communicate()
            if stderr:
                logger.error(f"WMIC error :: {stderr.decode()}")
                return False

            output: str = stdout.decode()
            for line in output.splitlines():
                line: str = line.strip()
                if user_id in line:
                    match: re.Match = re.search(r"--user=(\d+)", line)
                    if match and match.group(1) == user_id:
                        logger.info(f"found active process :: user_id={user_id}")
                        return True

            logger.info(f"no active process :: user_id={user_id}")
            return False
        except Exception as e:
            logger.error(f"error checking process :: {e}")
            return False

    @utils.async_exception
    async def start_service(self, *args, job_id: str, time_range: dict, job_type: str) -> None:
        try:
            user_id: str = [arg.split("=")[1] for arg in args if arg.startswith("--user=")][0]
        except IndexError:
            return

        if user_id not in self.lock:
            self.lock[user_id] = asyncio.Lock()

        async with self.lock[user_id]:
            while await self.active_process(user_id=user_id):
                logger.info(f"waiting for active process :: user_id={user_id}")
                await asyncio.sleep(60)

            await asyncio.create_subprocess_exec(
                "poetry", "run", "python", config.main_script_path, *args
            )
            logger.info(f"service executed :: {job_id}")

            if job_id and time_range:
                start_date: datetime = datetime.now().date()
                if job_type == "daily":
                    start_date += timedelta(days=1)
                elif job_type == "weekly":
                    start_date += timedelta(weeks=1)
                elif job_type == "monthly":
                    start_date += relativedelta(months=1)

                random_time: dict = await self.get_random_time(time_range=time_range)
                self.scheduler.reschedule_job(job_id, trigger=CronTrigger(**random_time, start_date=start_date))
                logger.info(f"updated job :: {job_id} :: {random_time} :: {start_date}")

    @utils.async_exception
    async def create_job(self) -> None:
        for schedule in config.SCHEDULE:
            if not schedule["enabled"]:
                continue

            task = dict()
            for arg in schedule["args"]:
                arg_name, arg_value = arg.split("=")

                if "user" in arg_name:
                    task["user_id"] = arg_value
                if "service" in arg_name:
                    task["service"] = arg_value
                if "category" in arg_name:
                    task["category"] = arg_value

            start_date: datetime = datetime.now().date()
            last_date: datetime = await db.get_task(task=task)

            if isinstance(last_date, datetime):
                if schedule["type"] == "daily":
                    start_date: datetime = last_date.date() + timedelta(days=1)
                elif schedule["type"] == "weekly":
                    start_date: datetime = last_date.date() + timedelta(weeks=1)
                elif schedule["type"] == "monthly":
                    start_date: datetime = last_date.date() + relativedelta(months=1)

                if start_date < datetime.now().date():
                    start_date: datetime = datetime.now().date()

            start_time: t.Optional[int] = None
            if start_date == datetime.now().date():
                start_hour, end_hour = map(int, schedule["time_range"]["hour"].split("-"))
                now_hour: int = datetime.now().hour
                is_next_day: bool = False

                if now_hour >= end_hour:
                    is_next_day: bool = True

                if not is_next_day and (now_hour > start_hour):
                    start_time: int = now_hour + 1

                if is_next_day:
                    start_date += timedelta(days=1)

            job_id = str(schedule["args"])
            random_time: dict = await self.get_random_time(time_range=schedule["time_range"], start_time=start_time)
            logger.info(f"add job :: {job_id} :: {random_time} :: {start_date}")

            self.scheduler.add_job(
                self.start_service,
                trigger=CronTrigger(**random_time, start_date=start_date),
                args=schedule["args"],
                kwargs={"job_id": job_id, "time_range": schedule["time_range"], "job_type": schedule["type"]},
                id=job_id
            )

    @utils.async_exception
    async def execute(self) -> None:
        await self.create_job()
        self.scheduler.start()
        asyncio.create_task(self.keep_alive())
        asyncio.create_task(bot_task())

        try:
            while True:
                await asyncio.sleep(60)
        finally:
            try:
                process: asyncio.subprocess.Process = await asyncio.create_subprocess_exec(
                    "taskkill", "/IM", "python.exe", "/F",
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE
                )
                await process.communicate()
            except Exception:
                pass

            self.scheduler.shutdown()
            logger.info("scheduler stopped")

    @utils.exception
    def run(self) -> None:
        try:
            asyncio.run(self.execute())
        except (KeyboardInterrupt, asyncio.CancelledError):
            logger.info("service stopped")


if __name__ == "__main__":
    scheduler: Scheduler = Scheduler()
    scheduler.run()
