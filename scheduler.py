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
    async def get_next_date_by_day(self, day: int, next_month: bool = False) -> datetime:
        today = datetime.now().date()

        if not next_month and day >= today.day:
            return today.replace(day=day)

        next_month = today + relativedelta(months=1)
        return next_month.replace(day=day)

    @utils.async_exception
    async def set_exact_time(
            self,
            time_range: list,
            is_first: bool = False,
            last_date: t.Optional[datetime] = None
    ) -> dict:
        now_time: datetime = datetime.now().time()
        time_range = [time_range[0]] if is_first else time_range

        for period in time_range:
            start_dt: datetime = datetime.strptime(period["start"], "%H:%M").time()
            end_dt: datetime = datetime.strptime(period["end"], "%H:%M").time()
            now_dt: datetime = now_time

            if isinstance(last_date, datetime):
                # print(last_date.date() == datetime.now().date())
                # print(start_dt <= last_date.time() <= end_dt)
                # print(start_dt, last_date.time(), end_dt)
                if last_date.date() == datetime.now().date() and start_dt <= last_date.time() <= end_dt:
                    continue

            if now_dt <= start_dt or is_first:
                base_time = start_dt
            elif start_dt < now_dt < end_dt:
                base_time = now_dt
            else:
                continue

            start_minutes = base_time.hour * 60 + base_time.minute
            end_minutes = end_dt.hour * 60 + end_dt.minute

            if end_minutes <= start_minutes:
                end_minutes += 24 * 60

            delta_minutes = end_minutes - start_minutes
            random_minutes = random.randint(1, delta_minutes)
            final_minutes = start_minutes + random_minutes

            hour = (final_minutes // 60) % 24
            minute = final_minutes % 60

            return {"hour": hour, "minute": minute}

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
    async def start_service(self, *args, job_id: str, time_range: dict, day: int, job_type: str) -> None:
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
                start_time = await self.set_exact_time(time_range=time_range, last_date=datetime.now())

                if job_type == "daily" and not start_time:
                    start_date += timedelta(days=1)
                elif job_type == "weekly":
                    start_date += timedelta(weeks=1)
                elif job_type == "monthly":
                    start_date += relativedelta(months=1)

                if day:
                    start_date = await self.get_next_date_by_day(day=day, next_month=True)

                if start_date != datetime.now().date():
                    start_time = await self.set_exact_time(time_range=time_range, is_first=True)

                self.scheduler.reschedule_job(job_id, trigger=CronTrigger(**start_time, start_date=start_date))
                logger.info(f"updated job :: {job_id} :: {start_time} :: {start_date}")

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
            time_range: list = schedule.get("time_range")
            day: int = schedule.get("day")

            if not time_range:
                continue

            start_time: dict = await self.set_exact_time(time_range=time_range, last_date=last_date)

            if not isinstance(last_date, datetime):
                if not start_time:
                    start_date = datetime.now().date() + timedelta(days=1)
            else:
                if schedule["type"] == "daily" and not start_time:
                    start_date = last_date.date() + timedelta(days=1)
                elif schedule["type"] == "weekly":
                    start_date = last_date.date() + timedelta(weeks=1)
                elif schedule["type"] == "monthly":
                    start_date = last_date.date() + relativedelta(months=1)

                if start_date <= datetime.now().date():
                    if not start_time:
                        start_date = datetime.now().date() + timedelta(days=1)
                    else:
                        start_date = datetime.now().date()

            if day:
                start_date = await self.get_next_date_by_day(day=day)

            if start_date != datetime.now().date():
                start_time = await self.set_exact_time(time_range=time_range, is_first=True)

            job_id = str(schedule["args"])
            logger.info(f"add job :: {job_id} :: {start_time} :: {start_date}")

            self.scheduler.add_job(
                self.start_service,
                trigger=CronTrigger(**start_time, start_date=start_date),
                args=schedule["args"],
                kwargs={
                    "job_id": job_id,
                    "time_range": schedule["time_range"],
                    "day": schedule.get("day"),
                    "job_type": schedule["type"]
                },
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
