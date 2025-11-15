from pathlib import Path
from datetime import datetime, timedelta
from contextlib import asynccontextmanager

import aiosqlite

try:
    from loggers.logger import logger
    from utils.decorators import utils
    from settings.config import config
except ImportError as ie:
    exit(f"{ie} :: {Path(__file__).resolve()}")


class Database:
    @asynccontextmanager
    async def connection(self) -> aiosqlite.Connection:
        connection: aiosqlite.Connection = await aiosqlite.connect(database=config.db_path)
        connection.row_factory = aiosqlite.Row

        try:
            yield connection
        finally:
            await connection.close()

    @utils.async_exception
    async def get_today_tasks(self) -> str:
        start_of_hour: str = (datetime.now() - timedelta(hours=1)).strftime("%Y-%m-%d %H:%M:%S")
        end_of_hour: str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        query = """
                SELECT *
                FROM task
                WHERE status = 'failed'
                AND created_at BETWEEN ? AND ?
                """
        values: tuple = (start_of_hour, end_of_hour)

        async with self.connection() as session:
            async with session.execute(query, values) as cursor:
                rows: aiosqlite.Row = await cursor.fetchall()
                return [dict(row) for row in rows]

    @utils.async_exception
    async def get_task(self, task: dict, description: bool = False) -> datetime | int:
        fields: dict = {
            "user_id": task.get("user_id"),
            "service": task.get("service"),
            "category": task.get("category")
        }

        conditions: list = [f"{key} = ?" for key, value in fields.items() if value]
        values: list = [value for value in fields.values() if value]

        query: str = f"""
                SELECT {'description' if description else 'created_at'}
                FROM task
--                 WHERE {' AND '.join(conditions)} AND status != 'started'
                WHERE {' AND '.join(conditions)}
                ORDER BY created_at DESC
                LIMIT 1
            """
        async with self.connection() as session:
            async with session.execute(query, values) as cursor:
                row: aiosqlite.Row = await cursor.fetchone()
                if description:
                    try:
                        return int(dict(row)["description"].split(":")[-1].strip())
                    except (IndexError, ValueError, AttributeError):
                        return 53
                return datetime.strptime(dict(row)["created_at"], "%Y-%m-%d %H:%M:%S") if row else None

    @utils.async_exception
    async def update_task(self, task: dict) -> None:
        query: str = """
            INSERT INTO task (task_id, user_id, service, category, status, created_at, description)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(task_id) DO UPDATE SET
                status = excluded.status,
                description = excluded.description
        """
        values: tuple = (
            task.get("task_id"),
            task.get("user_id"),
            task.get("service"),
            task.get("category"),
            task.get("status"),
            task.get("created_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
            task.get("description")
        )

        async with self.connection() as session:
            await session.execute(query, values)
            await session.commit()

    @utils.async_exception
    async def add_sms(self, sms_to: str, otp_code: str) -> None:
        query: str = """
            INSERT INTO sms (phone, otp_code, created_at)
            VALUES (?, ?, ?)
        """
        values: tuple = (sms_to, otp_code, datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        async with self.connection() as session:
            await session.execute(query, values)
            await session.commit()

    @utils.async_exception
    async def get_sms_code(self, phone: str) -> str:
        threshold: str = (datetime.now() - timedelta(seconds=60)).strftime("%Y-%m-%d %H:%M:%S")
        query: str = """
                SELECT otp_code
                FROM sms
                WHERE phone = ?
                AND created_at > ?
                ORDER BY created_at DESC
                LIMIT 1
            """
        values: tuple = (phone, threshold)

        async with self.connection() as session:
            async with session.execute(query, values) as cursor:
                row: aiosqlite.Row = await cursor.fetchone()
                return dict(row)["otp_code"] if row else None


db: Database = Database()
