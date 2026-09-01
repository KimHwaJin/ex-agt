"""Deployment migration entrypoint for Agent and LangGraph schemas."""

import asyncio
import os

from alembic import command
from alembic.config import Config
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from ex_agent.config import Settings


async def migrate() -> None:
    settings = Settings()
    await asyncio.to_thread(command.upgrade, Config("alembic.ini"), "head")
    os.environ["EW_DATABASE_URL"] = settings.agent_checkpoint_database_url
    await asyncio.to_thread(
        command.upgrade,
        Config("worker_migrations/alembic.ini"),
        "head",
    )
    async with AsyncPostgresSaver.from_conn_string(
        settings.agent_checkpoint_database_url
    ) as saver:
        await saver.setup()


def run_migrations() -> None:
    asyncio.run(migrate())


if __name__ == "__main__":
    run_migrations()
