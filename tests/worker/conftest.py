import asyncio
import os
import sys
from pathlib import Path
from uuid import uuid4

import pytest

from worker import ExecutorWorker, Settings

ALEMBIC_CONFIG = (
    Path(__file__).resolve().parents[2] / "worker_migrations" / "alembic.ini"
)


@pytest.fixture(scope="session")
async def migrated_database_url():
    if not os.getenv("TEST_DATABASE_URL") or not os.getenv("TEST_REDIS_URL"):
        # ty currently mis-infers pytest's _with_exception decorator.
        pytest.skip("Requires isolated PostgreSQL and Redis")  # ty: ignore
    database_url = os.environ["TEST_DATABASE_URL"].replace(
        "postgresql+psycopg://", "postgresql://"
    )
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "alembic",
        "-c",
        str(ALEMBIC_CONFIG),
        "upgrade",
        "head",
        env={**os.environ, "EW_DATABASE_URL": database_url},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        stdout, stderr = await asyncio.wait_for(process.communicate(), 30)
    finally:
        if process.returncode is None:
            process.kill()
            await process.wait()
    assert process.returncode == 0, (stdout + stderr).decode()
    return database_url


@pytest.fixture
async def worker(migrated_database_url):
    settings = Settings(
        database_url=migrated_database_url,
        redis_url=os.environ["TEST_REDIS_URL"],
        namespace=f"test-ew-{uuid4()}",
        executor_event_stream=f"test-executor-{uuid4()}",
        claim_idle_milliseconds=2100,
        concurrency=2,
        poll_seconds=0.02,
        idle_poll_seconds=0.05,
        health_port=0,
        shutdown_seconds=0.05,
    )
    async with ExecutorWorker(settings, {}) as instance:
        yield instance
