import asyncio
import os
import sys
import time
from contextlib import asynccontextmanager
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from redis.asyncio import Redis
from sqlalchemy import make_url

from ex_agent.config import Settings
from ex_agent.maintenance.contracts import StreamMaintenanceRequest
from ex_agent.maintenance.operations import StreamMaintenanceOperations
from ex_agent.maintenance.recovery import StreamMaintenanceRecovery
from ex_agent.maintenance.store import (
    StreamMaintenanceConflict,
    StreamMaintenanceStore,
)
from ex_agent.persistence.database import create_engine, create_session_factory

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(
        not os.getenv("TEST_DATABASE_URL") or not os.getenv("TEST_REDIS_URL"),
        reason="Requires isolated PostgreSQL and Redis",
    ),
]


@asynccontextmanager
async def maintenance_harness():
    source = make_url(os.environ["TEST_DATABASE_URL"])
    admin = source.set(drivername="postgresql").render_as_string(
        hide_password=False
    )
    database = f"agent_maintenance_test_{uuid4().hex}"
    async with await psycopg.AsyncConnection.connect(
        admin,
        autocommit=True,
    ) as connection:
        await connection.execute(
            sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database))
        )
    scoped = source.set(database=database)
    database_url = scoped.render_as_string(hide_password=False)
    process = await asyncio.create_subprocess_exec(
        sys.executable,
        "-m",
        "alembic",
        "upgrade",
        "head",
        env={**os.environ, "AGENT_DATABASE_URL": database_url},
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await asyncio.wait_for(process.communicate(), 30)
    assert process.returncode == 0, (stdout + stderr).decode()
    engine = create_engine(database_url)
    redis = Redis.from_url(
        os.environ["TEST_REDIS_URL"],
        decode_responses=True,
    )
    try:
        yield create_session_factory(engine), redis
    finally:
        await redis.aclose()
        await engine.dispose()
        async with await psycopg.AsyncConnection.connect(
            admin,
            autocommit=True,
        ) as connection:
            await connection.execute(
                sql.SQL("DROP DATABASE {}").format(sql.Identifier(database))
            )


async def test_worker_trim_is_durable_exclusive_and_idempotent() -> None:
    async with maintenance_harness() as (sessions, redis):
        stream = f"test-maintenance-{uuid4()}"
        settings = Settings(
            agent_command_stream=stream,
            stream_retention_seconds=60,
            stream_minimum_retained_entries=1,
            stream_maintenance_operator_user_ids="operator-1",
            stream_maintenance_retry_seconds=0.01,
            stream_maintenance_poll_seconds=0.01,
        )
        store = StreamMaintenanceStore(sessions)
        recovery = StreamMaintenanceRecovery(
            store,
            redis,
            poll_seconds=0.01,
            retry_seconds=0.01,
        )
        operations = StreamMaintenanceOperations(settings, store, recovery)
        now_ms = int(time.time() * 1000)
        await redis.xadd(stream, {"value": "1"}, id=f"{now_ms - 90000}-0")
        await redis.xadd(stream, {"value": "2"}, id=f"{now_ms - 80000}-0")
        await redis.xadd(stream, {"value": "3"}, id=f"{now_ms - 70000}-0")
        request = StreamMaintenanceRequest(
            stream="agent_commands",
            idempotency_key="trim-1",
            reason="integration retention",
        )
        try:
            plan = await operations.plan(
                actor="operator-1",
                request=request.model_copy(
                    update={"idempotency_key": "plan-1"}
                ),
            )
            assert plan.job.state == "SUCCEEDED"
            assert plan.job.result is not None
            assert plan.job.result["stream_length"] == 3
            assert plan.job.updated_by == "operator-1"
            assert await redis.xlen(stream) == 3

            submitted = await operations.submit_trim(
                actor="operator-1",
                request=request,
            )
            assert submitted.job.state == "PENDING"
            with pytest.raises(
                StreamMaintenanceConflict,
                match="Another trim is active",
            ):
                await operations.submit_trim(
                    actor="operator-1",
                    request=request.model_copy(
                        update={"idempotency_key": "trim-2"}
                    ),
                )

            assert await recovery.once() == 1
            completed = await operations.detail(
                submitted.job.job_id,
                actor="operator-1",
            )
            assert completed.state == "SUCCEEDED"
            assert completed.result is not None
            assert completed.result["removed_entries"] == 2
            assert completed.result["result_recalculated_after_retry"] is False
            assert completed.created_by == "operator-1"
            assert completed.updated_by == "WORKER"
            assert await redis.xlen(stream) == 1

            replay = await operations.submit_trim(
                actor="operator-1",
                request=request,
            )
            assert replay.operation_replayed
            assert replay.job.job_id == submitted.job.job_id

            await redis.xadd(
                stream,
                {"value": "4"},
                id=f"{now_ms - 65000}-0",
            )
            await redis.xadd(
                stream,
                {"value": "5"},
                id=f"{now_ms - 64000}-0",
            )
            crashed = await operations.submit_trim(
                actor="operator-1",
                request=request.model_copy(
                    update={"idempotency_key": "trim-3"}
                ),
            )
            claimed = await store.claim(
                crashed.job.job_id,
                claim_timeout_seconds=0.01,
            )
            assert claimed is not None and claimed.attempts == 1
            await asyncio.sleep(0.02)
            crash_recovery = StreamMaintenanceRecovery(
                store,
                redis,
                claim_timeout_seconds=0.01,
                poll_seconds=0.01,
                retry_seconds=0.01,
            )
            assert await crash_recovery.once() == 1
            recovered = await operations.detail(
                crashed.job.job_id,
                actor="operator-1",
            )
            assert recovered.attempts == 2
            assert recovered.result is not None
            assert recovered.result["result_recalculated_after_retry"]
        finally:
            await redis.delete(stream)
