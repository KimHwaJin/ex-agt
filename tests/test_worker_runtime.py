import asyncio
import os
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from ex_agent.config import Settings
from ex_agent.worker import WorkflowWorker

pytestmark = pytest.mark.skipif(
    not {"TEST_DATABASE_URL", "TEST_REDIS_URL"}.issubset(os.environ),
    reason="Compose database and Redis are not configured",
)


@pytest.mark.redis
@pytest.mark.postgres
@pytest.mark.asyncio
async def test_worker_starts_bounded_graph_slots() -> None:
    database_url = os.environ["TEST_DATABASE_URL"]
    redis_url = os.environ["TEST_REDIS_URL"]
    suffix = uuid4()
    command_stream = f"test-worker-commands-{suffix}"
    executor_stream = f"test-worker-executor-events-{suffix}"
    settings = Settings(
        agent_database_url=database_url,
        agent_checkpoint_database_url=database_url.replace(
            "postgresql+psycopg://",
            "postgresql://",
        ),
        agent_redis_url=redis_url,
        agent_command_stream=command_stream,
        agent_command_consumer_group=f"test-worker-group-{suffix}",
        executor_event_stream=executor_stream,
        executor_event_consumer_group=f"test-executor-group-{suffix}",
        worker_command_concurrency=2,
        checkpoint_pool_max_size=2,
        outbox_poll_milliseconds=50,
        worker_metrics_enabled=False,
    )
    worker = WorkflowWorker(settings)
    run_task = asyncio.create_task(worker.run())
    try:
        await asyncio.sleep(1)
        if run_task.done():
            await run_task
        assert len(worker._graphs) == 2
    finally:
        run_task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await run_task
        redis = Redis.from_url(redis_url, decode_responses=True)
        await redis.delete(command_stream, executor_stream)
        await redis.aclose()
