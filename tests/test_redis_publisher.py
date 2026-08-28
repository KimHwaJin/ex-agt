import os
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from ex_agent.config import Settings
from ex_agent.persistence.database import (
    create_engine,
    create_session_factory,
)
from ex_agent.persistence.repository import AgentRepository
from ex_agent.transport.streams import CommandPublisher

pytestmark = pytest.mark.skipif(
    not {"TEST_DATABASE_URL", "TEST_REDIS_URL"}.issubset(os.environ),
    reason="Compose database and Redis are not configured",
)


@pytest.mark.redis
@pytest.mark.postgres
@pytest.mark.asyncio
async def test_pending_command_is_published_once() -> None:
    database_url = os.environ["TEST_DATABASE_URL"]
    redis_url = os.environ["TEST_REDIS_URL"]
    engine = create_engine(database_url)
    repository = AgentRepository(create_session_factory(engine))
    redis = Redis.from_url(redis_url, decode_responses=True)
    stream = f"test-agent-commands-{uuid4()}"
    task_id = uuid4()
    settings = Settings(
        agent_database_url=database_url,
        agent_redis_url=redis_url,
        agent_command_stream=stream,
    )
    try:
        await repository.create_task(
            task_id=task_id,
            input_message_id=uuid4(),
            user_id="integration-user",
            project_id="integration-project",
            session_id=f"session-{task_id}",
            content="분석해줘",
            idempotency_key=f"create-{task_id}",
        )
        publisher = CommandPublisher(settings, repository, redis)
        assert await publisher.publish_pending() >= 1
        assert await publisher.publish_pending() == 0
        entries = await redis.xrange(stream)
        assert str(task_id) in {fields["task_id"] for _, fields in entries}
    finally:
        await redis.delete(stream)
        await redis.aclose()
        await engine.dispose()
