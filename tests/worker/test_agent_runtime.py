from pathlib import Path

import pytest
from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from agent.graph import checkpoint_serializer
from agent.runtime.factory import open_agent_runtime
from ex_agent.config import Settings
from ex_agent.llm.factory import DeterministicHashEmbeddings


@pytest.mark.postgres
@pytest.mark.redis
async def test_runtime_factory_builds_shared_production_graph(worker):
    checkpoint_url = worker.settings.database_url
    settings = Settings(
        agent_database_url=checkpoint_url.replace(
            "postgresql://", "postgresql+psycopg://"
        ),
        agent_checkpoint_database_url=checkpoint_url,
        agent_redis_url=worker.settings.redis_url,
        agent_skill_root=Path("skills"),
        worker_metrics_enabled=False,
    )
    async with AsyncPostgresSaver.from_conn_string(
        checkpoint_url,
        serde=checkpoint_serializer(),
    ) as saver:
        # Deployment responsibility; the runtime itself never calls setup.
        await saver.setup()
        async with open_agent_runtime(
            settings,
            worker,
            saver,
            model=FakeListChatModel(responses=["unused"]),
            embeddings=DeterministicHashEmbeddings(32),
        ) as runtime:
            assert runtime.graph.checkpointer is saver
            assert runtime.admission.graph is runtime.graph
            assert runtime.lifecycle.failure_recovery.worker_store is (
                worker.store
            )
            assert callable(runtime.event_handler)
            assert not runtime.lifecycle.running
