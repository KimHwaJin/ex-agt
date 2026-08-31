import os
from uuid import uuid4

import pytest
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from ex_agent.transport.consumer import AckDecision, StreamMessage
from examples.api_agent_worker.runner import SharedGraphRunner
from examples.api_agent_worker.testing import (
    FakeExecutor,
    MemoryCommands,
    MemoryRunGuard,
)
from examples.api_agent_worker.worker import ExecutorResumeHandler
from examples.api_agent_worker.workflow import build_graph
from examples.durable_event_to_langgraph.contracts import WorkflowCommand


@pytest.mark.postgres
@pytest.mark.skipif(
    "TEST_DATABASE_URL" not in os.environ,
    reason="Requires the isolated Compose PostgreSQL test database",
)
async def test_worker_restores_checkpoint_after_api_connection_closes():
    url = os.environ["TEST_DATABASE_URL"].replace(
        "postgresql+psycopg://", "postgresql://"
    )
    task_id = uuid4()
    executor = FakeExecutor()
    guard = MemoryRunGuard()
    store = MemoryCommands()
    async with AsyncPostgresSaver.from_conn_string(url) as api_saver:
        await api_saver.setup()
        api_runner = SharedGraphRunner(build_graph(api_saver, executor))
        async with guard.hold(task_id):
            await api_runner.start(task_id, "Postgres handoff")
            await api_runner.review(task_id, uuid4(), True)
        initial = await api_runner.view(task_id)
        assert initial["phase"] == "WAITING"

    # New saver/connection/graph; no in-memory checkpoint object is shared.
    # Business store/gate/Executor remain fakes, not cross-process adapters.
    async with AsyncPostgresSaver.from_conn_string(url) as worker_saver:
        worker_runner = SharedGraphRunner(build_graph(worker_saver, executor))
        command = WorkflowCommand(
            command_id=uuid4(),
            workflow_id=str(task_id),
            command_type="EXECUTOR_SIGNAL",
            payload={
                "type": "EXECUTOR_BOUNDARY",
                "execution_id": initial["execution_id"],
                "event_id": str(uuid4()),
                "event_sequence": 5,
                "event_type": "execution.completed",
            },
        )
        store.add(command)
        executor.status = "SUCCEEDED"
        handler = ExecutorResumeHandler(store, worker_runner, guard)
        result = await handler.handle(
            StreamMessage(
                "1-0",
                {
                    "task_id": str(task_id),
                    "command_id": str(command.command_id),
                },
            )
        )
        assert result.decision is AckDecision.ACK
        assert (await worker_runner.view(task_id))["phase"] == "SUCCEEDED"

    async with AsyncPostgresSaver.from_conn_string(url) as restarted_saver:
        reader = SharedGraphRunner(build_graph(restarted_saver, executor))
        assert (await reader.view(task_id))["phase"] == "SUCCEEDED"
