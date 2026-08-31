"""One-process smoke demo; no Redis, LLM, Executor, or server is started."""

import asyncio
from uuid import UUID, uuid4

import httpx
from fastapi import FastAPI, HTTPException
from langgraph.checkpoint.memory import InMemorySaver

from ex_agent.transport.consumer import StreamMessage
from examples.api_agent_worker.api import create_router
from examples.api_agent_worker.runner import SharedGraphRunner
from examples.api_agent_worker.testing import (
    FakeExecutor,
    MemoryCommands,
    MemoryRunGuard,
)
from examples.api_agent_worker.worker import ExecutorResumeHandler
from examples.api_agent_worker.workflow import build_graph
from examples.durable_event_to_langgraph.contracts import WorkflowCommand


async def main() -> None:
    task_id = uuid4()
    saver = InMemorySaver()
    executor = FakeExecutor()
    guard = MemoryRunGuard()
    store = MemoryCommands()
    api_runner = SharedGraphRunner(build_graph(saver, executor))
    worker_runner = SharedGraphRunner(build_graph(saver, executor))

    async def admit(
        requested: UUID, user_id: str, operation: str, body: dict
    ) -> None:
        if requested != task_id or user_id != "demo-user":
            raise HTTPException(404, "Task not found")

    app = FastAPI()
    app.include_router(create_router(api_runner, guard, admit))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://demo",
        headers={"X-User-ID": "demo-user"},
    ) as client:
        started = await client.post(
            f"/handoff/tasks/{task_id}/start",
            json={"request_id": str(uuid4()), "objective": "Demo analysis"},
        )
        started.raise_for_status()
        print("API:", started.json()["phase"])
        approved = await client.post(
            f"/handoff/tasks/{task_id}/review",
            json={"request_id": str(uuid4()), "approved": True},
        )
        approved.raise_for_status()
        print("API:", approved.json()["phase"])

    command = WorkflowCommand(
        command_id=uuid4(),
        workflow_id=str(task_id),
        command_type="EXECUTOR_SIGNAL",
        payload={
            "type": "EXECUTOR_BOUNDARY",
            "execution_id": approved.json()["execution_id"],
            "event_id": str(uuid4()),
            "event_sequence": 5,
            "event_type": "execution.completed",
        },
    )
    store.add(command)
    executor.status = "SUCCEEDED"
    handler = ExecutorResumeHandler(store, worker_runner, guard)
    message = StreamMessage(
        "1-0", {"task_id": str(task_id), "command_id": str(command.command_id)}
    )
    print("Worker:", (await handler.handle(message)).outcome)
    print("Replay:", (await handler.handle(message)).outcome)
    print("Final:", (await api_runner.view(task_id))["phase"])


if __name__ == "__main__":
    asyncio.run(main())
