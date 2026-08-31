from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid4

import httpx
import pytest
from fastapi import FastAPI, HTTPException
from langgraph.checkpoint.memory import InMemorySaver

from ex_agent.transport.consumer import (
    AckDecision,
    PermanentMessageError,
    StreamMessage,
)
from examples.api_agent_worker.api import create_router
from examples.api_agent_worker.runner import SharedGraphRunner
from examples.api_agent_worker.testing import (
    FakeExecutor,
    MemoryCommands,
    MemoryRunGuard,
)
from examples.api_agent_worker.worker import ExecutorResumeHandler
from examples.api_agent_worker.workflow import build_graph
from examples.durable_event_to_langgraph.contracts import (
    CommandState,
    WorkflowCommand,
)


@dataclass
class Rig:
    task_id: UUID
    saver: Any
    api_runner: SharedGraphRunner
    worker_runner: SharedGraphRunner
    guard: MemoryRunGuard
    executor: FakeExecutor
    commands: MemoryCommands
    app: FastAPI

    def handler(self) -> ExecutorResumeHandler:
        return ExecutorResumeHandler(
            self.commands, self.worker_runner, self.guard
        )

    def client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(
            transport=httpx.ASGITransport(app=self.app),
            base_url="http://handoff",
            headers={"X-User-ID": "owner"},
        )

    async def start(self) -> None:
        async with self.client() as client:
            response = await client.post(
                f"/handoff/tasks/{self.task_id}/start",
                json={"request_id": str(uuid4()), "objective": "Analyze"},
            )
        assert response.status_code == 200
        assert response.json()["phase"] == "PLAN_REVIEW"

    async def approve(self, request_id: UUID | None = None) -> httpx.Response:
        async with self.client() as client:
            return await client.post(
                f"/handoff/tasks/{self.task_id}/review",
                json={
                    "request_id": str(request_id or uuid4()),
                    "approved": True,
                },
            )

    def command(self, sequence: int = 5) -> StreamMessage:
        execution_id = next(iter(self.executor.submissions.values()), uuid4())
        command = WorkflowCommand(
            command_id=uuid4(),
            workflow_id=str(self.task_id),
            command_type="EXECUTOR_SIGNAL",
            payload={
                "type": "EXECUTOR_BOUNDARY",
                "execution_id": str(execution_id),
                "event_id": str(uuid4()),
                "event_sequence": sequence,
                "event_type": "execution.operation_completed",
            },
        )
        self.commands.add(command)
        return StreamMessage(
            f"{sequence}-0",
            {
                "command_id": str(command.command_id),
                "task_id": str(self.task_id),
            },
        )


@pytest.fixture
def rig() -> Rig:
    saver = InMemorySaver()
    executor = FakeExecutor()
    api_runner = SharedGraphRunner(build_graph(saver, executor))
    worker_runner = SharedGraphRunner(build_graph(saver, executor))
    guard = MemoryRunGuard()
    task_id = uuid4()

    async def admission(
        requested_task: UUID, user_id: str, operation: str, body: dict
    ) -> None:
        # Only authorization is modeled here, not the host's session DB.
        if requested_task != task_id or user_id != "owner":
            raise HTTPException(404, "Task not found")

    app = FastAPI()
    app.include_router(create_router(api_runner, guard, admission))
    return Rig(
        task_id,
        saver,
        api_runner,
        worker_runner,
        guard,
        executor,
        MemoryCommands(),
        app,
    )


async def test_api_invokes_and_worker_resumes_separate_graph(rig: Rig) -> None:
    await rig.start()
    assert not rig.commands._commands
    response = await rig.approve()
    assert response.status_code == 200
    assert response.json()["interrupts"][0]["kind"] == "EXECUTOR_EVENT"
    assert rig.api_runner.graph is not rig.worker_runner.graph
    rig.executor.status = "SUCCEEDED"
    result = await rig.handler().handle(rig.command())
    assert result.decision is AckDecision.ACK
    assert (await rig.api_runner.view(rig.task_id))["phase"] == "SUCCEEDED"


async def test_event_cannot_resume_user_approval(rig: Rig) -> None:
    await rig.start()
    result = await rig.handler().handle(rig.command())
    assert result.decision is AckDecision.RETRY
    assert not rig.executor.submissions
    assert (await rig.api_runner.view(rig.task_id))["phase"] == "PLAN_REVIEW"


async def test_early_event_waits_for_api_checkpoint(rig: Rig) -> None:
    await rig.start()
    rig.executor.submit_release.clear()
    approval = asyncio.create_task(rig.approve())
    try:
        await asyncio.wait_for(rig.executor.submit_entered.wait(), 2)
        message = rig.command()
        first = await rig.handler().handle(message)
        assert first.decision is AckDecision.RETRY
        assert first.outcome == "not_ready"
    finally:
        rig.executor.submit_release.set()
        response = await approval
    assert response.status_code == 200
    assert (await rig.handler().handle(message)).decision is AckDecision.ACK


async def test_user_cannot_resume_executor_wait(rig: Rig) -> None:
    await rig.start()
    assert (await rig.approve()).status_code == 200
    assert (await rig.approve()).status_code == 409
    assert len(rig.executor.submissions) == 1


async def test_same_approval_request_replays_without_submission(rig: Rig):
    await rig.start()
    request_id = uuid4()
    assert (await rig.approve(request_id)).status_code == 200
    assert (await rig.approve(request_id)).status_code == 200
    assert len(rig.executor.submissions) == 1
    async with rig.client() as client:
        conflict = await client.post(
            f"/handoff/tasks/{rig.task_id}/review",
            json={"request_id": str(request_id), "approved": False},
        )
    assert conflict.status_code == 409


async def test_duplicate_and_late_old_command_are_not_reapplied(rig: Rig):
    await rig.start()
    await rig.approve()
    old = rig.command(5)
    handler = rig.handler()
    assert (await handler.handle(old)).outcome == "applied"
    assert (await handler.handle(old)).outcome == "duplicate"
    newer = rig.command(10)
    assert (await handler.handle(newer)).outcome == "applied"
    # Simulate loss of DB completion but keep both checkpoint receipts.
    await rig.commands.mark_retry(UUID(old.fields["command_id"]), "crash")
    assert (await handler.handle(old)).outcome == "already_checkpointed"
    assert rig.executor.reconciliations == 2


async def test_later_boundary_does_not_reopen_terminal_graph(rig: Rig):
    await rig.start()
    await rig.approve()
    rig.executor.status = "SUCCEEDED"
    handler = rig.handler()
    assert (await handler.handle(rig.command(5))).outcome == "applied"
    completed = rig.command(6)
    command = rig.commands._commands[UUID(completed.fields["command_id"])]
    command.payload["event_type"] = "execution.completed"
    assert (await handler.handle(completed)).outcome == "applied"
    assert (await handler.handle(completed)).outcome == "duplicate"
    snapshot = await rig.api_runner.graph.aget_state(
        rig.api_runner.config(rig.task_id)
    )
    assert not snapshot.next
    assert snapshot.values["phase"] == "SUCCEEDED"
    assert snapshot.values["last_event_sequence"] == 6
    assert rig.executor.reconciliations == 1


async def test_node_failure_continues_checkpoint_without_second_resume(
    rig: Rig,
) -> None:
    await rig.start()
    await rig.approve()
    message = rig.command()
    rig.executor.reconcile_failures = 1
    handler = rig.handler()
    assert (await handler.handle(message)).decision is AckDecision.RETRY
    snapshot = await rig.worker_runner.graph.aget_state(
        rig.worker_runner.config(rig.task_id)
    )
    assert snapshot.next == ("apply_event",)
    # Replace the worker graph instance to model an adapter restart.
    rig.worker_runner = SharedGraphRunner(build_graph(rig.saver, rig.executor))
    assert (await rig.handler().handle(message)).outcome == "applied"
    assert rig.executor.reconciliations == 1


async def test_checkpoint_after_resume_recovers_missing_done(
    rig: Rig, monkeypatch: pytest.MonkeyPatch
):
    await rig.start()
    await rig.approve()
    message = rig.command()
    command_id = UUID(message.fields["command_id"])

    async def fail_done(_: UUID) -> None:
        raise ConnectionError("DB completion failed")

    with monkeypatch.context() as patch:
        patch.setattr(rig.commands, "mark_done", fail_done)
        with pytest.raises(ConnectionError, match="DB completion"):
            await rig.handler().handle(message)
    assert (
        await rig.handler().handle(message)
    ).outcome == "already_checkpointed"
    saved = await rig.commands.get_command(command_id)
    assert saved is not None and saved.state is CommandState.DONE
    assert rig.executor.reconciliations == 1


async def test_api_start_does_not_continue_worker_pending_node(rig: Rig):
    await rig.start()
    await rig.approve()
    message = rig.command()
    rig.executor.reconcile_failures = 1
    await rig.handler().handle(message)
    async with rig.client() as client:
        replay = await client.post(
            f"/handoff/tasks/{rig.task_id}/start",
            json={"request_id": str(uuid4()), "objective": "Analyze"},
        )
    assert replay.status_code == 200
    assert rig.executor.reconciliations == 0
    assert (await rig.handler().handle(message)).outcome == "applied"


async def test_worker_rejects_mismatched_execution(rig: Rig) -> None:
    await rig.start()
    await rig.approve()
    message = rig.command()
    command = await rig.commands.get_command(
        UUID(message.fields["command_id"])
    )
    assert command is not None
    command.payload["execution_id"] = str(uuid4())
    with pytest.raises(PermanentMessageError, match="binding mismatch"):
        await rig.handler().handle(message)


@pytest.mark.parametrize("command_type", ["START", "RESUME"])
async def test_worker_rejects_api_commands(rig: Rig, command_type: str):
    command = WorkflowCommand(uuid4(), str(rig.task_id), command_type, {})
    rig.commands.add(command)
    with pytest.raises(PermanentMessageError, match="only accepts"):
        await rig.handler().handle(
            StreamMessage(
                "1-0",
                {
                    "task_id": str(rig.task_id),
                    "command_id": str(command.command_id),
                },
            )
        )


async def test_api_requires_authorization_before_invoke(rig: Rig) -> None:
    async with rig.client() as client:
        response = await client.post(
            f"/handoff/tasks/{rig.task_id}/start",
            headers={"X-User-ID": "other-user"},
            json={"request_id": str(uuid4()), "objective": "Analyze"},
        )
    assert response.status_code == 404
    assert (await rig.api_runner.view(rig.task_id))["phase"] == "NOT_STARTED"


async def test_store_defers_later_command_until_previous_done(rig: Rig):
    await rig.start()
    await rig.approve()
    first = rig.command(5)
    second = rig.command(10)
    handler = rig.handler()
    assert (await handler.handle(second)).decision is AckDecision.RETRY
    assert rig.executor.reconciliations == 0
    assert (await handler.handle(first)).outcome == "applied"
    assert (await handler.handle(second)).outcome == "applied"
