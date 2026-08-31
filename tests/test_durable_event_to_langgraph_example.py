from __future__ import annotations

import json
from uuid import UUID, uuid4

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from ex_agent.transport.consumer import (
    AckDecision,
    PermanentMessageError,
    StreamMessage,
)
from examples.durable_event_to_langgraph.contracts import CommandState
from examples.durable_event_to_langgraph.handlers import (
    DurableCommandHandler,
    ExternalEventHandler,
)
from examples.durable_event_to_langgraph.memory_store import (
    InMemoryDurableStore,
)
from examples.durable_event_to_langgraph.workflow import (
    LangGraphWorkflowRunner,
    build_workflow,
)


def _event_message(
    workflow_id: str,
    sequence: int,
    event_type: str,
) -> StreamMessage:
    return StreamMessage(
        message_id=f"{sequence}-0",
        fields={
            "event_id": str(uuid4()),
            "workflow_id": workflow_id,
            "sequence": str(sequence),
            "event_type": event_type,
            "payload": json.dumps({"result": "ok"}),
        },
    )


@pytest.fixture
def runtime() -> tuple[
    InMemoryDurableStore,
    LangGraphWorkflowRunner,
]:
    store = InMemoryDurableStore()
    runner = LangGraphWorkflowRunner(build_workflow(InMemorySaver()))
    return store, runner


async def test_event_becomes_command_and_resumes_graph(
    runtime: tuple[InMemoryDurableStore, LangGraphWorkflowRunner],
) -> None:
    store, runner = runtime
    workflow_id = "workflow-1"
    await runner.start(workflow_id, "analyze data")
    event_handler = ExternalEventHandler(store)
    event_message = _event_message(
        workflow_id,
        1,
        "workflow.step_completed",
    )

    accepted = await event_handler.handle(event_message)
    duplicate = await event_handler.handle(event_message)

    assert accepted.outcome == "accepted"
    assert duplicate.outcome == "duplicate"
    assert len(store.outbox) == 1

    command_handler = DurableCommandHandler(store, runner)
    result = await command_handler.handle(
        StreamMessage("2-0", store.outbox[0])
    )

    command_id = store.outbox[0]["command_id"]
    command = await store.get_command(UUID(command_id))
    snapshot = await runner.state(workflow_id)
    assert result.decision is AckDecision.ACK
    assert result.outcome == "applied"
    assert command is not None
    assert command.state is CommandState.DONE
    assert snapshot["last_command_id"] == command_id
    assert snapshot["applied_count"] == 1


async def test_checkpoint_closes_crash_before_command_done(
    runtime: tuple[InMemoryDurableStore, LangGraphWorkflowRunner],
) -> None:
    store, runner = runtime
    workflow_id = "workflow-crash-window"
    await runner.start(workflow_id, "analyze data")
    event_handler = ExternalEventHandler(store)
    await event_handler.handle(
        _event_message(workflow_id, 1, "workflow.step_completed")
    )
    command_fields = store.outbox[0]
    command_id = UUID(command_fields["command_id"])
    command = await store.get_command(command_id)
    assert command is not None

    applied = await runner.resume(command)
    recovered = await DurableCommandHandler(store, runner).handle(
        StreamMessage("2-0", command_fields)
    )

    snapshot = await runner.state(workflow_id)
    saved = await store.get_command(command_id)
    assert applied is True
    assert recovered.outcome == "already_checkpointed"
    assert snapshot["applied_count"] == 1
    assert saved is not None
    assert saved.state is CommandState.DONE


async def test_non_boundary_event_advances_sequence_without_command(
    runtime: tuple[InMemoryDurableStore, LangGraphWorkflowRunner],
) -> None:
    store, _ = runtime
    result = await ExternalEventHandler(store).handle(
        _event_message("workflow-progress", 1, "workflow.progress")
    )

    assert result.decision is AckDecision.ACK
    assert store.outbox == []


async def test_sequence_gap_is_retryable(
    runtime: tuple[InMemoryDurableStore, LangGraphWorkflowRunner],
) -> None:
    store, _ = runtime
    with pytest.raises(RuntimeError, match="sequence gap"):
        await ExternalEventHandler(store).handle(
            _event_message("workflow-gap", 2, "workflow.completed")
        )


def test_malformed_event_is_permanent() -> None:
    handler = ExternalEventHandler(InMemoryDurableStore())
    message = StreamMessage("1-0", {"event_id": "not-a-uuid"})

    with pytest.raises(PermanentMessageError):
        handler.lock_key(message)
