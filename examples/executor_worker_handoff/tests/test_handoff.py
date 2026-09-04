from __future__ import annotations

import ast
from pathlib import Path
from uuid import uuid4

import pytest
from langchain_core.messages import HumanMessage
from langgraph.checkpoint.memory import InMemorySaver

import agent
import worker
from agent_worker.graph_boundary import (
    ExecutorBoundaryNodes,
    ExecutorBoundaryState,
    receipt_update,
)
from agent_worker.langgraph_adapter import LangGraphEventAdapter
from agent_worker.worker_hooks import build_handlers
from worker import EventContext, ExecutorEvent

PACKAGE_ROOT = Path(__file__).parents[1]


def event() -> ExecutorEvent:
    return ExecutorEvent.model_validate(
        {
            "event_id": str(uuid4()),
            "execution_id": str(uuid4()),
            "event_type": "execution.completed",
            "event_sequence": 1,
            "schema_version": "1.0",
            "occurred_at": "2026-09-03T00:00:00Z",
            "payload": {},
        }
    )


def test_imports_the_handoff_worker_copy() -> None:
    # Test the installed distribution, not a PYTHONPATH/source mount.
    assert "site-packages" in Path(worker.__file__).parts
    assert Path(worker.__file__).parent.parent == (
        Path(agent.__file__).parent.parent
    )


def test_package_has_no_source_service_imports() -> None:
    for source in (PACKAGE_ROOT / "src").rglob("*.py"):
        tree = ast.parse(source.read_text())
        imported: list[str] = []
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and node.module:
                imported.append(node.module)
            elif isinstance(node, ast.Import):
                imported.extend(alias.name for alias in node.names)
        assert not any(
            name == forbidden or name.startswith(f"{forbidden}.")
            for name in imported
            for forbidden in ("ex_agent", "agent.runtime")
        )


def test_hooks_use_original_executor_event_types() -> None:
    async def resume(context: EventContext) -> None:
        del context

    handlers = build_handlers(resume)
    assert handlers == {
        "execution.operation_completed": resume,
        "execution.completed": resume,
    }


@pytest.mark.asyncio
async def test_register_node_uses_thread_id_as_session_id() -> None:
    calls = []

    class Bindings:
        async def register(self, **kwargs) -> None:
            calls.append(kwargs)

    execution_id = uuid4()
    nodes = ExecutorBoundaryNodes(Bindings())
    await nodes.register_execution(
        {"task_id": "task-1", "execution_id": str(execution_id)},
        {"configurable": {"thread_id": "session-1"}},
    )
    assert calls == [
        {
            "execution_id": execution_id,
            "session_id": "session-1",
            "task_id": "task-1",
        }
    ]


def test_receipt_update_is_stable() -> None:
    source_event = event()
    command_id = uuid4()
    state: ExecutorBoundaryState = {
        "task_id": "task-1",
        "execution_id": str(source_event.execution_id),
        "ew_pending": {
            "command_id": str(command_id),
            "task_id": "task-1",
            "execution_id": str(source_event.execution_id),
            "event": source_event.model_dump(mode="json"),
        },
        "ew_receipts": {},
        "ew_sequences": {},
    }
    update = receipt_update(state)
    assert update == {
        "ew_receipts": {str(command_id): str(source_event.event_id)},
        "ew_sequences": {str(source_event.execution_id): 1},
    }


def test_worker_settings_do_not_require_source_service_config(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setenv("EW_DATABASE_URL", "postgresql://db/agent")
    monkeypatch.setenv("EW_REDIS_URL", "redis://redis/0")
    settings = worker.Settings()
    assert settings.database_url == "postgresql://db/agent"
    assert settings.redis_url == "redis://redis/0"


def test_event_context_maps_session_to_graph_thread() -> None:
    source_event = event()
    context = EventContext(
        namespace="agent",
        session_id="session-1",
        task_id="task-1",
        execution_id=source_event.execution_id,
        command_id=uuid4(),
        event=source_event,
    )
    assert context.graph_config == {"configurable": {"thread_id": "session-1"}}


@pytest.mark.asyncio
async def test_sample_graph_waits_and_worker_resumes_it() -> None:
    registrations = []

    class Bindings:
        async def register(self, **kwargs) -> None:
            registrations.append(kwargs)

    graph = agent.build_graph(
        bindings=Bindings(),
        checkpointer=InMemorySaver(),
    )
    config = {"configurable": {"thread_id": "sample-session"}}
    await graph.ainvoke(
        {"messages": [HumanMessage(content="샘플 실행")]},
        config,
    )
    waiting = await graph.aget_state(config)
    assert waiting.next == ("wait_executor_event",)
    assert len(registrations) == 1

    source_event = ExecutorEvent.model_validate(
        {
            "event_id": str(uuid4()),
            "execution_id": waiting.values["execution_id"],
            "event_type": "execution.completed",
            "event_sequence": 1,
            "schema_version": "1.0",
            "occurred_at": "2026-09-03T00:00:00Z",
            "payload": {"status": "SUCCEEDED"},
        }
    )
    context = EventContext(
        namespace="sample",
        session_id="sample-session",
        task_id=waiting.values["task_id"],
        execution_id=source_event.execution_id,
        command_id=uuid4(),
        event=source_event,
    )
    adapter = LangGraphEventAdapter(graph)
    await adapter(context)
    await adapter(context)

    completed = await graph.aget_state(config)
    assert not completed.next
    assert completed.values["received_events"] == [
        source_event.model_dump(mode="json")
    ]
    assert completed.values["ew_receipts"][str(context.command_id)] == str(
        source_event.event_id
    )
