"""Agent integration points. See docs/worker/agent-integration.md.

No demo graph is loaded implicitly. Implement create_graph before deploying.
The reusable consumer/Inbox/Outbox modules do not need application edits.
"""

from __future__ import annotations

from typing import Any

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from worker import EventContext
from worker.contracts import EventHandler
from worker.store import Store


async def create_graph(
    checkpointer: AsyncPostgresSaver,
    bindings: Store,
) -> Any:
    """TODO 1: import and build the SAME Agent graph used by your API.

    Replace the exception with your graph factory call. For example, adapt
    your own factory to accept checkpointer=checkpointer, bindings=bindings.
    Return a compiled graph, not the StateGraph builder or an invoke result.

    Compile with the supplied checkpointer. Inject bindings into Executor
    submission nodes so Worker-resumed nodes can register new executions.
    Never close these supplied resources or call checkpointer.setup here.

    The default adapter requires active_task_id, execution_id, ew_pending,
    ew_receipts, ew_sequences and an EXECUTOR_EVENT interrupt. If your State
    differs, adapt langgraph_adapter.py and your wait/apply nodes together.
    """
    raise NotImplementedError(
        "Connect your Agent in "
        "agent.integrations.worker_hooks.create_graph(); "
        "see docs/worker/agent-integration.md. No events have been consumed."
    )


def build_handlers(resume_graph: EventHandler) -> dict[str, EventHandler]:
    """TODO 2: choose the events your graph actually waits for.

    These two registrations follow the reference wait/apply contract.
    Unregistered event types are recorded IGNORED by the Router. Implement
    progress persistence first, then enable on_step_completed below.
    Keep this registry identical across replicas sharing the same group.
    """
    return {
        # "execution.step_completed": on_step_completed,
        "execution.operation_completed": resume_graph,
        "execution.completed": resume_graph,
    }


async def on_step_completed(context: EventContext) -> None:
    """TODO 3 (optional): persist/publish progress through your own service.

    Available: context.session_id/task_id/execution_id/command_id and
    context.event.payload. Make writes idempotent by command_id + operation.
    Returning means DONE, so never leave a registered handler as pass/log.
    Dispatcher already holds the session guard; do not lock again here.
    """
    raise NotImplementedError(
        "Implement worker_hooks.on_step_completed before registering it"
    )
