"""Executor event registry for the shared Agent runtime."""

from __future__ import annotations

from typing import Any

from worker import EventContext
from worker.contracts import EventHandler


async def create_graph(checkpointer: Any, bindings: Any) -> Any:
    """Deprecated hook retained for source-compatible handoff packages."""

    raise NotImplementedError(
        "Use agent.runtime.open_agent_runtime; Connect your Agent through "
        "the shared runtime factory"
    )


def build_handlers(resume_graph: EventHandler) -> dict[str, EventHandler]:
    """Register only events the shared graph currently waits for.

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
    """Optional progress projection; register only after implementation.

    Available: context.session_id/task_id/execution_id/command_id and
    context.event.payload. Make writes idempotent by command_id + operation.
    Returning means DONE, so never leave a registered handler as pass/log.
    Dispatcher already holds the session guard; do not lock again here.
    """
    raise NotImplementedError(
        "Implement worker_hooks.on_step_completed before registering it"
    )
