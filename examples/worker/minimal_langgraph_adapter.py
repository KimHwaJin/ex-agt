"""Validated Worker event to LangGraph interrupt adapter.

Copy this file into the recipient Agent package and adapt only the state and
interrupt field names when its graph contract differs. The graph must use
``session_id`` as its ``thread_id`` and be compiled with a persistent
checkpointer.

This intentionally omits receipt-based duplicate recovery. Graph-side
effects must therefore be idempotent by ``command_id``.
"""

from __future__ import annotations

from typing import Any

from langgraph.types import Command

from worker import DeferEvent, EventContext, IgnoreEvent, RejectEvent


class MinimalLangGraphAdapter:
    """Resume only the matching Executor-event interrupt."""

    def __init__(self, graph: Any) -> None:
        self.graph = graph

    async def __call__(self, context: EventContext) -> None:
        config = context.graph_config
        snapshot = await self.graph.aget_state(config)
        values = snapshot.values

        if not values:
            raise DeferEvent("API has not checkpointed the execution yet")
        if not snapshot.next:
            raise IgnoreEvent("Execution graph already ended")

        interrupted = [
            interrupt
            for task in snapshot.tasks
            for interrupt in task.interrupts
        ]
        if len(interrupted) != 1:
            raise DeferEvent("Expected exactly one Executor wait")

        boundary = interrupted[0]
        value = boundary.value
        if not isinstance(value, dict) or value.get("kind") != (
            "EXECUTOR_EVENT"
        ):
            raise DeferEvent("Checkpoint is waiting for different input")

        execution_id = str(context.execution_id)
        if (
            values.get("task_id") != context.task_id
            or values.get("execution_id") != execution_id
            or value.get("task_id") != context.task_id
            or value.get("execution_id") != execution_id
        ):
            raise RejectEvent("Interrupt binding mismatch")

        action = {
            "command_id": str(context.command_id),
            "task_id": context.task_id,
            "event": context.event.model_dump(mode="json"),
        }
        await self.graph.ainvoke(
            Command(resume={boundary.id: action}),
            config,
            durability="sync",
        )


__all__ = ["MinimalLangGraphAdapter"]
