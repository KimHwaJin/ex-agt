"""Optional reference contract. Core runtime never imports LangGraph."""

from __future__ import annotations

from typing import Any

from langgraph.types import Command

from executor_worker.contracts import (
    DeferEvent,
    EventContext,
    IgnoreEvent,
    RejectEvent,
)


class SessionGraphAdapter:
    """Invoke while Dispatcher/host holds the shared SessionGuard.

    Required state and wait-node contract are demonstrated in examples.
    This is not a universal adapter for arbitrary Agent state schemas.
    """

    def __init__(self, graph: Any) -> None:
        self.graph = graph

    async def __call__(self, context: EventContext) -> None:
        config = context.graph_config
        snapshot = await self.graph.aget_state(config)
        values = snapshot.values
        command_id = str(context.command_id)
        event = context.event.model_dump(mode="json")
        pending = values.get("ew_pending", {})
        receipts = values.get("ew_receipts", {})
        interrupted = [i for t in snapshot.tasks for i in t.interrupts]
        receipt = receipts.get(command_id)
        if receipt is not None:
            if receipt != str(context.event.event_id):
                raise RejectEvent("Command receipt identity mismatch")
            if (
                pending.get("command_id") == command_id
                and snapshot.next
                and not interrupted
                and values.get("active_task_id") == context.task_id
                and values.get("execution_id") == str(context.execution_id)
            ):
                await self.graph.ainvoke(None, config, durability="sync")
            return
        if not values:
            raise DeferEvent("API has not checkpointed the execution yet")
        if values.get("active_task_id") != context.task_id or values.get(
            "execution_id"
        ) != str(context.execution_id):
            if str(context.execution_id) in values.get("ew_sequences", {}):
                raise IgnoreEvent("Event belongs to an inactive execution")
            raise DeferEvent("API has not checkpointed this binding yet")
        action = {
            "command_id": command_id,
            "task_id": context.task_id,
            "event": event,
        }
        if snapshot.next and not interrupted:
            if pending != action:
                raise DeferEvent("Another invocation requires recovery")
            # Wait node already consumed the resume. Continue pending nodes,
            # do not inject another resume into a later approval/wait.
            await self.graph.ainvoke(None, config, durability="sync")
        elif len(interrupted) == 1:
            boundary = interrupted[0]
            value = boundary.value
            if not isinstance(value, dict) or (
                value.get("kind") != "EXECUTOR_EVENT"
            ):
                raise DeferEvent("Checkpoint is waiting for user input")
            if (
                value.get("execution_id") != str(context.execution_id)
                or value.get("task_id") != context.task_id
            ):
                raise RejectEvent("Interrupt binding mismatch")
            last = values.get("ew_sequences", {}).get(
                str(context.execution_id),
                0,
            )
            if context.event.event_sequence <= last:
                raise IgnoreEvent("Older execution sequence already applied")
            await self.graph.ainvoke(
                Command(resume={boundary.id: action}),
                config,
                durability="sync",
            )
        elif not snapshot.next:
            raise IgnoreEvent("Execution graph already ended")
        else:
            raise DeferEvent("Expected exactly one Executor wait")
        after = await self.graph.aget_state(config)
        if after.values.get("ew_receipts", {}).get(command_id) != (
            str(context.event.event_id)
        ):
            raise DeferEvent("Handler has not recorded its receipt yet")
