"""Convert a durable Worker EventContext into a LangGraph resume."""

from __future__ import annotations

from typing import Any

from langgraph.types import Command

from worker import DeferEvent, EventContext, IgnoreEvent, RejectEvent


class LangGraphEventAdapter:
    """Resume only the Executor interrupt bound to this event."""

    def __init__(self, graph: Any) -> None:
        self.graph = graph

    async def __call__(self, context: EventContext) -> None:
        config = context.graph_config
        snapshot = await self.graph.aget_state(config)
        values = snapshot.values
        command_id = str(context.command_id)
        execution_id = str(context.execution_id)
        event_id = str(context.event.event_id)
        pending = values.get("ew_pending", {})
        receipts = values.get("ew_receipts", {})
        interrupts = [
            item for task in snapshot.tasks for item in task.interrupts
        ]

        receipt = receipts.get(command_id)
        if receipt is not None:
            if receipt != event_id:
                raise RejectEvent("Command receipt identity mismatch")
            if (
                snapshot.next
                and not interrupts
                and pending.get("command_id") == command_id
                and values.get("task_id") == context.task_id
                and values.get("execution_id") == execution_id
            ):
                await self.graph.ainvoke(
                    None,
                    config,
                    durability="sync",
                )
            return

        if not values:
            raise DeferEvent("Agent has not checkpointed the execution yet")
        if (
            values.get("task_id") != context.task_id
            or values.get("execution_id") != execution_id
        ):
            if execution_id in values.get("ew_sequences", {}):
                raise IgnoreEvent("Event belongs to an inactive execution")
            raise DeferEvent("Agent has not checkpointed this binding yet")

        action = {
            "command_id": command_id,
            "task_id": context.task_id,
            "execution_id": execution_id,
            "event": context.event.model_dump(mode="json"),
        }
        if snapshot.next and not interrupts:
            if pending != action:
                raise DeferEvent("Another graph invocation needs recovery")
            await self.graph.ainvoke(None, config, durability="sync")
        elif len(interrupts) == 1:
            boundary = interrupts[0]
            value = boundary.value
            if not isinstance(value, dict) or value.get("kind") != (
                "EXECUTOR_EVENT"
            ):
                raise DeferEvent("Graph is waiting for non-Executor input")
            if (
                value.get("task_id") != context.task_id
                or value.get("execution_id") != execution_id
            ):
                raise RejectEvent("Interrupt binding mismatch")
            last = values.get("ew_sequences", {}).get(execution_id, 0)
            if context.event.event_sequence <= last:
                raise IgnoreEvent("Older execution sequence already applied")
            await self.graph.ainvoke(
                Command(resume={boundary.id: action}),
                config,
                durability="sync",
            )
        elif not snapshot.next:
            raise IgnoreEvent("Agent graph already ended")
        else:
            raise DeferEvent("Expected exactly one Executor wait")

        after = await self.graph.aget_state(config)
        if after.values.get("ew_receipts", {}).get(command_id) != event_id:
            raise DeferEvent("Agent has not recorded the event receipt yet")
