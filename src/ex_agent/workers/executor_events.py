from __future__ import annotations

from collections.abc import Awaitable, Callable
from uuid import UUID

from ex_agent.executor.client import ExecutorClient
from ex_agent.executor.contracts import ExecutorEvent
from ex_agent.persistence.repository import AgentRepository
from ex_agent.transport.streams import CommandPublisher

PersistEvent = Callable[[str, UUID, ExecutorEvent], Awaitable[None]]


class ExecutorEventProcessor:
    def __init__(
        self,
        repository: AgentRepository,
        executor: ExecutorClient | None,
        publisher: CommandPublisher | None,
    ) -> None:
        self._repository = repository
        self._executor = executor
        self._publisher = publisher

    async def process(
        self,
        stream: str,
        event: ExecutorEvent,
        *,
        catch_up: bool = False,
        persist_event: PersistEvent | None = None,
    ) -> bool:
        binding = await self._repository.binding_for_execution(
            event.execution_id
        )
        if binding is None:
            return False
        history: list[ExecutorEvent] = []
        if catch_up:
            if self._executor is None:
                raise RuntimeError("Executor client is not configured")
            history = await self._executor.events_after(
                event.execution_id,
                after_sequence=binding.last_event_sequence,
                limit=500,
            )
            event = max(
                [event, *history],
                key=lambda item: item.event_sequence,
            )
        elif event.event_sequence > binding.last_event_sequence + 1:
            if self._executor is None:
                raise RuntimeError("Executor client is not configured")
            history = await self._executor.events_after(
                event.execution_id,
                after_sequence=binding.last_event_sequence,
                limit=(event.event_sequence - binding.last_event_sequence),
            )
        ordered = merge_contiguous_events(
            event,
            history,
            after_sequence=binding.last_event_sequence,
        )
        persist = persist_event or self.persist
        for ordered_event in ordered:
            await persist(stream, binding.task_id, ordered_event)
        return True

    async def persist(
        self,
        stream: str,
        task_id: UUID,
        event: ExecutorEvent,
    ) -> None:
        dedupe_id = f"event:{event.event_id}"
        if event.event_type not in {
            "execution.operation_completed",
            "execution.completed",
        }:
            await self._repository.record_executor_progress(
                stream_name=stream,
                message_id=dedupe_id,
                task_id=task_id,
                event_type=event.event_type,
                event_sequence=event.event_sequence,
                payload={
                    "execution_id": str(event.execution_id),
                    "event_id": str(event.event_id),
                    "event_sequence": event.event_sequence,
                    "executor_payload": event.payload,
                },
            )
            return
        payload = {
            "type": "EXECUTOR_BOUNDARY",
            "execution_id": str(event.execution_id),
            "event_id": str(event.event_id),
            "event_sequence": event.event_sequence,
            "event_type": event.event_type,
        }
        inserted = await self._repository.ingest_executor_signal(
            stream_name=stream,
            message_id=dedupe_id,
            task_id=task_id,
            idempotency_key=f"executor-event:{event.event_id}",
            event_sequence=event.event_sequence,
            payload=payload,
        )
        publisher = self._publisher
        if inserted:
            if publisher is None:
                raise RuntimeError(
                    "Executor event publisher is not configured"
                )
            await publisher.publish_pending()


def merge_contiguous_events(
    current: ExecutorEvent,
    history: list[ExecutorEvent],
    *,
    after_sequence: int,
) -> list[ExecutorEvent]:
    if current.event_sequence <= after_sequence:
        return []
    by_sequence: dict[int, ExecutorEvent] = {}
    for event in [*history, current]:
        if event.execution_id != current.execution_id:
            raise ValueError("Executor history mixed execution IDs")
        if not after_sequence < event.event_sequence <= current.event_sequence:
            continue
        existing = by_sequence.get(event.event_sequence)
        if existing is not None and existing.event_id != event.event_id:
            raise ValueError("Executor history has conflicting event IDs")
        by_sequence[event.event_sequence] = event
    expected = list(range(after_sequence + 1, current.event_sequence + 1))
    if sorted(by_sequence) != expected:
        raise ValueError("Executor event history did not close sequence gap")
    return [by_sequence[sequence] for sequence in expected]


__all__ = ["ExecutorEventProcessor", "merge_contiguous_events"]
