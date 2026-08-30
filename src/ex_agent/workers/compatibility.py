from __future__ import annotations

from typing import Any, cast
from uuid import UUID

from ex_agent.executor.contracts import ExecutorEvent
from ex_agent.transport.consumer import StreamMessage
from ex_agent.workers.commands import CommandProcessor
from ex_agent.workers.context import WorkerContext
from ex_agent.workers.executor_events import ExecutorEventProcessor
from ex_agent.workers.handlers import CommandHandler, ExecutorEventHandler


class WorkerCompatibility(WorkerContext):
    """Stable handler and test-facing methods on WorkflowWorker."""

    async def _handle_command(
        self,
        graph: Any,
        consumer: str,
        stream: str,
        group: str,
        message_id: str,
        fields: dict[str, str],
    ) -> None:
        runtime = self._command_consumer(
            stream=stream,
            group=group,
            graphs=[graph],
        )
        handler = CommandHandler(cast(Any, self), graph)
        await runtime.process_message(
            consumer,
            handler,
            StreamMessage(message_id, fields),
        )

    async def _process_command(
        self,
        graph: Any,
        command_id: UUID,
    ) -> None:
        await self._commands().process(graph, command_id)

    async def _run_graph_command(self, graph: Any, command: Any) -> None:
        await self._commands().run_graph(graph, command)

    async def _run_failure_compensation(self, command: Any) -> None:
        await self._commands().run_failure_compensation(command)

    async def _compensate_failed_execution(
        self,
        task_id: UUID,
        failure_message: str,
    ) -> str:
        return await self._commands().compensate_failed_execution(
            task_id,
            failure_message,
        )

    def _commands(self) -> CommandProcessor:
        processor = getattr(self, "_command_processor", None)
        if processor is None:
            processor = CommandProcessor(
                self._settings,
                self._repository,
                self._executor,
            )
            self._command_processor = processor
        return processor

    async def _handle_executor_event(
        self,
        consumer: str,
        stream: str,
        group: str,
        message_id: str,
        fields: dict[str, str],
        *,
        catch_up: bool = False,
    ) -> None:
        runtime = self._executor_event_consumer(
            stream=stream,
            group=group,
            concurrency=1,
        )
        handler = ExecutorEventHandler(cast(Any, self), stream)
        await runtime.process_message(
            consumer,
            handler,
            StreamMessage(message_id, fields, reclaimed=catch_up),
        )

    async def _process_executor_event(
        self,
        stream: str,
        event: ExecutorEvent,
        *,
        catch_up: bool = False,
    ) -> bool:
        return await self._executor_events().process(
            stream,
            event,
            catch_up=catch_up,
            persist_event=self._persist_executor_event,
        )

    async def _persist_executor_event(
        self,
        stream: str,
        task_id: UUID,
        event: ExecutorEvent,
    ) -> None:
        await self._executor_events().persist(stream, task_id, event)

    def _executor_events(self) -> ExecutorEventProcessor:
        processor = getattr(self, "_executor_event_processor", None)
        if processor is None:
            processor = ExecutorEventProcessor(
                self._repository,
                getattr(self, "_executor", None),
                getattr(self, "_publisher", None),
            )
            self._executor_event_processor = processor
        return processor


__all__ = ["WorkerCompatibility"]
