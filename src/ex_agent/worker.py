from __future__ import annotations

import asyncio
import logging
from contextlib import AsyncExitStack
from typing import Any
from uuid import UUID, uuid4

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.types import Command, Interrupt
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from ex_agent.application.services import DefaultWorkflowServices
from ex_agent.config import Settings
from ex_agent.domain.enums import TaskStatus
from ex_agent.executor.client import ExecutorClient
from ex_agent.executor.contracts import ExecutorEvent
from ex_agent.graph.builder import build_workflow_graph
from ex_agent.persistence.database import (
    create_engine,
    create_session_factory,
)
from ex_agent.persistence.repository import AgentRepository
from ex_agent.tools.registry import ToolRegistry
from ex_agent.transport.streams import CommandPublisher

logger = logging.getLogger(__name__)


class WorkflowWorker:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self._engine = create_engine(settings.agent_database_url)
        self._repository = AgentRepository(
            create_session_factory(self._engine)
        )
        self._redis = Redis.from_url(
            settings.agent_redis_url,
            decode_responses=True,
        )
        self._publisher = CommandPublisher(
            settings,
            self._repository,
            self._redis,
        )
        self._executor = ExecutorClient(
            settings.executor_base_url,
            timeout_seconds=settings.executor_request_timeout_seconds,
        )
        self._registry = ToolRegistry(_skill_root())
        self._registry.load()
        self._services = DefaultWorkflowServices(
            settings,
            self._repository,
            self._executor,
            self._registry,
        )
        self._consumer = f"worker-{uuid4()}"
        self._exit_stack = AsyncExitStack()
        self._graph: Any = None

    async def run(self) -> None:
        saver_context = AsyncPostgresSaver.from_conn_string(
            self._settings.agent_checkpoint_database_url
        )
        saver = await self._exit_stack.enter_async_context(saver_context)
        await saver.setup()
        self._graph = build_workflow_graph(
            self._services,
            checkpointer=saver,
        )
        await self._ensure_groups()
        await self._publisher.publish_pending()
        try:
            await asyncio.gather(
                self._command_loop(),
                self._executor_event_loop(),
                self._outbox_loop(),
            )
        finally:
            await self.close()

    async def close(self) -> None:
        await self._exit_stack.aclose()
        await self._executor.close()
        await self._redis.aclose()
        await self._engine.dispose()

    async def _ensure_groups(self) -> None:
        await self._ensure_group(
            self._settings.agent_command_stream,
            self._settings.agent_command_consumer_group,
        )
        await self._ensure_group(
            self._settings.executor_event_stream,
            self._settings.executor_event_consumer_group,
        )

    async def _ensure_group(self, stream: str, group: str) -> None:
        try:
            await self._redis.xgroup_create(
                stream,
                group,
                id="0",
                mkstream=True,
            )
        except ResponseError as error:
            if "BUSYGROUP" not in str(error):
                raise

    async def _outbox_loop(self) -> None:
        while True:
            await self._publisher.publish_pending()
            await asyncio.sleep(1)

    async def _command_loop(self) -> None:
        stream = self._settings.agent_command_stream
        group = self._settings.agent_command_consumer_group
        while True:
            claimed = await self._claim_stale_commands(stream, group)
            if claimed:
                await self._handle_command_entries(
                    stream,
                    group,
                    claimed,
                )
                continue
            messages = await self._redis.xreadgroup(
                group,
                self._consumer,
                {stream: ">"},
                count=10,
                block=self._settings.command_block_milliseconds,
            )
            for _, entries in messages:
                await self._handle_command_entries(stream, group, entries)

    async def _claim_stale_commands(
        self,
        stream: str,
        group: str,
    ) -> list[tuple[str, dict[str, str]]]:
        response = await self._redis.xautoclaim(
            stream,
            group,
            self._consumer,
            min_idle_time=(self._settings.command_claim_idle_milliseconds),
            start_id="0-0",
            count=self._settings.command_claim_batch_size,
        )
        return _autoclaim_entries(response)

    async def _handle_command_entries(
        self,
        stream: str,
        group: str,
        entries: list[tuple[str, dict[str, str]]],
    ) -> None:
        for message_id, fields in entries:
            await self._handle_command(stream, group, message_id, fields)

    async def _handle_command(
        self,
        stream: str,
        group: str,
        message_id: str,
        fields: dict[str, str],
    ) -> None:
        command_id = UUID(fields["command_id"])
        task_id = UUID(fields["task_id"])
        lock_key = f"agent:task-lock:{task_id}"
        lock_value = str(uuid4())
        acquired = await self._redis.set(
            lock_key,
            lock_value,
            nx=True,
            ex=300,
        )
        if not acquired:
            return
        try:
            command = await self._repository.get_command(command_id)
            if command is None or command.state == "DONE":
                await self._redis.xack(stream, group, message_id)
                return
            await self._repository.set_command_state(command_id, "PROCESSING")
            await self._run_graph_command(command)
            await self._repository.set_command_state(command_id, "DONE")
            await self._redis.xack(stream, group, message_id)
        except Exception as error:
            logger.exception(
                "Workflow command failed", extra={"task_id": str(task_id)}
            )
            current = await self._repository.get_command(command_id)
            message = f"{type(error).__name__}: {error}"
            if current is not None and current.attempt_count >= 3:
                await self._repository.set_command_state(
                    command_id,
                    "FAILED",
                    message,
                )
                await self._repository.commit_message(
                    task_id,
                    f"Agent workflow 처리에 실패했습니다: {message}",
                    status=TaskStatus.FAILED,
                )
            else:
                await self._repository.set_command_state(
                    command_id,
                    "PENDING",
                    message,
                )
            await self._redis.xack(stream, group, message_id)
        finally:
            if await self._redis.get(lock_key) == lock_value:
                await self._redis.delete(lock_key)

    async def _run_graph_command(self, command: Any) -> None:
        task = await self._repository.get_task(command.task_id)
        if task is None:
            raise LookupError(f"Unknown task: {command.task_id}")
        config = _task_graph_config(task.id)
        if command.command_type == "START":
            graph_input: Any = {
                "user_id": task.user_id,
                "project_id": task.project_id,
                "session_id": task.session_id,
                "active_task_id": str(task.id),
                "current_input_message_id": str(task.input_message_id),
                "user_message": task.user_message,
            }
        else:
            await self._repository.clear_interrupt(task.id)
            graph_input = Command(resume=command.payload)
        result = await self._graph.ainvoke(graph_input, config=config)
        interrupts = result.get("__interrupt__", ())
        if interrupts:
            payload = _interrupt_payload(interrupts[0])
            await self._repository.record_interrupt(task.id, payload)

    async def _executor_event_loop(self) -> None:
        stream = self._settings.executor_event_stream
        group = self._settings.executor_event_consumer_group
        while True:
            messages = await self._redis.xreadgroup(
                group,
                self._consumer,
                {stream: ">"},
                count=50,
                block=self._settings.command_block_milliseconds,
            )
            for _, entries in messages:
                for message_id, fields in entries:
                    await self._handle_executor_event(
                        stream,
                        group,
                        message_id,
                        fields,
                    )

    async def _handle_executor_event(
        self,
        stream: str,
        group: str,
        message_id: str,
        fields: dict[str, str],
    ) -> None:
        event = ExecutorEvent.from_redis(fields)
        binding = await self._repository.binding_for_execution(
            event.execution_id
        )
        if binding is None:
            await self._redis.xack(stream, group, message_id)
            return
        history: list[ExecutorEvent] = []
        if event.event_sequence > binding.last_event_sequence + 1:
            history = await self._executor.events_after(
                event.execution_id,
                after_sequence=binding.last_event_sequence,
                limit=(event.event_sequence - binding.last_event_sequence),
            )
        ordered = _merge_contiguous_events(
            event,
            history,
            after_sequence=binding.last_event_sequence,
        )
        for ordered_event in ordered:
            await self._persist_executor_event(
                stream,
                binding.task_id,
                ordered_event,
            )
        await self._redis.xack(stream, group, message_id)

    async def _persist_executor_event(
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
        if inserted:
            await self._publisher.publish_pending()


def _merge_contiguous_events(
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


def _interrupt_payload(value: Any) -> dict[str, Any]:
    if isinstance(value, Interrupt):
        raw = value.value
    else:
        raw = getattr(value, "value", value)
    if not isinstance(raw, dict):
        raise TypeError("Graph interrupt payload must be an object")
    return raw


def _autoclaim_entries(
    response: Any,
) -> list[tuple[str, dict[str, str]]]:
    if not isinstance(response, (list, tuple)) or len(response) < 2:
        raise TypeError("Redis XAUTOCLAIM returned an invalid response")
    entries = response[1]
    if not isinstance(entries, list):
        raise TypeError("Redis XAUTOCLAIM entries must be a list")
    return entries


def _task_graph_config(task_id: UUID) -> dict[str, Any]:
    return {"configurable": {"thread_id": str(task_id)}}


def _skill_root() -> Any:
    from pathlib import Path

    return Path(__file__).resolve().parents[2] / "skills"
