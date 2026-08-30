from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from http.server import HTTPServer
from time import perf_counter
from typing import Any
from uuid import UUID, uuid4

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.types import Command, Interrupt
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from ex_agent.application.services import DefaultWorkflowServices
from ex_agent.config import Settings
from ex_agent.domain.enums import TaskStatus
from ex_agent.executor.client import ExecutorClient, ExecutorRequestError
from ex_agent.executor.contracts import ExecutorEvent
from ex_agent.graph.builder import build_workflow_graph
from ex_agent.metrics import (
    CHECKPOINT_POOL,
    DELIVERY_BACKLOG,
    LOCK_CONTENTION,
    OUTBOX_PUBLISHED,
    OUTBOX_RELAY_SECONDS,
    REDIS_STREAM_LAG,
    REDIS_STREAM_PENDING,
    WORKER_ACTIVE,
    WORKER_CONFIGURED_SLOTS,
    WORKER_OPERATION_SECONDS,
    WORKER_OPERATIONS,
    WORKER_RETRIES,
    start_worker_metrics_server,
    update_database_pool_metrics,
)
from ex_agent.persistence.database import (
    create_engine,
    create_session_factory,
)
from ex_agent.persistence.repository import AgentRepository
from ex_agent.tools.registry import ToolRegistry
from ex_agent.transport.streams import CommandPublisher

logger = logging.getLogger(__name__)

_RENEW_LOCK_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('expire', KEYS[1], ARGV[2])
end
return 0
"""

_RELEASE_LOCK_SCRIPT = """
if redis.call('get', KEYS[1]) == ARGV[1] then
    return redis.call('del', KEYS[1])
end
return 0
"""

_FAILURE_COMPENSATION = "FAILURE_COMPENSATION"
_EXECUTOR_TERMINAL_STATUSES = {"CANCELLED", "SUCCEEDED", "FAILED"}


class TaskLockLostError(RuntimeError):
    def __init__(self, lock_key: str) -> None:
        super().__init__(f"Task lock was lost: {lock_key}")


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
        self._registry = ToolRegistry(settings.agent_skill_root)
        self._registry.load()
        self._services = DefaultWorkflowServices(
            settings,
            self._repository,
            self._executor,
            self._registry,
        )
        self._consumer = f"worker-{uuid4()}"
        self._checkpoint_pool: AsyncConnectionPool[Any] | None = None
        self._graphs: list[Any] = []
        self._metrics_server: HTTPServer | None = None

    async def run(self) -> None:
        WORKER_CONFIGURED_SLOTS.labels(kind="command").set(
            self._settings.worker_command_concurrency
        )
        WORKER_CONFIGURED_SLOTS.labels(kind="executor_event").set(
            self._settings.worker_executor_event_concurrency
        )
        if self._settings.worker_metrics_enabled:
            self._metrics_server = start_worker_metrics_server(
                self._settings.worker_metrics_host,
                self._settings.worker_metrics_port,
            )
        self._checkpoint_pool = AsyncConnectionPool(
            conninfo=self._settings.agent_checkpoint_database_url,
            kwargs={
                "autocommit": True,
                "prepare_threshold": 0,
                "row_factory": dict_row,
            },
            min_size=self._settings.checkpoint_pool_min_size,
            max_size=self._settings.checkpoint_pool_max_size,
            open=False,
        )
        await self._checkpoint_pool.open()
        try:
            for index in range(self._settings.worker_command_concurrency):
                saver = AsyncPostgresSaver(
                    self._checkpoint_pool,
                    serde=_checkpoint_serializer(),
                )
                if index == 0:
                    await saver.setup()
                self._graphs.append(
                    build_workflow_graph(
                        self._services,
                        checkpointer=saver,
                    )
                )
            await self._ensure_groups()
            await self._publisher.publish_pending()
            await asyncio.gather(
                *(
                    self._command_loop(graph, slot_index)
                    for slot_index, graph in enumerate(self._graphs)
                ),
                *(
                    self._executor_event_loop(slot_index)
                    for slot_index in range(
                        self._settings.worker_executor_event_concurrency
                    )
                ),
                self._outbox_loop(),
                self._metrics_loop(),
            )
        finally:
            await self.close()

    async def close(self) -> None:
        if self._metrics_server is not None:
            await asyncio.to_thread(self._metrics_server.shutdown)
            self._metrics_server.server_close()
            self._metrics_server = None
        if self._checkpoint_pool is not None:
            await self._checkpoint_pool.close()
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

    def _next_retry_delay(self, current: float) -> float:
        return min(current * 2, self._settings.worker_retry_max_seconds)

    async def _metrics_loop(self) -> None:
        while True:
            try:
                await self._collect_runtime_metrics()
            except Exception:
                logger.exception("Runtime metrics collection failed")
                WORKER_RETRIES.labels(component="metrics").inc()
            await asyncio.sleep(self._settings.worker_metrics_refresh_seconds)

    async def _collect_runtime_metrics(self) -> None:
        update_database_pool_metrics("worker", self._engine)
        for kind, states in {
            "command": ("PENDING", "PUBLISHING"),
            "task_event": ("PENDING", "PUBLISHING"),
        }.items():
            for state in states:
                DELIVERY_BACKLOG.labels(kind=kind, state=state).set(0)
        for (kind, state), count in (
            await self._repository.delivery_backlog_counts()
        ).items():
            DELIVERY_BACKLOG.labels(kind=kind, state=state).set(count)
        await self._set_stream_metrics(
            logical_name="commands",
            stream=self._settings.agent_command_stream,
            group=self._settings.agent_command_consumer_group,
        )
        await self._set_stream_metrics(
            logical_name="executor_events",
            stream=self._settings.executor_event_stream,
            group=self._settings.executor_event_consumer_group,
        )
        if self._checkpoint_pool is not None:
            for stat, value in self._checkpoint_pool.get_stats().items():
                if isinstance(value, int | float):
                    CHECKPOINT_POOL.labels(stat=stat).set(value)

    async def _set_stream_metrics(
        self,
        *,
        logical_name: str,
        stream: str,
        group: str,
    ) -> None:
        pending: Any = await self._redis.xpending(stream, group)
        pending_count = (
            pending.get("pending", 0) if isinstance(pending, dict) else 0
        )
        REDIS_STREAM_PENDING.labels(stream=logical_name).set(pending_count)
        groups: Any = await self._redis.xinfo_groups(stream)
        lag = 0
        for group_info in groups:
            if group_info.get("name") == group:
                lag = group_info.get("lag") or 0
                break
        REDIS_STREAM_LAG.labels(stream=logical_name).set(lag)

    async def _outbox_loop(self) -> None:
        retry_delay = self._settings.worker_retry_initial_seconds
        poll_milliseconds = self._settings.outbox_poll_milliseconds
        while True:
            started_at = perf_counter()
            try:
                published = await self._publisher.publish_pending()
                OUTBOX_PUBLISHED.inc(published)
                retry_delay = self._settings.worker_retry_initial_seconds
                if published:
                    poll_milliseconds = self._settings.outbox_poll_milliseconds
                else:
                    poll_milliseconds = min(
                        poll_milliseconds * 2,
                        self._settings.outbox_idle_max_milliseconds,
                    )
            except Exception:
                logger.exception("Outbox relay iteration failed")
                WORKER_RETRIES.labels(component="outbox").inc()
                await asyncio.sleep(retry_delay)
                retry_delay = self._next_retry_delay(retry_delay)
                continue
            finally:
                OUTBOX_RELAY_SECONDS.observe(perf_counter() - started_at)
            await asyncio.sleep(poll_milliseconds / 1000)

    async def _command_loop(self, graph: Any, slot_index: int) -> None:
        stream = self._settings.agent_command_stream
        group = self._settings.agent_command_consumer_group
        consumer = f"{self._consumer}-command-{slot_index}"
        retry_delay = self._settings.worker_retry_initial_seconds
        while True:
            try:
                claimed = await self._claim_stale_commands(
                    stream,
                    group,
                    consumer,
                )
                if claimed:
                    message_id, fields = claimed[0]
                    await self._handle_command(
                        graph,
                        consumer,
                        stream,
                        group,
                        message_id,
                        fields,
                    )
                    retry_delay = self._settings.worker_retry_initial_seconds
                    continue
                messages = await self._redis.xreadgroup(
                    group,
                    consumer,
                    {stream: ">"},
                    count=1,
                    block=self._settings.command_block_milliseconds,
                )
                for _, entries in messages:
                    for message_id, fields in entries:
                        await self._handle_command(
                            graph,
                            consumer,
                            stream,
                            group,
                            message_id,
                            fields,
                        )
                retry_delay = self._settings.worker_retry_initial_seconds
            except Exception:
                logger.exception(
                    "Command consumer iteration failed",
                    extra={"consumer": consumer},
                )
                WORKER_RETRIES.labels(component="command_consumer").inc()
                await asyncio.sleep(retry_delay)
                retry_delay = self._next_retry_delay(retry_delay)

    async def _claim_stale_commands(
        self,
        stream: str,
        group: str,
        consumer: str,
    ) -> list[tuple[str, dict[str, str]]]:
        response = await self._redis.xautoclaim(
            stream,
            group,
            consumer,
            min_idle_time=(self._settings.command_claim_idle_milliseconds),
            start_id="0-0",
            count=1,
        )
        return _autoclaim_entries(response)

    async def _handle_command(
        self,
        graph: Any,
        consumer: str,
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
            ex=self._settings.task_lock_ttl_seconds,
        )
        if not acquired:
            LOCK_CONTENTION.labels(kind="task").inc()
            return
        started_at = perf_counter()
        outcome = "succeeded"
        WORKER_ACTIVE.labels(kind="command").inc()
        heartbeat: asyncio.Task[None] | None = None
        command_task: asyncio.Task[None] | None = None
        try:
            heartbeat = asyncio.create_task(
                self._renew_lock_and_stream_lease(
                    stream,
                    group,
                    message_id,
                    lock_key,
                    lock_value,
                    consumer,
                    lock_ttl_seconds=(self._settings.task_lock_ttl_seconds),
                    renew_interval_seconds=(
                        self._settings.task_lock_renew_interval_seconds
                    ),
                )
            )
            command_task = asyncio.create_task(
                self._process_command(
                    graph,
                    stream,
                    group,
                    message_id,
                    command_id,
                )
            )
            done, _ = await asyncio.wait(
                {heartbeat, command_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if heartbeat in done:
                error = heartbeat.exception()
                command_task.cancel()
                with suppress(asyncio.CancelledError):
                    await command_task
                if error is not None:
                    raise error
                raise TaskLockLostError(lock_key)
            await command_task
        except Exception as error:
            outcome = "failed"
            logger.exception(
                "Workflow command failed", extra={"task_id": str(task_id)}
            )
            current = await self._repository.get_command(command_id)
            message = f"{type(error).__name__}: {error}"
            if (
                current is not None
                and current.command_type == _FAILURE_COMPENSATION
            ):
                await self._repository.set_command_state(
                    command_id,
                    "PENDING",
                    message,
                )
            elif current is not None and current.attempt_count >= 3:
                await self._repository.prepare_failure_compensation(
                    command_id,
                    task_id,
                    message,
                )
            else:
                await self._repository.set_command_state(
                    command_id,
                    "PENDING",
                    message,
                )
            await self._redis.xack(stream, group, message_id)
        finally:
            for task in (heartbeat, command_task):
                if task is not None and not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
            WORKER_ACTIVE.labels(kind="command").dec()
            WORKER_OPERATIONS.labels(
                kind="command",
                outcome=outcome,
            ).inc()
            WORKER_OPERATION_SECONDS.labels(kind="command").observe(
                perf_counter() - started_at
            )
            await self._redis.eval(
                _RELEASE_LOCK_SCRIPT,
                1,
                lock_key,
                lock_value,
            )

    async def _process_command(
        self,
        graph: Any,
        stream: str,
        group: str,
        message_id: str,
        command_id: UUID,
    ) -> None:
        command = await self._repository.get_command(command_id)
        if command is None or command.state in {"DONE", "FAILED"}:
            await self._redis.xack(stream, group, message_id)
            return
        task = await self._repository.get_task(command.task_id)
        if task is not None and TaskStatus(task.status).is_terminal:
            await self._repository.set_command_state(command_id, "DONE")
            await self._redis.xack(stream, group, message_id)
            return
        await self._repository.set_command_state(command_id, "PROCESSING")
        if command.command_type == _FAILURE_COMPENSATION:
            await self._run_failure_compensation(command)
            await self._redis.xack(stream, group, message_id)
            return
        await self._run_graph_command(graph, command)
        await self._repository.set_command_state(command_id, "DONE")
        await self._redis.xack(stream, group, message_id)

    async def _renew_lock_and_stream_lease(
        self,
        stream: str,
        group: str,
        message_id: str,
        lock_key: str,
        lock_value: str,
        consumer: str,
        *,
        lock_ttl_seconds: int,
        renew_interval_seconds: int,
    ) -> None:
        while True:
            await asyncio.sleep(renew_interval_seconds)
            renewed = await self._redis.eval(
                _RENEW_LOCK_SCRIPT,
                1,
                lock_key,
                lock_value,
                lock_ttl_seconds,
            )
            if renewed != 1:
                raise TaskLockLostError(lock_key)
            await self._redis.xclaim(
                stream,
                group,
                consumer,
                min_idle_time=0,
                message_ids=[message_id],
                justid=True,
            )

    async def _run_graph_command(self, graph: Any, command: Any) -> None:
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
        result = await graph.ainvoke(graph_input, config=config)
        interrupts = result.get("__interrupt__", ())
        if interrupts:
            payload = _interrupt_payload(interrupts[0])
            await self._repository.record_interrupt(task.id, payload)

    async def _run_failure_compensation(self, command: Any) -> None:
        raw_message = command.payload.get("failure_message")
        if not isinstance(raw_message, str) or not raw_message:
            raise ValueError("Failure compensation omitted failure_message")
        executor_status = await self._compensate_failed_execution(
            command.task_id,
            raw_message,
        )
        if executor_status == "NOT_REQUIRED":
            cleanup_message = "연결된 Executor 실행은 생성되지 않았습니다."
        elif executor_status == "CANCELLED":
            cleanup_message = "연결된 Executor 실행의 취소를 확인했습니다."
        else:
            cleanup_message = (
                "연결된 Executor 실행이 이미 "
                f"{executor_status} 상태로 종료된 것을 확인했습니다."
            )
        content = (
            f"Agent workflow 처리에 실패했습니다: {raw_message}. "
            f"{cleanup_message}"
        )
        await self._repository.complete_failure_compensation(
            command.id,
            command.task_id,
            content,
            failure_message=raw_message,
            executor_status=executor_status,
        )

    async def _compensate_failed_execution(
        self,
        task_id: UUID,
        failure_message: str,
    ) -> str:
        task = await self._repository.get_task(task_id)
        if task is None:
            raise LookupError(f"Unknown task: {task_id}")
        if task.execution_id is None:
            return "NOT_REQUIRED"
        execution_id = task.execution_id
        async with asyncio.timeout(
            self._settings.executor_failure_cleanup_timeout_seconds
        ):
            result = await self._executor.result(execution_id)
            status = result.execution.state.status
            if status in _EXECUTOR_TERMINAL_STATUSES:
                return status
            try:
                response = await self._executor.cancel(
                    execution_id,
                    idempotency_key=(f"task:{task_id}:agent-failure-cancel"),
                    actor_type="AGENT",
                    actor_id="ex-agent",
                    reason=f"Agent workflow failed: {failure_message}",
                )
            except ExecutorRequestError as error:
                if error.status_code not in {409, 422}:
                    raise
            else:
                await self._repository.update_binding(
                    task_id,
                    execution_version=response.state.version,
                )
            while True:
                result = await self._executor.result(execution_id)
                status = result.execution.state.status
                if status in _EXECUTOR_TERMINAL_STATUSES:
                    return status
                await asyncio.sleep(
                    self._settings.executor_failure_cleanup_poll_seconds
                )

    async def _executor_event_loop(self, slot_index: int) -> None:
        stream = self._settings.executor_event_stream
        group = self._settings.executor_event_consumer_group
        consumer = f"{self._consumer}-executor-{slot_index}"
        retry_delay = self._settings.worker_retry_initial_seconds
        while True:
            try:
                claimed = await self._claim_stale_executor_events(
                    stream,
                    group,
                    consumer,
                )
                if claimed:
                    message_id, fields = claimed[0]
                    await self._handle_executor_event(
                        consumer,
                        stream,
                        group,
                        message_id,
                        fields,
                    )
                    retry_delay = self._settings.worker_retry_initial_seconds
                    continue
                messages = await self._redis.xreadgroup(
                    group,
                    consumer,
                    {stream: ">"},
                    count=1,
                    block=self._settings.command_block_milliseconds,
                )
                for _, entries in messages:
                    for message_id, fields in entries:
                        await self._handle_executor_event(
                            consumer,
                            stream,
                            group,
                            message_id,
                            fields,
                        )
                retry_delay = self._settings.worker_retry_initial_seconds
            except Exception:
                logger.exception(
                    "Executor event consumer iteration failed",
                    extra={"consumer": consumer},
                )
                WORKER_RETRIES.labels(
                    component="executor_event_consumer"
                ).inc()
                await asyncio.sleep(retry_delay)
                retry_delay = self._next_retry_delay(retry_delay)

    async def _claim_stale_executor_events(
        self,
        stream: str,
        group: str,
        consumer: str,
    ) -> list[tuple[str, dict[str, str]]]:
        response = await self._redis.xautoclaim(
            stream,
            group,
            consumer,
            min_idle_time=(
                self._settings.executor_event_claim_idle_milliseconds
            ),
            start_id="0-0",
            count=1,
        )
        return _autoclaim_entries(response)

    async def _handle_executor_event(
        self,
        consumer: str,
        stream: str,
        group: str,
        message_id: str,
        fields: dict[str, str],
    ) -> None:
        event = ExecutorEvent.from_redis(fields)
        lock_key = f"agent:execution-lock:{event.execution_id}"
        lock_value = str(uuid4())
        acquired = await self._redis.set(
            lock_key,
            lock_value,
            nx=True,
            ex=self._settings.executor_event_lock_ttl_seconds,
        )
        if not acquired:
            LOCK_CONTENTION.labels(kind="execution").inc()
            return
        started_at = perf_counter()
        outcome = "succeeded"
        WORKER_ACTIVE.labels(kind="executor_event").inc()
        heartbeat: asyncio.Task[None] | None = None
        event_task: asyncio.Task[None] | None = None
        try:
            heartbeat = asyncio.create_task(
                self._renew_lock_and_stream_lease(
                    stream,
                    group,
                    message_id,
                    lock_key,
                    lock_value,
                    consumer,
                    lock_ttl_seconds=(
                        self._settings.executor_event_lock_ttl_seconds
                    ),
                    renew_interval_seconds=(
                        self._settings.executor_event_lock_renew_interval_seconds
                    ),
                )
            )
            event_task = asyncio.create_task(
                self._process_executor_event(
                    stream,
                    group,
                    message_id,
                    event,
                )
            )
            done, _ = await asyncio.wait(
                {heartbeat, event_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
            if heartbeat in done:
                error = heartbeat.exception()
                event_task.cancel()
                with suppress(asyncio.CancelledError):
                    await event_task
                if error is not None:
                    raise error
                raise TaskLockLostError(lock_key)
            await event_task
        except Exception:
            outcome = "failed"
            logger.exception(
                "Executor event processing failed",
                extra={
                    "execution_id": str(event.execution_id),
                    "event_id": str(event.event_id),
                },
            )
        finally:
            for task in (heartbeat, event_task):
                if task is not None and not task.done():
                    task.cancel()
                    with suppress(asyncio.CancelledError):
                        await task
            WORKER_ACTIVE.labels(kind="executor_event").dec()
            WORKER_OPERATIONS.labels(
                kind="executor_event",
                outcome=outcome,
            ).inc()
            WORKER_OPERATION_SECONDS.labels(kind="executor_event").observe(
                perf_counter() - started_at
            )
            await self._redis.eval(
                _RELEASE_LOCK_SCRIPT,
                1,
                lock_key,
                lock_value,
            )

    async def _process_executor_event(
        self,
        stream: str,
        group: str,
        message_id: str,
        event: ExecutorEvent,
    ) -> None:
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


def _checkpoint_serializer() -> JsonPlusSerializer:
    return JsonPlusSerializer(
        allowed_msgpack_modules=[
            ("ex_agent.domain.contracts", "IntentDecision"),
            ("ex_agent.domain.contracts", "PlanDraft"),
            ("ex_agent.domain.contracts", "RiskReview"),
            ("ex_agent.domain.contracts", "WorkflowCandidate"),
            ("ex_agent.domain.enums", "ExecutionMode"),
            ("ex_agent.domain.enums", "Intent"),
            ("ex_agent.domain.enums", "PlanningKind"),
            ("ex_agent.domain.enums", "RiskLevel"),
            ("ex_agent.domain.enums", "TaskStatus"),
        ]
    )
