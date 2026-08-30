from __future__ import annotations

import asyncio
import logging
from time import perf_counter
from typing import Any
from uuid import UUID, uuid4
from wsgiref.simple_server import WSGIServer

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
    REDIS_DEAD_LETTERED,
    REDIS_STREAM_LAG,
    REDIS_STREAM_PENDING,
    WORKER_ACTIVE,
    WORKER_CONFIGURED_SLOTS,
    WORKER_OPERATION_SECONDS,
    WORKER_OPERATIONS,
    WORKER_RETRIES,
    record_readiness,
    start_worker_metrics_server,
    update_database_pool_metrics,
)
from ex_agent.persistence.database import (
    create_engine,
    create_session_factory,
)
from ex_agent.persistence.repository import AgentRepository
from ex_agent.readiness import (
    ReadinessResult,
    ReadinessState,
    probe_dependencies,
)
from ex_agent.tools.registry import ToolRegistry
from ex_agent.transport.consumer import (
    AckDecision,
    ConsumerObserver,
    HandlerResult,
    PermanentMessageError,
    RedisStreamConsumer,
    RedisStreamConsumerConfig,
    StreamMessage,
    _autoclaim_page,
)
from ex_agent.transport.streams import CommandPublisher

logger = logging.getLogger(__name__)

_FAILURE_COMPENSATION = "FAILURE_COMPENSATION"
_EXECUTOR_TERMINAL_STATUSES = {"CANCELLED", "SUCCEEDED", "FAILED"}


class _WorkerConsumerObserver(ConsumerObserver):
    def __init__(
        self,
        *,
        kind: str,
        lock_kind: str,
        retry_component: str,
        stream: str,
    ) -> None:
        self._kind = kind
        self._lock_kind = lock_kind
        self._retry_component = retry_component
        self._stream = stream

    def operation_started(self) -> None:
        WORKER_ACTIVE.labels(kind=self._kind).inc()

    def lock_contended(self) -> None:
        LOCK_CONTENTION.labels(kind=self._lock_kind).inc()

    def operation_finished(self, outcome: str, duration: float) -> None:
        WORKER_ACTIVE.labels(kind=self._kind).dec()
        WORKER_OPERATIONS.labels(
            kind=self._kind,
            outcome=outcome,
        ).inc()
        WORKER_OPERATION_SECONDS.labels(kind=self._kind).observe(duration)

    def transport_retry(self) -> None:
        WORKER_RETRIES.labels(component=self._retry_component).inc()

    def dead_lettered(self) -> None:
        REDIS_DEAD_LETTERED.labels(stream=self._stream).inc()


class _CommandHandler:
    def __init__(self, worker: WorkflowWorker, graph: Any) -> None:
        self._worker = worker
        self._graph = graph

    def lock_key(self, message: StreamMessage) -> str:
        try:
            task_id = UUID(message.fields["task_id"])
        except (KeyError, ValueError) as error:
            raise PermanentMessageError(
                f"Invalid command task_id: {error}"
            ) from error
        return f"agent:task-lock:{task_id}"

    async def handle(self, message: StreamMessage) -> HandlerResult:
        try:
            command_id = UUID(message.fields["command_id"])
            task_id = UUID(message.fields["task_id"])
        except (KeyError, ValueError) as error:
            raise PermanentMessageError(
                f"Invalid command envelope: {error}"
            ) from error
        try:
            await self._worker._process_command(self._graph, command_id)
        except Exception as error:
            logger.exception(
                "Workflow command failed",
                extra={"task_id": str(task_id)},
            )
            current = await self._worker._repository.get_command(command_id)
            failure_message = f"{type(error).__name__}: {error}"
            if (
                current is not None
                and current.command_type == _FAILURE_COMPENSATION
            ):
                await self._worker._repository.set_command_state(
                    command_id,
                    "PENDING",
                    failure_message,
                )
            elif current is not None and current.attempt_count >= 3:
                await self._worker._repository.prepare_failure_compensation(
                    command_id,
                    task_id,
                    failure_message,
                )
            else:
                await self._worker._repository.set_command_state(
                    command_id,
                    "PENDING",
                    failure_message,
                )
            return HandlerResult(AckDecision.ACK, outcome="failed")
        return HandlerResult(AckDecision.ACK)


class _ExecutorEventHandler:
    def __init__(self, worker: WorkflowWorker, stream: str) -> None:
        self._worker = worker
        self._stream = stream

    def _event(self, message: StreamMessage) -> ExecutorEvent:
        try:
            return ExecutorEvent.from_redis(message.fields)
        except (KeyError, TypeError, ValueError) as error:
            raise PermanentMessageError(
                f"Invalid Executor event envelope: {error}"
            ) from error

    def lock_key(self, message: StreamMessage) -> str:
        event = self._event(message)
        return f"agent:execution-lock:{event.execution_id}"

    async def handle(self, message: StreamMessage) -> HandlerResult:
        event = self._event(message)
        processed = await self._worker._process_executor_event(
            self._stream,
            event,
            catch_up=message.reclaimed,
        )
        if processed is False:
            return HandlerResult(
                AckDecision.RETRY,
                outcome="binding_pending",
                reason="Executor binding is not visible yet",
            )
        return HandlerResult(AckDecision.ACK)


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
        instance_id = settings.worker_instance_id or str(uuid4())
        self._consumer = f"worker-{instance_id}"
        self._checkpoint_pool: AsyncConnectionPool[Any] | None = None
        self._graphs: list[Any] = []
        self._readiness = ReadinessState()
        self._metrics_server: WSGIServer | None = None
        record_readiness("worker", ReadinessResult.starting())

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
                self._readiness,
                stale_after_seconds=(
                    self._settings.worker_readiness_stale_seconds
                ),
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
            command_consumer = self._command_consumer()
            executor_event_consumer = self._executor_event_consumer()
            await asyncio.gather(
                command_consumer.ensure_group(),
                executor_event_consumer.ensure_group(),
            )
            await asyncio.gather(
                command_consumer.cleanup_consumers(),
                executor_event_consumer.cleanup_consumers(),
            )
            await self._publisher.publish_pending()
            await asyncio.gather(
                command_consumer.run(),
                executor_event_consumer.run(),
                self._outbox_loop(),
                self._metrics_loop(),
            )
        finally:
            await self.close()

    async def close(self) -> None:
        stopping = ReadinessResult.starting()
        self._readiness.update(stopping)
        record_readiness("worker", stopping)
        if self._metrics_server is not None:
            await asyncio.to_thread(self._metrics_server.shutdown)
            self._metrics_server.server_close()
            self._metrics_server = None
        if self._checkpoint_pool is not None:
            await self._checkpoint_pool.close()
        await self._executor.close()
        await self._redis.aclose()
        await self._engine.dispose()

    def _command_consumer(
        self,
        *,
        stream: str | None = None,
        group: str | None = None,
        graphs: list[Any] | None = None,
    ) -> RedisStreamConsumer:
        selected_graphs = graphs or self._graphs
        config = RedisStreamConsumerConfig(
            stream=stream or self._settings.agent_command_stream,
            group=group or self._settings.agent_command_consumer_group,
            consumer_prefix=f"{self._consumer}-command",
            concurrency=len(selected_graphs),
            block_milliseconds=(self._settings.command_block_milliseconds),
            claim_idle_milliseconds=(
                self._settings.command_claim_idle_milliseconds
            ),
            claim_batch_size=self._settings.stream_claim_batch_size,
            dead_letter_stream=(
                self._settings.agent_command_dead_letter_stream
            ),
            lock_ttl_seconds=self._settings.task_lock_ttl_seconds,
            lock_renew_interval_seconds=(
                self._settings.task_lock_renew_interval_seconds
            ),
            consumer_gc_idle_milliseconds=(
                self._settings.consumer_gc_idle_milliseconds
            ),
        )
        observer = _WorkerConsumerObserver(
            kind="command",
            lock_kind="task",
            retry_component="command_consumer",
            stream="commands",
        )
        return RedisStreamConsumer(
            self._redis,
            config,
            lambda slot_index: _CommandHandler(
                self,
                selected_graphs[slot_index],
            ),
            observer=observer,
            retry_initial_seconds=(
                self._settings.worker_retry_initial_seconds
            ),
            retry_max_seconds=self._settings.worker_retry_max_seconds,
        )

    def _executor_event_consumer(
        self,
        *,
        stream: str | None = None,
        group: str | None = None,
        concurrency: int | None = None,
    ) -> RedisStreamConsumer:
        selected_stream = stream or self._settings.executor_event_stream
        config = RedisStreamConsumerConfig(
            stream=selected_stream,
            group=group or self._settings.executor_event_consumer_group,
            consumer_prefix=f"{self._consumer}-executor",
            concurrency=(
                concurrency or self._settings.worker_executor_event_concurrency
            ),
            block_milliseconds=(self._settings.command_block_milliseconds),
            claim_idle_milliseconds=(
                self._settings.executor_event_claim_idle_milliseconds
            ),
            claim_batch_size=self._settings.stream_claim_batch_size,
            dead_letter_stream=(
                self._settings.executor_event_dead_letter_stream
            ),
            lock_ttl_seconds=(self._settings.executor_event_lock_ttl_seconds),
            lock_renew_interval_seconds=(
                self._settings.executor_event_lock_renew_interval_seconds
            ),
            consumer_gc_idle_milliseconds=(
                self._settings.consumer_gc_idle_milliseconds
            ),
        )
        observer = _WorkerConsumerObserver(
            kind="executor_event",
            lock_kind="execution",
            retry_component="executor_event_consumer",
            stream="executor_events",
        )
        return RedisStreamConsumer(
            self._redis,
            config,
            lambda _: _ExecutorEventHandler(self, selected_stream),
            observer=observer,
            retry_initial_seconds=(
                self._settings.worker_retry_initial_seconds
            ),
            retry_max_seconds=self._settings.worker_retry_max_seconds,
        )

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
                readiness = await probe_dependencies(
                    self._engine,
                    self._redis,
                    timeout_seconds=(
                        self._settings.readiness_probe_timeout_seconds
                    ),
                )
                self._readiness.update(readiness)
                record_readiness("worker", readiness)
                if readiness.ready:
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
        handler = _CommandHandler(self, graph)
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
        command = await self._repository.get_command(command_id)
        if command is None or command.state in {"DONE", "FAILED"}:
            return
        task = await self._repository.get_task(command.task_id)
        if task is not None and TaskStatus(task.status).is_terminal:
            await self._repository.set_command_state(command_id, "DONE")
            return
        await self._repository.set_command_state(command_id, "PROCESSING")
        if command.command_type == _FAILURE_COMPENSATION:
            await self._run_failure_compensation(command)
            return
        await self._run_graph_command(graph, command)
        await self._repository.set_command_state(command_id, "DONE")

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
        handler = _ExecutorEventHandler(self, stream)
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
        binding = await self._repository.binding_for_execution(
            event.execution_id
        )
        if binding is None:
            return False
        history: list[ExecutorEvent] = []
        if catch_up:
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
        return True

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
    return _autoclaim_page(response)[1]


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
