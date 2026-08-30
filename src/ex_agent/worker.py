from __future__ import annotations

import asyncio
import logging
from time import perf_counter
from typing import Any
from uuid import UUID, uuid4
from wsgiref.simple_server import WSGIServer

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from ex_agent.application.services import DefaultWorkflowServices
from ex_agent.config import Settings
from ex_agent.executor.client import ExecutorClient
from ex_agent.executor.contracts import ExecutorEvent
from ex_agent.graph.builder import build_workflow_graph
from ex_agent.metrics import (
    CHECKPOINT_POOL,
    DELIVERY_BACKLOG,
    OUTBOX_PUBLISHED,
    OUTBOX_RELAY_SECONDS,
    REDIS_STREAM_LAG,
    REDIS_STREAM_PENDING,
    WORKER_CONFIGURED_SLOTS,
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
    RedisStreamConsumer,
    RedisStreamConsumerConfig,
    StreamMessage,
)
from ex_agent.transport.streams import CommandPublisher
from ex_agent.workers.checkpoints import autoclaim_entries, task_graph_config
from ex_agent.workers.checkpoints import (
    checkpoint_serializer as _checkpoint_serializer,
)
from ex_agent.workers.commands import CommandProcessor
from ex_agent.workers.executor_events import (
    ExecutorEventProcessor,
    merge_contiguous_events,
)
from ex_agent.workers.handlers import CommandHandler as _CommandHandler
from ex_agent.workers.handlers import (
    ExecutorEventHandler as _ExecutorEventHandler,
)
from ex_agent.workers.observers import (
    WorkerConsumerObserver as _WorkerConsumerObserver,
)

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
        self._command_processor = CommandProcessor(
            settings,
            self._repository,
            self._executor,
        )
        self._executor_event_processor = ExecutorEventProcessor(
            self._repository,
            self._executor,
            self._publisher,
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


def _autoclaim_entries(
    response: Any,
) -> list[tuple[str, dict[str, str]]]:
    return autoclaim_entries(response)


def _task_graph_config(task_id: UUID) -> dict[str, Any]:
    return task_graph_config(task_id)


def _merge_contiguous_events(
    current: ExecutorEvent,
    history: list[ExecutorEvent],
    *,
    after_sequence: int,
) -> list[ExecutorEvent]:
    return merge_contiguous_events(
        current,
        history,
        after_sequence=after_sequence,
    )
