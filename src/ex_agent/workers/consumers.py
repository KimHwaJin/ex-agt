from __future__ import annotations

from typing import Any, cast

from redis.exceptions import ResponseError

from ex_agent.transport.consumer import (
    RedisStreamConsumer,
    RedisStreamConsumerConfig,
)
from ex_agent.workers.context import WorkerContext
from ex_agent.workers.handlers import CommandHandler, ExecutorEventHandler
from ex_agent.workers.observers import WorkerConsumerObserver


class WorkerConsumers(WorkerContext):
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
            max_retry_attempts=(self._settings.command_max_retry_attempts),
            retry_state_ttl_seconds=(
                self._settings.stream_retry_state_ttl_seconds
            ),
        )
        observer = WorkerConsumerObserver(
            kind="command",
            lock_kind="task",
            retry_component="command_consumer",
            stream="commands",
        )
        worker = cast(Any, self)
        return RedisStreamConsumer(
            self._redis,
            config,
            lambda slot_index: CommandHandler(
                worker,
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
            max_retry_attempts=(
                self._settings.executor_event_max_retry_attempts
            ),
            retry_state_ttl_seconds=(
                self._settings.stream_retry_state_ttl_seconds
            ),
        )
        observer = WorkerConsumerObserver(
            kind="executor_event",
            lock_kind="execution",
            retry_component="executor_event_consumer",
            stream="executor_events",
        )
        worker = cast(Any, self)
        return RedisStreamConsumer(
            self._redis,
            config,
            lambda _: ExecutorEventHandler(worker, selected_stream),
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


__all__ = ["WorkerConsumers"]
