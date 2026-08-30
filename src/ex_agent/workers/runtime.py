from __future__ import annotations

import asyncio

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from ex_agent.graph.builder import build_workflow_graph
from ex_agent.metrics import (
    WORKER_CONFIGURED_SLOTS,
    record_readiness,
    start_worker_metrics_server,
)
from ex_agent.readiness import ReadinessResult
from ex_agent.workers.checkpoints import checkpoint_serializer
from ex_agent.workers.context import WorkerContext


class WorkerRuntime(WorkerContext):
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
                    serde=checkpoint_serializer(),
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
                command_consumer.initialize(),
                executor_event_consumer.initialize(),
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


__all__ = ["WorkerRuntime"]
