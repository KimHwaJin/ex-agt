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
        if self._run_task is not None:
            raise RuntimeError("Workflow worker is a one-shot runtime")
        current_task = asyncio.current_task()
        if current_task is None:
            raise RuntimeError("Workflow worker requires an asyncio task")
        self._run_task = current_task
        self._running = True
        self._stopped.clear()
        try:
            if self._stop_requested.is_set():
                return
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
            if self._stop_requested.is_set():
                return
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
            self._command_stream_consumer = self._command_consumer()
            self._executor_stream_consumer = self._executor_event_consumer()
            await asyncio.gather(
                self._command_stream_consumer.initialize(),
                self._executor_stream_consumer.initialize(),
            )
            if self._stop_requested.is_set():
                return
            await self._publisher.publish_pending()
            if self._stop_requested.is_set():
                return
            self._runtime_tasks = {
                asyncio.create_task(self._command_stream_consumer.run()),
                asyncio.create_task(self._executor_stream_consumer.run()),
                asyncio.create_task(self._outbox_loop()),
                asyncio.create_task(self._metrics_loop()),
            }
            try:
                await asyncio.gather(*self._runtime_tasks)
            except asyncio.CancelledError:
                externally_cancelled = current_task.cancelling() > 0
                if externally_cancelled or not self._stop_requested.is_set():
                    raise
        finally:
            try:
                tasks = tuple(self._runtime_tasks)
                for task in tasks:
                    if not task.done():
                        task.cancel()
                if tasks:
                    await asyncio.gather(*tasks, return_exceptions=True)
                self._runtime_tasks.clear()
                await self.close()
            finally:
                self._running = False
                self._stopped.set()

    async def shutdown(
        self,
        grace_period_seconds: float | None = None,
    ) -> None:
        grace_period = (
            self._settings.worker_shutdown_grace_seconds
            if grace_period_seconds is None
            else grace_period_seconds
        )
        if grace_period < 0:
            raise ValueError("grace_period_seconds cannot be negative")
        async with self._shutdown_lock:
            self._mark_stopping()
            self._stop_requested.set()
            for consumer in (
                self._command_stream_consumer,
                self._executor_stream_consumer,
            ):
                if consumer is not None:
                    consumer.request_stop()
            if not self._running:
                return
            try:
                await asyncio.wait_for(
                    self._stopped.wait(),
                    timeout=grace_period,
                )
                return
            except TimeoutError:
                pass
            tasks = tuple(self._runtime_tasks)
            for task in tasks:
                if not task.done():
                    task.cancel()
            run_task = self._run_task
            current_task = asyncio.current_task()
            if (
                run_task is not None
                and run_task is not current_task
                and not run_task.done()
            ):
                run_task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)
            if run_task is not None and run_task is not current_task:
                await asyncio.gather(run_task, return_exceptions=True)

    def _mark_stopping(self) -> None:
        stopping = ReadinessResult.stopping()
        self._readiness.update(stopping)
        record_readiness("worker", stopping)

    async def close(self) -> None:
        self._mark_stopping()
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
