from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID, uuid4
from wsgiref.simple_server import WSGIServer

from psycopg_pool import AsyncConnectionPool
from redis.asyncio import Redis

from ex_agent.application.services import DefaultWorkflowServices
from ex_agent.config import Settings
from ex_agent.executor.client import ExecutorClient
from ex_agent.executor.contracts import ExecutorEvent
from ex_agent.metrics import record_readiness
from ex_agent.persistence.database import (
    create_engine,
    create_session_factory,
)
from ex_agent.persistence.repository import AgentRepository
from ex_agent.readiness import ReadinessResult, ReadinessState
from ex_agent.tools.registry import ToolRegistry
from ex_agent.transport.streams import CommandPublisher
from ex_agent.workers.checkpoints import autoclaim_entries, task_graph_config
from ex_agent.workers.checkpoints import (
    checkpoint_serializer as _checkpoint_serializer,
)
from ex_agent.workers.commands import CommandProcessor
from ex_agent.workers.compatibility import WorkerCompatibility
from ex_agent.workers.consumers import WorkerConsumers
from ex_agent.workers.executor_events import (
    ExecutorEventProcessor,
    merge_contiguous_events,
)
from ex_agent.workers.maintenance import WorkerMaintenance
from ex_agent.workers.runtime import WorkerRuntime


class WorkflowWorker(
    WorkerRuntime,
    WorkerConsumers,
    WorkerMaintenance,
    WorkerCompatibility,
):
    """Worker composition root and backward-compatible public façade."""

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
        self._command_stream_consumer = None
        self._executor_stream_consumer = None
        self._runtime_tasks: set[asyncio.Task[None]] = set()
        self._run_task: asyncio.Task[None] | None = None
        self._stop_requested = asyncio.Event()
        self._stopped = asyncio.Event()
        self._stopped.set()
        self._shutdown_lock = asyncio.Lock()
        self._running = False
        record_readiness("worker", ReadinessResult.starting())


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


__all__ = [
    "WorkflowWorker",
    "_autoclaim_entries",
    "_checkpoint_serializer",
    "_merge_contiguous_events",
    "_task_graph_config",
]
