from __future__ import annotations

import asyncio
from typing import Any
from wsgiref.simple_server import WSGIServer

from psycopg_pool import AsyncConnectionPool
from sqlalchemy.ext.asyncio import AsyncEngine

from ex_agent.application.services import DefaultWorkflowServices
from ex_agent.config import Settings
from ex_agent.executor.client import ExecutorClient
from ex_agent.persistence.repository import AgentRepository
from ex_agent.readiness import ReadinessState
from ex_agent.tools.registry import ToolRegistry
from ex_agent.transport.consumer import RedisStreamConsumer
from ex_agent.transport.streams import CommandPublisher
from ex_agent.workers.commands import CommandProcessor
from ex_agent.workers.executor_events import ExecutorEventProcessor


class WorkerContext:
    """Shared attributes and cross-capability contracts for one worker."""

    _settings: Settings
    _engine: AsyncEngine
    _repository: AgentRepository
    _redis: Any
    _publisher: CommandPublisher
    _executor: ExecutorClient
    _command_processor: CommandProcessor
    _executor_event_processor: ExecutorEventProcessor
    _registry: ToolRegistry
    _services: DefaultWorkflowServices
    _consumer: str
    _checkpoint_pool: AsyncConnectionPool[Any] | None
    _graphs: list[Any]
    _readiness: ReadinessState
    _metrics_server: WSGIServer | None
    _command_stream_consumer: RedisStreamConsumer | None
    _executor_stream_consumer: RedisStreamConsumer | None
    _runtime_tasks: set[asyncio.Task[None]]
    _run_task: asyncio.Task[None] | None
    _stop_requested: asyncio.Event
    _stopped: asyncio.Event
    _shutdown_lock: asyncio.Lock
    _running: bool

    def _command_consumer(
        self,
        *,
        stream: str | None = None,
        group: str | None = None,
        graphs: list[Any] | None = None,
    ) -> RedisStreamConsumer:
        raise NotImplementedError

    def _executor_event_consumer(
        self,
        *,
        stream: str | None = None,
        group: str | None = None,
        concurrency: int | None = None,
    ) -> RedisStreamConsumer:
        raise NotImplementedError

    async def _outbox_loop(self) -> None:
        raise NotImplementedError

    async def _metrics_loop(self) -> None:
        raise NotImplementedError


__all__ = ["WorkerContext"]
