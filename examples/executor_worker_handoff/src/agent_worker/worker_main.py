"""Standalone Worker entrypoint with no dependency on the source service."""

from __future__ import annotations

import asyncio
import logging
import signal
from typing import Any

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from pydantic_settings import BaseSettings, SettingsConfigDict

from agent_worker.graph_provider import build_agent_graph
from agent_worker.langgraph_adapter import LangGraphEventAdapter
from agent_worker.worker_hooks import build_handlers
from worker import EventContext, ExecutorWorker, Settings
from worker.contracts import EventHandler


class HostSettings(BaseSettings):
    """Only the host-specific setting needed by this entrypoint."""

    model_config = SettingsConfigDict(env_prefix="AGENT_", extra="ignore")

    checkpoint_database_url: str


class DeferredHandler:
    """Break the Worker → bindings → graph → handler construction cycle."""

    def __init__(self) -> None:
        self._target: EventHandler | None = None

    def bind(self, target: EventHandler) -> None:
        if self._target is not None:
            raise RuntimeError("Handler is already bound")
        self._target = target

    async def __call__(self, context: EventContext) -> None:
        if self._target is None:
            raise RuntimeError("Agent graph handler is not initialized")
        await self._target(context)

    async def ready(self) -> bool:
        return self._target is not None


def _validate_graph(graph: Any) -> None:
    required = ("aget_state", "ainvoke")
    if not all(callable(getattr(graph, name, None)) for name in required):
        raise TypeError(
            "Graph must provide async get-state and invoke methods"
        )
    if getattr(graph, "checkpointer", None) in (None, False):
        raise ValueError("Graph must use a persistent checkpointer")


def _install_signal_handlers(worker: ExecutorWorker) -> list[signal.Signals]:
    loop = asyncio.get_running_loop()
    installed: list[signal.Signals] = []
    for signum in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(signum, worker.request_stop)
        except NotImplementedError:
            continue
        installed.append(signum)
    return installed


async def main() -> None:
    worker_settings = Settings()
    host_settings = HostSettings()
    deferred = DeferredHandler()
    handlers = build_handlers(deferred)
    if not handlers or any(
        not event_type.strip() or not callable(handler)
        for event_type, handler in handlers.items()
    ):
        raise ValueError("build_handlers() must return valid handlers")

    async with ExecutorWorker(worker_settings, handlers) as worker:
        async with AsyncPostgresSaver.from_conn_string(
            host_settings.checkpoint_database_url
        ) as checkpointer:
            graph = build_agent_graph(
                bindings=worker.bindings,
                checkpointer=checkpointer,
            )
            _validate_graph(graph)
            deferred.bind(LangGraphEventAdapter(graph))
            worker.add_readiness_check("agent-graph", deferred.ready)
            installed = _install_signal_handlers(worker)
            try:
                await worker.run()
            finally:
                loop = asyncio.get_running_loop()
                for signum in installed:
                    loop.remove_signal_handler(signum)


def run_worker() -> None:
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())


if __name__ == "__main__":
    run_worker()
