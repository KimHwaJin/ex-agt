"""Production FastAPI composition for direct session-graph admission."""

from __future__ import annotations

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from agent.graph import checkpoint_serializer
from agent.runtime import (
    build_worker_settings,
    open_agent_runtime,
    recovery_lifespan,
)
from agent.runtime.bridge import ApiWorkerBridge
from ex_agent.api.container import ApiContainer
from ex_agent.config import Settings


@asynccontextmanager
async def open_api_container(
    settings: Settings,
) -> AsyncIterator[ApiContainer]:
    """Open API resources; schema initialization remains a migration Job."""

    worker_settings = build_worker_settings(settings)
    async with ApiWorkerBridge(worker_settings) as bridge:
        async with AsyncPostgresSaver.from_conn_string(
            settings.agent_checkpoint_database_url,
            serde=checkpoint_serializer(),
        ) as saver:
            async with open_agent_runtime(
                settings,
                bridge,
                saver,
            ) as runtime:
                container = ApiContainer.from_runtime(
                    settings,
                    runtime=runtime,
                    redis=bridge.redis,
                )
                async with recovery_lifespan(
                    runtime.lifecycle,
                    shutdown_timeout_seconds=(
                        settings.worker_shutdown_grace_seconds
                    ),
                ):
                    yield container


__all__ = ["open_api_container"]
