from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any

from fastapi import FastAPI

from ex_agent.api.container import ApiContainer
from ex_agent.api.routers.failures import failure_router
from ex_agent.api.routers.health import health_router
from ex_agent.api.routers.promotions import promotion_router
from ex_agent.api.routers.stream_maintenance import (
    stream_maintenance_router,
)
from ex_agent.api.routers.tasks import (
    task_router,
    validate_signal_against_interrupt,
)
from ex_agent.api.routers.workflows import workflow_router
from ex_agent.config import Settings, get_settings


def create_app(
    settings: Settings | None = None,
    *,
    start_runtime: bool = True,
) -> FastAPI:
    resolved = settings or get_settings()

    @asynccontextmanager
    async def lifespan(app: FastAPI) -> AsyncIterator[None]:
        if start_runtime:
            from agent.api_host import open_api_container

            async with open_api_container(resolved) as container:
                app.state.container = container
                yield
        else:
            container = ApiContainer(resolved)
            app.state.container = container
            try:
                yield
            finally:
                await container.close()

    app = FastAPI(
        title="Execution Agent API",
        version="0.1.0",
        lifespan=lifespan,
    )
    app.include_router(health_router(resolved))
    app.include_router(task_router(resolved))
    app.include_router(failure_router())
    app.include_router(stream_maintenance_router())
    app.include_router(promotion_router())
    app.include_router(workflow_router())
    return app


def _validate_signal_against_interrupt(
    interrupt: dict[str, Any] | None,
    signal: dict[str, Any],
) -> None:
    validate_signal_against_interrupt(interrupt, signal)


__all__ = ["ApiContainer", "create_app"]
