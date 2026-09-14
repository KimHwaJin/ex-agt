"""Attach our lifecycle after the host constructs its FastAPI app."""

from contextlib import asynccontextmanager

from fastapi import FastAPI

from agent_service.integrations.template_workflow import (
    RequestResolver,
    WorkflowManager,
)
from agent_service.settings import Settings
from api_service.factory import install_management_api
from api_service.security import IdentityProvider


def install_template_runtime(
    app: FastAPI,
    settings: Settings,
    *,
    manager: WorkflowManager,
    resolve: RequestResolver,
    identity_provider: IdentityProvider | None = None,
) -> None:
    """Host get_routers() must register get_management_routers() first."""
    if settings.logging_mode != "preconfigured":
        raise ValueError("Initialize host logging before attaching runtime")

    @asynccontextmanager
    async def bind(runtime):
        async with manager.bind(runtime.resources.runs, resolve):
            yield

    install_management_api(
        app,
        settings,
        include_routers=False,
        enable_dev_ui=False,
        runtime_context=bind,
        identity_provider=identity_provider,
    )
