"""App assembly and lifespan; HTTP behavior lives in sibling packages."""

import logging
from collections.abc import Callable
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from d_test.agent_service.bootstrap.logging import initialize_logging
from d_test.agent_service.runtime.management import ManagementRuntime
from d_test.agent_service.settings import Settings

from ..auth.providers import IdentityProvider, build_identity
from ..handlers.exceptions import install_exception_handlers
from ..middleware.body_limits import RunBodyLimit
from ..middleware.request_context import install_request_context
from ..routers import get_management_routers
from ..routers.dev import install_dev_ui
from ..routers.health import router as health_router
from .runtime import ApiRuntime

logger = logging.getLogger("api_service")


def install_management_api(
    app: FastAPI,
    settings: Settings,
    *,
    identity_provider: IdentityProvider | None = None,
    initialize_host_logging: bool = False,
    include_routers: bool = True,
    enable_dev_ui: bool = True,
    runtime_context: Callable | None = None,
) -> None:
    """Call before server startup; preserve the host's existing lifespan."""
    if getattr(app.state, "management_installed", False):
        raise RuntimeError("Management API is already installed")
    expected = {
        (getattr(route, "path", None), method)
        for item in get_management_routers()
        for route in item.routes
        for method in getattr(route, "methods", ())
    }
    # FastAPI may retain included routers lazily. Use its public schema
    # builder rather than relying on private _IncludedRouter internals.
    paths = get_openapi(title="route-check", version="1", routes=app.routes)[
        "paths"
    ]
    actual = {
        (path, method.upper())
        for path, methods in paths.items()
        for method in methods
    }
    if include_routers and expected & actual:
        raise RuntimeError("Management route collision")
    if not include_routers and not expected <= actual:
        raise RuntimeError("Register all management routers before installing")
    app.state.management_installed = True
    previous_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(application: FastAPI):
        if initialize_host_logging:
            initialize_logging(settings)
        async with previous_lifespan(application) as state:
            provider = (
                identity_provider
                if identity_provider is not None
                else build_identity(settings)
            )
            resources = ManagementRuntime.build(settings)
            application.state.management_runtime = ApiRuntime(
                resources=resources, identity=provider
            )
            try:
                await resources.start()
                logger.info("management_api_started")
                if runtime_context is None:
                    yield state
                else:
                    async with runtime_context(
                        application.state.management_runtime
                    ):
                        yield state
            finally:
                try:
                    await resources.close()
                finally:
                    del application.state.management_runtime
                    logger.info("management_api_stopped")

    app.router.lifespan_context = lifespan
    if include_routers:
        for item in get_management_routers():
            app.include_router(item)
    app.add_middleware(RunBodyLimit)
    if enable_dev_ui:
        install_dev_ui(app, settings)

    install_request_context(app)
    install_exception_handlers(app)


def create_app(settings: Settings) -> FastAPI:
    app = FastAPI(title="Management API", version="1.0.0")
    install_management_api(app, settings, initialize_host_logging=True)
    app.include_router(health_router)
    return app
