"""Standalone app factory and additive template integration."""

import logging
from collections.abc import Callable
from contextlib import asynccontextmanager
from dataclasses import dataclass
from time import monotonic
from typing import cast
from uuid import uuid4

from fastapi import APIRouter, FastAPI, Request
from fastapi.exception_handlers import request_validation_exception_handler
from fastapi.exceptions import RequestValidationError
from fastapi.openapi.utils import get_openapi
from fastapi.responses import JSONResponse
from psycopg import OperationalError
from psycopg_pool import PoolTimeout

from d_test.agent_service.application.management import ManagementService
from d_test.agent_service.bootstrap.logging import (
    import_callable,
    initialize_logging,
)
from d_test.agent_service.domain.management import DomainError
from d_test.agent_service.runtime.management import ManagementRuntime
from d_test.agent_service.settings import Settings

from .body_limits import RunBodyLimit
from .dev_ui import install_dev_ui
from .routers import router
from .run_routes import router as run_router
from .security import HeaderIdentityProvider, IdentityProvider

logger = logging.getLogger("api_service")


@dataclass
class ApiRuntime:
    resources: ManagementRuntime
    identity: IdentityProvider

    @property
    def service(self) -> ManagementService:
        return self.resources.service


def build_identity(settings: Settings) -> IdentityProvider:
    if settings.auth_mode == "external":
        factory = import_callable(settings.identity_provider_factory or "")
        return cast(IdentityProvider, factory(settings))
    return HeaderIdentityProvider(settings)


def get_management_routers() -> list[APIRouter]:
    """Append to the template's get_routers(), without creating another app."""
    return [router, run_router]


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

    @app.middleware("http")
    async def trace_request(request: Request, call_next):
        if not (
            request.url.path.startswith("/api/v1/")
            or request.url.path in {"/health/live", "/health/ready"}
        ):
            return await call_next(request)
        request_id = str(uuid4())
        request.state.management_request_id = request_id
        started = monotonic()
        response = await call_next(request)
        response.headers["X-Request-Id"] = request_id
        response.headers["Cache-Control"] = "no-store"
        # Never log headers, bodies, URLs with query strings, or tokens.
        logger.info(
            "management_request component=api request_id=%s status=%s "
            "elapsed_ms=%.2f",
            request_id,
            response.status_code,
            (monotonic() - started) * 1000,
        )
        return response

    async def domain_error(request: Request, exc: Exception):
        error = cast(DomainError, exc)
        return JSONResponse(
            status_code=error.status,
            content={
                "code": error.code,
                "message": error.message,
                "request_id": getattr(
                    request.state, "management_request_id", None
                ),
            },
        )

    async def validation_error(request: Request, exc: Exception):
        error = cast(RequestValidationError, exc)
        if not hasattr(request.state, "management_request_id"):
            return await request_validation_exception_handler(request, error)
        return JSONResponse(
            status_code=422,
            content={
                "code": "VALIDATION_ERROR",
                "message": "요청 필드를 확인해 주세요.",
                "request_id": request.state.management_request_id,
                "errors": [
                    {"location": list(item["loc"]), "type": item["type"]}
                    for item in error.errors()
                ],
            },
        )

    async def database_error(request: Request, exc: Exception):
        if not hasattr(request.state, "management_request_id"):
            raise exc
        logger.warning(
            "management_database_unavailable type=%s", type(exc).__name__
        )
        return JSONResponse(
            status_code=503,
            content={
                "code": "DATABASE_UNAVAILABLE",
                "message": "일시적으로 요청을 처리할 수 없습니다.",
                "request_id": request.state.management_request_id,
            },
        )

    app.add_exception_handler(DomainError, domain_error)
    # Preserve template-wide validation handlers when already customized.
    existing = app.exception_handlers.get(RequestValidationError)
    if existing is None or existing is request_validation_exception_handler:
        app.add_exception_handler(RequestValidationError, validation_error)
    if OperationalError not in app.exception_handlers:
        app.add_exception_handler(OperationalError, database_error)
    if PoolTimeout not in app.exception_handlers:
        app.add_exception_handler(PoolTimeout, database_error)


def create_app(settings: Settings) -> FastAPI:
    app = FastAPI(title="Management API", version="1.0.0")
    install_management_api(app, settings, initialize_host_logging=True)

    @app.get("/health/live", include_in_schema=False)
    async def live():
        return {"status": "ok"}

    @app.get("/health/ready", include_in_schema=False)
    async def ready(request: Request):
        runtime = request.app.state.management_runtime
        try:
            await runtime.resources.ready()
        except RuntimeError:
            return JSONResponse({"status": "not_ready"}, status_code=503)
        return {"status": "ready"}

    return app
