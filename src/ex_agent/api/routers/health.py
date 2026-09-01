from time import perf_counter

from fastapi import APIRouter, Depends
from fastapi.responses import JSONResponse, Response
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from ex_agent.api.container import ApiContainer, api_container
from ex_agent.config import Settings
from ex_agent.metrics import record_readiness, update_database_pool_metrics
from ex_agent.readiness import (
    DependencyStatus,
    ReadinessResult,
    probe_dependencies,
)


def health_router(settings: Settings) -> APIRouter:
    router = APIRouter()

    @router.get("/health/live", include_in_schema=False)
    @router.get("/healthz")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @router.get("/health/ready", include_in_schema=False)
    @router.get("/readyz", include_in_schema=False)
    async def readiness(
        container: ApiContainer = Depends(api_container),
    ) -> JSONResponse:
        result = await probe_dependencies(
            container.engine,
            container.redis,
            timeout_seconds=settings.readiness_probe_timeout_seconds,
        )
        if container.runtime_lifecycle is not None:
            started = perf_counter()
            runtime_ready = await container.runtime_lifecycle.ready()
            result = ReadinessResult(
                checks={
                    **result.checks,
                    "agent_runtime": DependencyStatus(
                        ready=runtime_ready,
                        latency_seconds=perf_counter() - started,
                        error=None if runtime_ready else "NotRunning",
                    ),
                },
                checked_at_epoch_seconds=result.checked_at_epoch_seconds,
            )
        record_readiness("api", result)
        payload = result.payload()
        return JSONResponse(
            content=payload,
            status_code=200 if result.ready else 503,
        )

    @router.get("/metrics", include_in_schema=False)
    async def metrics(
        container: ApiContainer = Depends(api_container),
    ) -> Response:
        update_database_pool_metrics("api", container.engine)
        return Response(
            content=generate_latest(),
            headers={"Content-Type": CONTENT_TYPE_LATEST},
        )

    return router


__all__ = ["health_router"]
