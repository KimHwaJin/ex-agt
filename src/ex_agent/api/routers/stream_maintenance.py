from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from ex_agent.api.container import ApiContainer, api_container, current_user
from ex_agent.api.contracts import error_responses
from ex_agent.maintenance.contracts import (
    StreamMaintenanceJobView,
    StreamMaintenanceOperationResult,
    StreamMaintenancePage,
    StreamMaintenanceRequest,
)
from ex_agent.maintenance.operations import StreamMaintenanceForbidden
from ex_agent.maintenance.store import StreamMaintenanceConflict


def stream_maintenance_router() -> APIRouter:
    router = APIRouter(
        prefix="/api/v1/operations/stream-maintenance",
        responses=error_responses(401, 403, 404, 409, 422, 503),
    )

    @router.post(
        "/plans",
        response_model=StreamMaintenanceOperationResult,
        operation_id="planStreamMaintenance",
    )
    async def plan_stream_maintenance(
        body: StreamMaintenanceRequest,
        actor: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> StreamMaintenanceOperationResult:
        try:
            return await _operations(container).plan(
                actor=actor,
                request=body,
            )
        except StreamMaintenanceForbidden as error:
            raise HTTPException(status_code=403, detail=str(error)) from error
        except StreamMaintenanceConflict as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error

    @router.post(
        "/jobs",
        response_model=StreamMaintenanceOperationResult,
        status_code=202,
        operation_id="createStreamMaintenanceJob",
    )
    async def create_stream_maintenance_job(
        body: StreamMaintenanceRequest,
        actor: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> StreamMaintenanceOperationResult:
        try:
            return await _operations(container).submit_trim(
                actor=actor,
                request=body,
            )
        except StreamMaintenanceForbidden as error:
            raise HTTPException(status_code=403, detail=str(error)) from error
        except StreamMaintenanceConflict as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error

    @router.get(
        "/jobs",
        response_model=StreamMaintenancePage,
        operation_id="listStreamMaintenanceJobs",
    )
    async def stream_maintenance_jobs(
        cursor: Annotated[str | None, Query(max_length=2048)] = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        actor: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> StreamMaintenancePage:
        try:
            return await _operations(container).jobs(
                actor=actor,
                cursor=cursor,
                limit=limit,
            )
        except StreamMaintenanceForbidden as error:
            raise HTTPException(status_code=403, detail=str(error)) from error
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error

    @router.get(
        "/jobs/{job_id}",
        response_model=StreamMaintenanceJobView,
        operation_id="getStreamMaintenanceJob",
    )
    async def stream_maintenance_job(
        job_id: UUID,
        actor: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> StreamMaintenanceJobView:
        try:
            return await _operations(container).detail(job_id, actor=actor)
        except StreamMaintenanceForbidden as error:
            raise HTTPException(status_code=403, detail=str(error)) from error
        except LookupError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error

    return router


def _operations(container: ApiContainer):
    operations = container.stream_maintenance_operations
    if operations is None:
        raise HTTPException(
            status_code=503,
            detail="Stream maintenance runtime is unavailable",
        )
    return operations


__all__ = ["stream_maintenance_router"]
