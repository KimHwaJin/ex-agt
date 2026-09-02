"""Authenticated operations for failure cleanup that stopped safely."""

from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from agent.failure.contracts import (
    FailureCleanupPage,
    FailureCleanupView,
    FailureOperationInput,
    FailureOperationResult,
)
from agent.failure.operations import (
    FailureOperationConflict,
    FailureOperationsForbidden,
)
from ex_agent.api.container import ApiContainer, api_container, current_user
from ex_agent.api.contracts import error_responses


def failure_router() -> APIRouter:
    router = APIRouter(
        prefix="/api/v1/operations/failure-cleanups",
        responses=error_responses(401, 403, 404, 409, 422, 503),
    )

    @router.get(
        "",
        response_model=FailureCleanupPage,
        operation_id="listBlockedFailureCleanups",
    )
    async def blocked_failures(
        cursor: Annotated[str | None, Query(max_length=2048)] = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        actor: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> FailureCleanupPage:
        operations = _operations(container)
        try:
            return await operations.blocked(
                actor=actor,
                cursor=cursor,
                limit=limit,
            )
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error
        except FailureOperationsForbidden as error:
            raise HTTPException(status_code=403, detail=str(error)) from error

    @router.get(
        "/{task_id}",
        response_model=FailureCleanupView,
        operation_id="getFailureCleanup",
    )
    async def failure_detail(
        task_id: UUID,
        actor: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> FailureCleanupView:
        operations = _operations(container)
        try:
            return await operations.detail(task_id, actor=actor)
        except FailureOperationsForbidden as error:
            raise HTTPException(status_code=403, detail=str(error)) from error
        except LookupError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error

    @router.post(
        "/{task_id}/retry",
        response_model=FailureOperationResult,
        status_code=202,
        operation_id="retryFailureCleanup",
    )
    async def retry_failure(
        task_id: UUID,
        body: FailureOperationInput,
        actor: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> FailureOperationResult:
        operations = _operations(container)
        try:
            return await operations.retry(
                task_id,
                actor=actor,
                request=body,
            )
        except FailureOperationsForbidden as error:
            raise HTTPException(status_code=403, detail=str(error)) from error
        except LookupError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except FailureOperationConflict as error:
            raise HTTPException(status_code=409, detail=str(error)) from error

    @router.post(
        "/{task_id}/finalize",
        response_model=FailureOperationResult,
        operation_id="finalizeFailureCleanup",
    )
    async def finalize_failure(
        task_id: UUID,
        body: FailureOperationInput,
        actor: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> FailureOperationResult:
        operations = _operations(container)
        try:
            return await operations.finalize(
                task_id,
                actor=actor,
                request=body,
            )
        except FailureOperationsForbidden as error:
            raise HTTPException(status_code=403, detail=str(error)) from error
        except LookupError as error:
            raise HTTPException(status_code=404, detail=str(error)) from error
        except FailureOperationConflict as error:
            raise HTTPException(status_code=409, detail=str(error)) from error

    return router


def _operations(container: ApiContainer):
    if container.failure_operations is None:
        raise HTTPException(
            status_code=503,
            detail="Failure operations runtime is unavailable",
        )
    return container.failure_operations


__all__ = ["failure_router"]
