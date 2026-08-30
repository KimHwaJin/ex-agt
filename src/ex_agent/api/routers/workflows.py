from __future__ import annotations

from collections.abc import Awaitable, Callable
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException

from ex_agent.api.container import ApiContainer, api_container, current_user
from ex_agent.application.promotions import (
    WorkflowPromotionForbiddenError,
    WorkflowPromotionNotEligibleError,
)
from ex_agent.application.workflow_lifecycle import (
    WorkflowLifecycleForbiddenError,
)
from ex_agent.domain.contracts import (
    WorkflowLifecycleResult,
    WorkflowStatusRequest,
    WorkflowVersionActivationRequest,
    WorkflowVersionCreateRequest,
    WorkflowVersionReviewRequest,
)
from ex_agent.persistence.repositories.workflow_lifecycle import (
    WorkflowLifecycleConflictError,
)


def workflow_router() -> APIRouter:
    router = APIRouter(prefix="/api/v1/workflows")

    @router.post(
        "/{workflow_id}/versions",
        response_model=WorkflowLifecycleResult,
        status_code=201,
    )
    async def create_version(
        workflow_id: UUID,
        body: WorkflowVersionCreateRequest,
        user_id: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> WorkflowLifecycleResult:
        return await _translate_errors(
            lambda: container.workflow_lifecycle.create_version(
                workflow_id,
                actor_user_id=user_id,
                request=body,
            )
        )

    @router.post(
        "/{workflow_id}/versions/{workflow_version_id}/reviews",
        response_model=WorkflowLifecycleResult,
    )
    async def review_version(
        workflow_id: UUID,
        workflow_version_id: UUID,
        body: WorkflowVersionReviewRequest,
        user_id: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> WorkflowLifecycleResult:
        return await _translate_errors(
            lambda: container.workflow_lifecycle.review_version(
                workflow_id,
                workflow_version_id,
                actor_user_id=user_id,
                request=body,
            )
        )

    @router.post(
        "/{workflow_id}/versions/{workflow_version_id}/activate",
        response_model=WorkflowLifecycleResult,
    )
    async def activate_version(
        workflow_id: UUID,
        workflow_version_id: UUID,
        body: WorkflowVersionActivationRequest,
        user_id: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> WorkflowLifecycleResult:
        return await _translate_errors(
            lambda: container.workflow_lifecycle.activate_version(
                workflow_id,
                workflow_version_id,
                actor_user_id=user_id,
                request=body,
            )
        )

    @router.post(
        "/{workflow_id}/status",
        response_model=WorkflowLifecycleResult,
    )
    async def update_status(
        workflow_id: UUID,
        body: WorkflowStatusRequest,
        user_id: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> WorkflowLifecycleResult:
        return await _translate_errors(
            lambda: container.workflow_lifecycle.update_status(
                workflow_id,
                actor_user_id=user_id,
                request=body,
            )
        )

    return router


async def _translate_errors(
    operation: Callable[[], Awaitable[WorkflowLifecycleResult]],
) -> WorkflowLifecycleResult:
    try:
        return await operation()
    except LookupError as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except (
        WorkflowLifecycleForbiddenError,
        WorkflowPromotionForbiddenError,
    ) as error:
        raise HTTPException(status_code=403, detail=str(error)) from error
    except (
        WorkflowLifecycleConflictError,
        WorkflowPromotionNotEligibleError,
    ) as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    except ValueError as error:
        raise HTTPException(status_code=422, detail=str(error)) from error


__all__ = ["workflow_router"]
