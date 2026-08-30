from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query

from ex_agent.api.container import ApiContainer, api_container, current_user
from ex_agent.application.promotions import (
    WorkflowPromotionForbiddenError,
    WorkflowPromotionNotEligibleError,
)
from ex_agent.application.workflow_lifecycle import (
    WorkflowLifecycleForbiddenError,
)
from ex_agent.domain.contracts import (
    WorkflowLifecycleActionPage,
    WorkflowLifecycleResult,
    WorkflowOperationsView,
    WorkflowStatusRequest,
    WorkflowVersionActivationRequest,
    WorkflowVersionCreateRequest,
    WorkflowVersionDetail,
    WorkflowVersionPage,
    WorkflowVersionReviewRequest,
)
from ex_agent.persistence.repositories.workflow_lifecycle import (
    WorkflowLifecycleConflictError,
)


def workflow_router() -> APIRouter:
    router = APIRouter(prefix="/api/v1/workflows")

    @router.get(
        "/{workflow_id}",
        response_model=WorkflowOperationsView,
    )
    async def workflow_overview(
        workflow_id: UUID,
        user_id: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> WorkflowOperationsView:
        return await _translate_errors(
            lambda: container.workflow_lifecycle.overview(
                workflow_id,
                actor_user_id=user_id,
            )
        )

    @router.get(
        "/{workflow_id}/versions",
        response_model=WorkflowVersionPage,
    )
    async def workflow_versions(
        workflow_id: UUID,
        cursor: Annotated[str | None, Query(max_length=2048)] = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        user_id: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> WorkflowVersionPage:
        return await _translate_errors(
            lambda: container.workflow_lifecycle.versions(
                workflow_id,
                actor_user_id=user_id,
                cursor=cursor,
                limit=limit,
            )
        )

    @router.get(
        "/{workflow_id}/versions/{workflow_version_id}",
        response_model=WorkflowVersionDetail,
    )
    async def workflow_version_detail(
        workflow_id: UUID,
        workflow_version_id: UUID,
        user_id: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> WorkflowVersionDetail:
        return await _translate_errors(
            lambda: container.workflow_lifecycle.version_detail(
                workflow_id,
                workflow_version_id,
                actor_user_id=user_id,
            )
        )

    @router.get(
        "/{workflow_id}/lifecycle-actions",
        response_model=WorkflowLifecycleActionPage,
    )
    async def workflow_lifecycle_actions(
        workflow_id: UUID,
        cursor: Annotated[str | None, Query(max_length=2048)] = None,
        limit: Annotated[int, Query(ge=1, le=100)] = 50,
        user_id: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> WorkflowLifecycleActionPage:
        return await _translate_errors(
            lambda: container.workflow_lifecycle.actions(
                workflow_id,
                actor_user_id=user_id,
                cursor=cursor,
                limit=limit,
            )
        )

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


async def _translate_errors[ResponseT](
    operation: Callable[[], Awaitable[ResponseT]],
) -> ResponseT:
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
