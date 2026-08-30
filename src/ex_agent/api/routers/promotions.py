from __future__ import annotations

from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException

from ex_agent.api.container import ApiContainer, api_container, current_user
from ex_agent.api.routers.tasks import owned_task
from ex_agent.application.promotions import (
    WorkflowPromotionForbiddenError,
    WorkflowPromotionNotEligibleError,
)
from ex_agent.domain.contracts import (
    WorkflowPromotionDraft,
    WorkflowPromotionRequest,
    WorkflowPromotionResult,
)
from ex_agent.persistence.repositories.promotions import (
    WorkflowPromotionConflictError,
)


def promotion_router() -> APIRouter:
    router = APIRouter(prefix="/api/v1")

    @router.get(
        "/tasks/{task_id}/workflow-promotion-draft",
        response_model=WorkflowPromotionDraft,
    )
    async def promotion_draft(
        task_id: UUID,
        user_id: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> WorkflowPromotionDraft:
        await owned_task(container, task_id, user_id)
        try:
            return await container.promotions.draft(
                task_id,
                actor_user_id=user_id,
            )
        except WorkflowPromotionNotEligibleError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except WorkflowPromotionForbiddenError as error:
            raise HTTPException(status_code=403, detail=str(error)) from error

    @router.post(
        "/tasks/{task_id}/workflow-promotions",
        response_model=WorkflowPromotionResult,
        status_code=201,
    )
    async def promote_workflow(
        task_id: UUID,
        body: WorkflowPromotionRequest,
        user_id: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> WorkflowPromotionResult:
        await owned_task(container, task_id, user_id)
        try:
            return await container.promotions.promote(
                task_id,
                actor_user_id=user_id,
                request=body,
            )
        except WorkflowPromotionNotEligibleError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except WorkflowPromotionForbiddenError as error:
            raise HTTPException(status_code=403, detail=str(error)) from error
        except WorkflowPromotionConflictError as error:
            raise HTTPException(status_code=409, detail=str(error)) from error
        except ValueError as error:
            raise HTTPException(status_code=422, detail=str(error)) from error

    return router


__all__ = ["promotion_router"]
