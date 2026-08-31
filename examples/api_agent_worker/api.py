from __future__ import annotations

from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, ConfigDict, Field

from examples.api_agent_worker.ports import (
    ApiAdmission,
    BoundaryNotReadyError,
    RunBusyError,
    RunGuard,
)
from examples.api_agent_worker.runner import SharedGraphRunner


class StartRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: UUID
    objective: str = Field(min_length=1, max_length=4000)


class ReviewRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")
    request_id: UUID
    approved: bool


def create_router(
    runner: SharedGraphRunner,
    guard: RunGuard,
    admit: ApiAdmission,
) -> APIRouter:
    """Mount behind the host BFF authentication boundary; not a public API.

    Task ownership, durable input logging and session locking are required
    via admit(), not omitted by silently trusting this forwarded header.
    The small responses are demo projections, not the production API schema.
    """
    router = APIRouter(prefix="/handoff/tasks")

    @router.post("/{task_id}/start")
    async def start(
        task_id: UUID,
        body: StartRequest,
        x_user_id: Annotated[str, Header(min_length=1)],
    ) -> dict:
        try:
            async with guard.hold(task_id):
                await admit(
                    task_id, x_user_id, "START", body.model_dump(mode="json")
                )
                await runner.start(task_id, body.objective)
                return await runner.view(task_id)
        except (RunBusyError, BoundaryNotReadyError) as error:
            raise HTTPException(409, str(error)) from error
        except ValueError as error:
            raise HTTPException(409, str(error)) from error

    @router.post("/{task_id}/review")
    async def review(
        task_id: UUID,
        body: ReviewRequest,
        x_user_id: Annotated[str, Header(min_length=1)],
    ) -> dict:
        try:
            async with guard.hold(task_id):
                await admit(
                    task_id, x_user_id, "REVIEW", body.model_dump(mode="json")
                )
                await runner.review(task_id, body.request_id, body.approved)
                return await runner.view(task_id)
        except (RunBusyError, BoundaryNotReadyError) as error:
            raise HTTPException(409, str(error)) from error
        except ValueError as error:
            raise HTTPException(409, str(error)) from error

    return router
