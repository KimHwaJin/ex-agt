"""Run submission, detail, cancellation and stream endpoints."""

from uuid import UUID

from fastapi import APIRouter, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

from d_test.agent_service.application.runs import RunService

from ..dependencies import CurrentUser, IdempotencyKey
from ..dependencies.parameters import Cursor, EventCursor, Limit
from ..dependencies.services import run_service as service
from ..schemas.common import Page
from ..schemas.runs import RunDetail, RunReceipt, RunRequest, RunSummary
from ..streaming.runs import stream_response

router = APIRouter(prefix="/api/v1", tags=["agent"])


@router.post(
    "/agent/runs",
    response_model=RunReceipt,
    status_code=202,
    responses={
        200: {
            "content": {
                "text/event-stream": {
                    "schema": {"type": "string"},
                }
            },
            "description": "Durable run event stream",
        }
    },
)
async def submit_run(
    payload: RunRequest,
    request: Request,
    user: CurrentUser,
    key: IdempotencyKey,
) -> Response:
    receipt, after = await service(request).submit(
        user.user_uuid, key, payload
    )
    if payload.stream:
        return await stream_response(
            request, user.user_uuid, receipt.run_id, after
        )
    return JSONResponse(receipt.model_dump(mode="json"), status_code=202)


@router.get("/agent/runs/{run_id}", response_model=RunDetail)
async def get_run(run_id: UUID, request: Request, user: CurrentUser):
    return await service(request).detail(user.user_uuid, run_id)


@router.get(
    "/agent/runs/{run_id}/stream",
    response_class=StreamingResponse,
    responses={
        200: {
            "content": {
                "text/event-stream": {
                    "schema": {"type": "string"},
                }
            }
        }
    },
)
async def get_stream(
    run_id: UUID,
    request: Request,
    user: CurrentUser,
    last_event_id: EventCursor = None,
):
    after = RunService.parse_event_id(run_id, last_event_id)
    return await stream_response(request, user.user_uuid, run_id, after)


@router.post(
    "/agent/runs/{run_id}/cancel",
    response_model=RunReceipt,
    responses={202: {"model": RunReceipt}},
)
async def cancel_run(run_id: UUID, request: Request, user: CurrentUser):
    receipt = await service(request).cancel(user.user_uuid, run_id)
    code = 202 if receipt.status == "cancelling" else 200
    return JSONResponse(receipt.model_dump(mode="json"), status_code=code)


@router.get("/sessions/{session_id}/runs", response_model=Page[RunSummary])
async def list_runs(
    session_id: UUID,
    request: Request,
    user: CurrentUser,
    limit: Limit = 20,
    cursor: Cursor = None,
):
    return await service(request).listing(
        user.user_uuid, session_id, limit, cursor, messages=False
    )
