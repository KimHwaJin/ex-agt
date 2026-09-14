"""Agent API transport; streaming observes durable runs, never drives them."""

import asyncio
import json
from time import monotonic
from typing import Annotated
from uuid import UUID

from fastapi import APIRouter, Header, Request, Response
from fastapi.responses import JSONResponse, StreamingResponse

from d_test.agent_service.application.runs import RunService
from d_test.agent_service.domain.management import DomainError, Page
from d_test.agent_service.domain.runs import (
    TERMINAL,
    Message,
    RunDetail,
    RunReceipt,
    RunRequest,
    RunSummary,
)

from .dependencies import CurrentUser, IdempotencyKey
from .routers import Cursor, Limit

router = APIRouter(prefix="/api/v1", tags=["agent"])
EventCursor = Annotated[
    str | None, Header(alias="Last-Event-ID", max_length=100)
]


def service(request: Request) -> RunService:
    return request.app.state.management_runtime.resources.runs


@router.get("/agent/models")
async def available_models(request: Request, user: CurrentUser):
    runs = service(request)
    # CurrentUser already resolves an active persisted identity.
    settings = runs.settings
    names = (
        settings.selectable_models
        if settings.agent_backend == "langgraph"
        else ()
    )
    return {
        "items": [{"name": name} for name in names],
        "default_model_name": settings.model_name if names else None,
        "source": "configured",
    }


async def stream_response(
    request: Request, owner: UUID, run_id: UUID, after: int
) -> StreamingResponse:
    runs = service(request)
    # Preflight before committing response headers: normal JSON errors.
    await runs.event_batch(owner, run_id, after)

    async def body():
        cursor = after
        start = monotonic()
        heartbeat = start
        try:
            while not await request.is_disconnected():
                events, status, watermark = await runs.event_batch(
                    owner, run_id, cursor
                )
                for event in events:
                    data = event.model_dump(mode="json", exclude={"sequence"})
                    encoded = json.dumps(data, ensure_ascii=False)
                    yield (
                        f"id: {event.event_id}\nevent: {event.type}\n"
                        f"data: {encoded}\n\n"
                    )
                    cursor = event.sequence
                if cursor >= watermark and (
                    status in TERMINAL or status == "awaiting_input"
                ):
                    return
                if monotonic() - start >= runs.settings.stream_max_seconds:
                    yield 'event: stream.closed\ndata: {"reconnect":true}\n\n'
                    return
                if monotonic() - heartbeat >= 10:
                    yield ": keepalive\n\n"
                    heartbeat = monotonic()
                if not events:
                    await asyncio.sleep(runs.settings.stream_poll_seconds)
        except asyncio.CancelledError:
            raise
        except Exception as error:
            payload = {
                "code": error.code
                if isinstance(error, DomainError)
                else "STREAM_UNAVAILABLE",
                "message": "연결 종료. 상태를 확인하고 재연결하세요.",
            }
            # A transport failure never changes the durable Run to failed.
            yield f"event: stream.error\ndata: {json.dumps(payload)}\n\n"

    return StreamingResponse(
        body(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-store",
            "X-Accel-Buffering": "no",
            "X-Run-Id": str(run_id),
        },
    )


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


@router.get("/sessions/{session_id}/messages", response_model=Page[Message])
async def list_messages(
    session_id: UUID,
    request: Request,
    user: CurrentUser,
    limit: Limit = 20,
    cursor: Cursor = None,
):
    return await service(request).listing(
        user.user_uuid, session_id, limit, cursor, messages=True
    )


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
