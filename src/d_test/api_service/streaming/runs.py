"""Observe durable run events over SSE; disconnect never cancels work."""

import asyncio
import json
from time import monotonic
from uuid import UUID

from fastapi import Request
from fastapi.responses import StreamingResponse

from d_test.agent_service.domain.management import DomainError
from d_test.agent_service.domain.runs import TERMINAL

from ..dependencies.services import run_service as service


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
