from __future__ import annotations

import json
from collections.abc import AsyncIterator
from typing import Any
from uuid import NAMESPACE_URL, UUID, uuid5

from fastapi import APIRouter, Depends, Header, HTTPException, Request
from fastapi.responses import StreamingResponse
from pydantic import TypeAdapter

from agent.admission.contracts import ApiRequest, RequestRecord
from agent.graph.state import TaskTurn
from agent.session import SessionBusyError, SessionConflictError
from ex_agent.api.container import (
    ApiContainer,
    api_container,
    current_user,
)
from ex_agent.api.contracts import (
    CancelRequest,
    ResumeRequest,
    TaskAcceptedResponse,
    TaskCreateRequest,
    TaskResponse,
    error_responses,
)
from ex_agent.config import Settings
from ex_agent.domain.contracts import ResumeSignal
from ex_agent.domain.enums import ResumeSignalType
from ex_agent.metrics import SSE_CONNECTIONS
from ex_agent.transport.streams import task_event_channel

_resume_adapter = TypeAdapter(ResumeSignal)


def task_router(settings: Settings) -> APIRouter:
    router = APIRouter(
        prefix="/api/v1",
        responses=error_responses(401, 404, 409, 422, 423, 503),
    )

    @router.post(
        "/projects/{project_id}/sessions/{session_id}/tasks",
        response_model=TaskAcceptedResponse,
        status_code=202,
        operation_id="createTask",
    )
    async def create_task(
        project_id: str,
        session_id: str,
        body: TaskCreateRequest,
        user_id: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> TaskAcceptedResponse:
        command = ApiRequest(
            request_id=_request_id(body.idempotency_key),
            kind="START",
            turn=TaskTurn(
                user_id=user_id,
                project_id=project_id,
                session_id=session_id,
                active_task_id=str(body.task_id),
                current_input_message_id=str(body.input_message_id),
                user_message=body.content,
                runtime_profile=container.settings.executor_runtime_profile,
            ),
        )
        await admit(container, command)
        task = await required_task(container, body.task_id)
        return task_accepted_response(task)

    @router.get(
        "/tasks/{task_id}",
        response_model=TaskResponse,
        operation_id="getTask",
    )
    async def get_task(
        task_id: UUID,
        user_id: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> TaskResponse:
        task = await owned_task(container, task_id, user_id)
        return task_response(task)

    @router.post(
        "/tasks/{task_id}/resume",
        response_model=TaskAcceptedResponse,
        status_code=202,
        operation_id="resumeTask",
    )
    async def resume_task(
        task_id: UUID,
        body: ResumeRequest,
        user_id: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> TaskAcceptedResponse:
        task = await owned_task(container, task_id, user_id)
        signal = _resume_adapter.validate_python(body.signal)
        payload = signal.model_dump(mode="json")
        validate_signal_against_interrupt(task.current_interrupt, payload)
        await admit(
            container,
            ApiRequest(
                request_id=_request_id(body.idempotency_key),
                kind="RESUME",
                turn=task_turn(
                    task,
                    container.settings.executor_runtime_profile,
                ),
                interrupt_id=required_interrupt_id(task.current_interrupt),
                payload=payload,
            ),
        )
        task = await required_task(container, task_id)
        return task_accepted_response(task)

    @router.post(
        "/tasks/{task_id}/cancel",
        response_model=TaskAcceptedResponse,
        status_code=202,
        operation_id="cancelTask",
    )
    async def cancel_task(
        task_id: UUID,
        body: CancelRequest,
        user_id: str = Depends(current_user),
        container: ApiContainer = Depends(api_container),
    ) -> TaskAcceptedResponse:
        task = await owned_task(container, task_id, user_id)
        if task.execution_id is None:
            raise HTTPException(
                status_code=409,
                detail="Task has no active Executor execution",
            )
        signal = {
            "type": ResumeSignalType.CANCEL_REQUESTED.value,
            "task_id": str(task_id),
            "reason": body.reason,
        }
        await admit(
            container,
            ApiRequest(
                request_id=_request_id(body.idempotency_key),
                kind="CANCEL",
                turn=task_turn(
                    task,
                    container.settings.executor_runtime_profile,
                ),
                interrupt_id=required_interrupt_id(task.current_interrupt),
                payload=signal,
            ),
        )
        task = await required_task(container, task_id)
        return task_accepted_response(task)

    @router.get(
        "/tasks/{task_id}/events",
        operation_id="streamTaskEvents",
    )
    async def task_events(
        task_id: UUID,
        request: Request,
        user_id: str = Depends(current_user),
        last_event_id: str | None = Header(
            default=None,
            alias="Last-Event-ID",
        ),
        container: ApiContainer = Depends(api_container),
    ) -> StreamingResponse:
        await owned_task(container, task_id, user_id)
        cursor = int(last_event_id or 0)

        async def stream() -> AsyncIterator[str]:
            nonlocal cursor
            channel = task_event_channel(settings, task_id)
            SSE_CONNECTIONS.inc()
            try:
                async with container.redis.pubsub() as pubsub:
                    await pubsub.subscribe(channel)
                    while not await request.is_disconnected():
                        events = await container.repository.events_after(
                            task_id,
                            cursor,
                        )
                        if events:
                            for event in events:
                                cursor = event.id
                                data = json.dumps(
                                    event.payload,
                                    ensure_ascii=False,
                                )
                                yield (
                                    f"id: {event.id}\n"
                                    f"event: {event.event_type}\n"
                                    f"data: {data}\n\n"
                                )
                            continue
                        notification = await pubsub.get_message(
                            ignore_subscribe_messages=True,
                            timeout=settings.sse_heartbeat_seconds,
                        )
                        if notification is None:
                            yield ": keepalive\n\n"
            finally:
                SSE_CONNECTIONS.dec()

        return StreamingResponse(
            stream(),
            media_type="text/event-stream",
            headers={"Cache-Control": "no-cache"},
        )

    return router


async def admit(container: ApiContainer, command: ApiRequest) -> RequestRecord:
    if container.admission is None:
        raise HTTPException(
            status_code=503,
            detail="Direct Agent runtime is unavailable",
        )
    try:
        record = await container.admission.handle(command)
    except SessionBusyError as error:
        raise HTTPException(
            status_code=423,
            detail={"active_task_id": error.active_task_id},
        ) from error
    except SessionConflictError as error:
        raise HTTPException(status_code=409, detail=str(error)) from error
    if record.state in {"REJECTED", "COMPENSATED"}:
        raise HTTPException(
            status_code=409,
            detail=record.last_error or "Agent request was rejected",
        )
    if record.state == "BLOCKED":
        raise HTTPException(
            status_code=503,
            detail=record.last_error or "Agent request recovery is blocked",
        )
    return record


async def owned_task(
    container: ApiContainer,
    task_id: UUID,
    user_id: str,
) -> Any:
    task = await container.repository.get_task(task_id)
    if task is None or task.user_id != user_id:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


async def required_task(container: ApiContainer, task_id: UUID) -> Any:
    task = await container.repository.get_task(task_id)
    if task is None:
        raise HTTPException(status_code=404, detail="Task not found")
    return task


def task_turn(task: Any, runtime_profile: str = "basic") -> TaskTurn:
    return TaskTurn(
        user_id=task.user_id,
        project_id=task.project_id,
        session_id=task.session_id,
        active_task_id=str(task.id),
        current_input_message_id=str(task.input_message_id),
        user_message=task.user_message,
        runtime_profile=runtime_profile,
    )


def _request_id(idempotency_key: str) -> UUID:
    return uuid5(NAMESPACE_URL, f"ex-agent/api/{idempotency_key}")


def required_interrupt_id(interrupt: dict[str, Any] | None) -> str:
    if interrupt is None:
        raise HTTPException(
            status_code=409,
            detail="Task is not awaiting input",
        )
    interrupt_id = interrupt.get("interrupt_id")
    if not isinstance(interrupt_id, str) or not interrupt_id:
        raise HTTPException(
            status_code=409,
            detail="Task interrupt is not resumable",
        )
    return interrupt_id


def task_response(task: Any) -> TaskResponse:
    return TaskResponse(
        task_id=task.id,
        user_id=task.user_id,
        project_id=task.project_id,
        session_id=task.session_id,
        status=task.status,
        execution_id=task.execution_id,
        current_interrupt=task.current_interrupt,
        terminal_message=task.terminal_message,
        version=task.version,
        created_at=task.created_at,
        updated_at=task.updated_at,
        created_by=task.created_by,
        updated_by=task.updated_by,
    )


def task_accepted_response(task: Any) -> TaskAcceptedResponse:
    return TaskAcceptedResponse(
        task_id=task.id,
        status=task.status,
        created_at=task.created_at,
        updated_at=task.updated_at,
        created_by=task.created_by,
        updated_by=task.updated_by,
    )


def validate_signal_against_interrupt(
    interrupt: dict[str, Any] | None,
    signal: dict[str, Any],
) -> None:
    if interrupt is None:
        raise HTTPException(
            status_code=409, detail="Task is not awaiting input"
        )
    if interrupt.get("kind") != signal.get("type"):
        raise HTTPException(
            status_code=409,
            detail="Resume signal does not match the current interrupt",
        )
    if signal.get("type") == ResumeSignalType.PLAN_REVIEW.value:
        expected = (
            "plan_revision_id",
            "plan_revision_number",
            "public_payload_hash",
        )
        if any(interrupt.get(key) != signal.get(key) for key in expected):
            raise HTTPException(
                status_code=409,
                detail="Plan decision is stale",
            )
    if signal.get("type") == ResumeSignalType.WORKFLOW_SELECTION.value:
        if interrupt.get("proposal_version") != signal.get("proposal_version"):
            raise HTTPException(
                status_code=409,
                detail="Workflow proposal is stale",
            )


__all__ = [
    "admit",
    "owned_task",
    "required_interrupt_id",
    "task_accepted_response",
    "task_response",
    "task_router",
    "task_turn",
    "validate_signal_against_interrupt",
]
