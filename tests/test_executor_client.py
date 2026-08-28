from uuid import uuid4

import httpx
import pytest
import respx

from ex_agent.executor.client import ExecutorClient, ExecutorRequestError


def _event(
    execution_id: str,
    sequence: int,
    event_type: str,
) -> dict[str, object]:
    return {
        "event_id": str(uuid4()),
        "event_type": event_type,
        "schema_version": "1.0",
        "execution_id": execution_id,
        "event_sequence": sequence,
        "payload": {},
        "occurred_at": "2026-08-27T00:00:00Z",
    }


@pytest.mark.asyncio
@respx.mock
async def test_submit_uses_executor_v1_contract() -> None:
    execution_id = uuid4()
    operation_id = uuid4()
    route = respx.post("http://executor/api/v1/executions").mock(
        return_value=httpx.Response(
            202,
            json={
                "execution_id": str(execution_id),
                "operation": {
                    "operation_id": str(operation_id),
                    "steps": [],
                },
                "state": {"status": "QUEUED", "version": 1},
            },
        )
    )
    client = ExecutorClient(
        "http://executor/api/v1",
        timeout_seconds=1,
    )
    try:
        receipt = await client.submit(
            idempotency_key="task:one:submit:1",
            mode="MULTI",
            wait_timeout_seconds=600,
            runtime_profile="basic",
            user_id="user-1",
            project_id="project-1",
            session_id="session-1",
            task_id="task-1",
            workflow_id=None,
            steps=[
                {
                    "sequence": 0,
                    "payload": {
                        "type": "PYTHON_EXECUTE",
                        "source": {
                            "type": "PATH",
                            "path": "task-1/1/step-0000.py",
                            "sha256": "a" * 64,
                        },
                    },
                }
            ],
        )
    finally:
        await client.close()

    assert receipt.execution_id == execution_id
    assert receipt.operation is not None
    assert receipt.operation.operation_id == operation_id
    request = route.calls.last.request
    body = request.content.decode()
    assert '"operation_mode":"MULTI"' in body
    assert '"operation_wait_timeout_seconds":600' in body
    assert '"type":"PATH"' in body
    assert '"content"' not in body


@pytest.mark.asyncio
@respx.mock
async def test_submit_rejects_inline_step_source() -> None:
    route = respx.post("http://executor/api/v1/executions")
    client = ExecutorClient(
        "http://executor/api/v1",
        timeout_seconds=1,
    )
    try:
        with pytest.raises(ValueError, match="must use PATH"):
            await client.submit(
                idempotency_key="task:inline:submit:1",
                mode="SINGLE",
                wait_timeout_seconds=600,
                runtime_profile="basic",
                user_id="user-1",
                project_id="project-1",
                session_id="session-1",
                task_id="task-inline",
                workflow_id=None,
                steps=[
                    {
                        "sequence": 0,
                        "payload": {
                            "type": "PYTHON_EXECUTE",
                            "source": {
                                "type": "INLINE",
                                "content": "print(1)",
                            },
                        },
                    }
                ],
            )
    finally:
        await client.close()

    assert not route.called


@pytest.mark.asyncio
@respx.mock
async def test_report_uses_path_source() -> None:
    execution_id = uuid4()
    artifact_id = uuid4()
    route = respx.post(
        f"http://executor/api/v1/executions/{execution_id}/artifacts"
    ).mock(
        return_value=httpx.Response(
            201,
            json={"artifact_id": str(artifact_id)},
        )
    )
    client = ExecutorClient(
        "http://executor/api/v1",
        timeout_seconds=1,
    )
    try:
        response = await client.materialize_report(
            execution_id,
            idempotency_key="task:one:report",
            path="task-1/reports/analysis-report.md",
            sha256="b" * 64,
        )
    finally:
        await client.close()

    assert response.artifact_id == artifact_id
    body = route.calls.last.request.content.decode()
    assert '"type":"PATH"' in body
    assert '"path":"task-1/reports/analysis-report.md"' in body
    assert '"sha256":"' + "b" * 64 + '"' in body
    assert '"content"' not in body


@pytest.mark.asyncio
@respx.mock
async def test_event_history_follows_executor_pagination() -> None:
    execution_id = uuid4()
    route = respx.get(
        f"http://executor/api/v1/executions/{execution_id}/events"
    ).mock(
        side_effect=[
            httpx.Response(
                200,
                json={
                    "items": [
                        _event(str(execution_id), 2, "execution.started")
                    ],
                    "next_cursor": "cursor-2",
                    "has_more": True,
                },
            ),
            httpx.Response(
                200,
                json={
                    "items": [
                        _event(
                            str(execution_id),
                            3,
                            "execution.operation_started",
                        )
                    ],
                    "next_cursor": None,
                    "has_more": False,
                },
            ),
        ]
    )
    client = ExecutorClient(
        "http://executor/api/v1",
        timeout_seconds=1,
    )
    try:
        events = await client.events_after(
            execution_id,
            after_sequence=1,
        )
    finally:
        await client.close()

    assert [event.event_sequence for event in events] == [2, 3]
    assert route.call_count == 2
    assert route.calls[0].request.url.params["after_sequence"] == "1"
    assert route.calls[1].request.url.params["cursor"] == "cursor-2"


@pytest.mark.asyncio
@respx.mock
async def test_executor_error_preserves_safe_response_detail() -> None:
    execution_id = uuid4()
    respx.get(f"http://executor/api/v1/executions/{execution_id}/result").mock(
        return_value=httpx.Response(
            422,
            json={
                "error": {
                    "code": "INVALID_EXECUTION_SPEC",
                    "message": "PATH source does not exist.",
                }
            },
        )
    )
    client = ExecutorClient(
        "http://executor/api/v1",
        timeout_seconds=1,
    )
    try:
        with pytest.raises(ExecutorRequestError) as captured:
            await client.result(execution_id)
    finally:
        await client.close()

    assert captured.value.status_code == 422
    assert "INVALID_EXECUTION_SPEC" in captured.value.detail
