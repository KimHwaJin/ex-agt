from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

from fastapi.testclient import TestClient

from agent.failure.contracts import (
    FailureCleanupPage,
    FailureCleanupView,
    FailureOperationResult,
)
from ex_agent.api.app import create_app
from ex_agent.api.container import api_container, current_user
from ex_agent.config import Settings


def view() -> FailureCleanupView:
    now = datetime.now(UTC)
    return FailureCleanupView(
        task_id=uuid4(),
        session_id="session-1",
        state="BLOCKED",
        version=3,
        reason="handler failed",
        source={"kind": "API"},
        attempts=20,
        next_attempt_at=now,
        last_error="model unavailable",
        execution_id=None,
        executor_status=None,
        evidence_complete=False,
        preserve_terminal=False,
        final_status="FAILED",
        message=None,
        last_operation_id=None,
        last_operation_action=None,
        last_operation_reason=None,
        last_operation_at=None,
        last_operation_by=None,
        created_at=now,
        updated_at=now,
        created_by="AGENT",
        updated_by="AGENT",
    )


def test_failure_operation_routes_are_connected_to_runtime() -> None:
    cleanup = view()
    result = FailureOperationResult(
        cleanup=cleanup,
        operation_replayed=False,
    )
    operations = SimpleNamespace(
        blocked=AsyncMock(
            return_value=FailureCleanupPage(
                items=[cleanup],
                has_more=False,
            )
        ),
        detail=AsyncMock(return_value=cleanup),
        retry=AsyncMock(return_value=result),
        finalize=AsyncMock(return_value=result),
    )
    container = SimpleNamespace(failure_operations=operations)
    app = create_app(Settings(), start_runtime=False)
    app.dependency_overrides[api_container] = lambda: container
    app.dependency_overrides[current_user] = lambda: "operator-1"
    body = {
        "idempotency_key": "operation-1",
        "expected_version": 3,
        "reason": "Operator reviewed evidence",
    }

    with TestClient(app) as client:
        listed = client.get("/api/v1/operations/failure-cleanups")
        detail = client.get(
            f"/api/v1/operations/failure-cleanups/{cleanup.task_id}"
        )
        retried = client.post(
            f"/api/v1/operations/failure-cleanups/{cleanup.task_id}/retry",
            json=body,
        )
        finalized = client.post(
            f"/api/v1/operations/failure-cleanups/{cleanup.task_id}/finalize",
            json={**body, "idempotency_key": "operation-2"},
        )

    assert listed.status_code == detail.status_code == 200
    assert retried.status_code == 202
    assert finalized.status_code == 200
    operations.blocked.assert_awaited_once_with(
        actor="operator-1",
        cursor=None,
        limit=50,
    )
    operations.retry.assert_awaited_once()
    operations.finalize.assert_awaited_once()


def test_failure_operations_are_unavailable_without_agent_runtime() -> None:
    app = create_app(Settings(), start_runtime=False)
    container = SimpleNamespace(failure_operations=None)
    app.dependency_overrides[api_container] = lambda: container
    app.dependency_overrides[current_user] = lambda: "operator-1"

    with TestClient(app) as client:
        response = client.get("/api/v1/operations/failure-cleanups")

    assert response.status_code == 503
