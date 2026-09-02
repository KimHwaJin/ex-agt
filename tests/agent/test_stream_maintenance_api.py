from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

from fastapi.testclient import TestClient

from ex_agent.api.app import create_app
from ex_agent.api.container import api_container, current_user
from ex_agent.config import Settings
from ex_agent.maintenance.contracts import (
    StreamMaintenanceJobView,
    StreamMaintenanceOperationResult,
    StreamMaintenancePage,
)


def view() -> StreamMaintenanceJobView:
    now = datetime.now(UTC)
    return StreamMaintenanceJobView(
        job_id=uuid4(),
        stream="executor_events",
        action="TRIM",
        state="PENDING",
        reason="weekly retention",
        retention_seconds=604800,
        minimum_retained_entries=1000,
        attempts=0,
        next_attempt_at=now,
        result=None,
        last_error=None,
        created_at=now,
        updated_at=now,
        created_by="operator-1",
        updated_by="operator-1",
    )


def test_stream_maintenance_routes_are_connected() -> None:
    maintenance = view()
    result = StreamMaintenanceOperationResult(
        job=maintenance,
        operation_replayed=False,
    )
    operations = SimpleNamespace(
        plan=AsyncMock(return_value=result),
        submit_trim=AsyncMock(return_value=result),
        jobs=AsyncMock(
            return_value=StreamMaintenancePage(
                items=[maintenance],
                has_more=False,
            )
        ),
        detail=AsyncMock(return_value=maintenance),
    )
    container = SimpleNamespace(stream_maintenance_operations=operations)
    app = create_app(Settings(), start_runtime=False)
    app.dependency_overrides[api_container] = lambda: container
    app.dependency_overrides[current_user] = lambda: "operator-1"
    body = {
        "stream": "executor_events",
        "idempotency_key": "maintenance-1",
        "reason": "weekly retention",
    }

    with TestClient(app) as client:
        planned = client.post(
            "/api/v1/operations/stream-maintenance/plans",
            json=body,
        )
        created = client.post(
            "/api/v1/operations/stream-maintenance/jobs",
            json={**body, "idempotency_key": "maintenance-2"},
        )
        listed = client.get("/api/v1/operations/stream-maintenance/jobs")
        detail = client.get(
            f"/api/v1/operations/stream-maintenance/jobs/{maintenance.job_id}"
        )

    assert planned.status_code == 200
    assert created.status_code == 202
    assert listed.status_code == detail.status_code == 200
    operations.plan.assert_awaited_once()
    operations.submit_trim.assert_awaited_once()


def test_stream_maintenance_requires_runtime() -> None:
    app = create_app(Settings(), start_runtime=False)
    container = SimpleNamespace(stream_maintenance_operations=None)
    app.dependency_overrides[api_container] = lambda: container
    app.dependency_overrides[current_user] = lambda: "operator-1"

    with TestClient(app) as client:
        response = client.get("/api/v1/operations/stream-maintenance/jobs")

    assert response.status_code == 503
