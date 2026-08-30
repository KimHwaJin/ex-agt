from __future__ import annotations

from datetime import UTC, datetime
from typing import Any
from uuid import UUID, uuid4

from fastapi import FastAPI
from fastapi.testclient import TestClient

from ex_agent.api.container import api_container
from ex_agent.api.identity import TrustedHeaderIdentityProvider
from ex_agent.api.routers.workflows import workflow_router
from ex_agent.application.workflow_lifecycle import (
    WorkflowLifecycleForbiddenError,
)
from ex_agent.domain.contracts import (
    WorkflowLifecycleActionPage,
    WorkflowLifecycleResult,
    WorkflowOperationsView,
    WorkflowVersionPage,
)
from ex_agent.persistence.repositories.workflow_lifecycle import (
    WorkflowLifecycleConflictError,
)


class FakeWorkflowLifecycle:
    def __init__(self, workflow_id: UUID) -> None:
        self.workflow_id = workflow_id
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def overview(
        self,
        workflow_id: UUID,
        *,
        actor_user_id: str,
    ) -> WorkflowOperationsView:
        self._record(
            "overview",
            workflow_id=workflow_id,
            actor_user_id=actor_user_id,
        )
        self._raise_for_actor(actor_user_id)
        return WorkflowOperationsView(
            workflow_id=workflow_id,
            name="운영 Workflow",
            description="HTTP 계약 검증",
            owner_user_id="owner",
            owner_project_id="project-one",
            visibility="SERVICE",
            status="ACTIVE",
            latest_version=2,
            active_workflow_version_id=uuid4(),
            active_version=1,
            access_policy={"version": "service-v1"},
            required_permission=None,
            created_at=datetime.now(UTC),
            updated_at=datetime.now(UTC),
        )

    async def versions(
        self,
        workflow_id: UUID,
        *,
        actor_user_id: str,
        cursor: str | None,
        limit: int,
    ) -> WorkflowVersionPage:
        self._record(
            "versions",
            workflow_id=workflow_id,
            actor_user_id=actor_user_id,
            cursor=cursor,
            limit=limit,
        )
        if cursor == "invalid":
            raise ValueError("Invalid Workflow pagination cursor")
        return WorkflowVersionPage(
            items=[],
            next_cursor="next-page" if cursor is None else None,
        )

    async def version_detail(
        self,
        workflow_id: UUID,
        workflow_version_id: UUID,
        *,
        actor_user_id: str,
    ) -> Any:
        self._record(
            "version_detail",
            workflow_id=workflow_id,
            workflow_version_id=workflow_version_id,
            actor_user_id=actor_user_id,
        )
        raise LookupError(f"Unknown Workflow version: {workflow_version_id}")

    async def actions(
        self,
        workflow_id: UUID,
        *,
        actor_user_id: str,
        cursor: str | None,
        limit: int,
    ) -> WorkflowLifecycleActionPage:
        self._record(
            "actions",
            workflow_id=workflow_id,
            actor_user_id=actor_user_id,
            cursor=cursor,
            limit=limit,
        )
        return WorkflowLifecycleActionPage(items=[], next_cursor=None)

    async def update_status(
        self,
        workflow_id: UUID,
        *,
        actor_user_id: str,
        request: Any,
    ) -> WorkflowLifecycleResult:
        self._record(
            "update_status",
            workflow_id=workflow_id,
            actor_user_id=actor_user_id,
            request=request,
        )
        if request.idempotency_key == "conflict":
            raise WorkflowLifecycleConflictError(
                "Idempotency key payload mismatch"
            )
        return WorkflowLifecycleResult(
            workflow_id=workflow_id,
            action=(
                "WORKFLOW_ACTIVATED"
                if request.status == "ACTIVE"
                else "WORKFLOW_DEACTIVATED"
            ),
            workflow_status=request.status,
            review_status=None,
            version_active=None,
            applied=True,
        )

    def _raise_for_actor(self, actor_user_id: str) -> None:
        if actor_user_id == "denied-user":
            raise WorkflowLifecycleForbiddenError(
                "actor does not own the Workflow"
            )

    def _record(self, name: str, **values: Any) -> None:
        self.calls.append((name, values))


class FakeContainer:
    def __init__(self, lifecycle: FakeWorkflowLifecycle) -> None:
        self.identity = TrustedHeaderIdentityProvider()
        self.workflow_lifecycle = lifecycle


def _client(
    workflow_id: UUID,
) -> tuple[TestClient, FakeWorkflowLifecycle]:
    lifecycle = FakeWorkflowLifecycle(workflow_id)
    container = FakeContainer(lifecycle)
    app = FastAPI()
    app.include_router(workflow_router())
    app.dependency_overrides[api_container] = lambda: container
    return TestClient(app), lifecycle


def test_workflow_overview_requires_and_forwards_bff_identity() -> None:
    workflow_id = uuid4()
    client, lifecycle = _client(workflow_id)

    unauthorized = client.get(f"/api/v1/workflows/{workflow_id}")
    response = client.get(
        f"/api/v1/workflows/{workflow_id}",
        headers={"X-User-ID": "owner"},
    )

    assert unauthorized.status_code == 401
    assert response.status_code == 200
    assert response.json()["latest_version"] == 2
    assert lifecycle.calls[-1] == (
        "overview",
        {"workflow_id": workflow_id, "actor_user_id": "owner"},
    )


def test_workflow_versions_forward_cursor_and_validate_limit() -> None:
    workflow_id = uuid4()
    client, lifecycle = _client(workflow_id)
    path = f"/api/v1/workflows/{workflow_id}/versions"

    first = client.get(
        path,
        params={"limit": 25},
        headers={"X-User-ID": "owner"},
    )
    second = client.get(
        path,
        params={"cursor": "next-page", "limit": 25},
        headers={"X-User-ID": "owner"},
    )
    invalid_limit = client.get(
        path,
        params={"limit": 101},
        headers={"X-User-ID": "owner"},
    )

    assert first.status_code == 200
    assert first.json()["next_cursor"] == "next-page"
    assert second.status_code == 200
    assert second.json()["next_cursor"] is None
    assert invalid_limit.status_code == 422
    assert lifecycle.calls[-1][1]["cursor"] == "next-page"


def test_workflow_router_maps_domain_errors_to_http_statuses() -> None:
    workflow_id = uuid4()
    version_id = uuid4()
    client, _lifecycle = _client(workflow_id)
    headers = {"X-User-ID": "owner"}

    forbidden = client.get(
        f"/api/v1/workflows/{workflow_id}",
        headers={"X-User-ID": "denied-user"},
    )
    missing = client.get(
        f"/api/v1/workflows/{workflow_id}/versions/{version_id}",
        headers=headers,
    )
    invalid_cursor = client.get(
        f"/api/v1/workflows/{workflow_id}/versions",
        params={"cursor": "invalid"},
        headers=headers,
    )
    conflict = client.post(
        f"/api/v1/workflows/{workflow_id}/status",
        headers=headers,
        json={
            "idempotency_key": "conflict",
            "status": "INACTIVE",
            "reason": "계약 검증",
        },
    )

    assert forbidden.status_code == 403
    assert missing.status_code == 404
    assert invalid_cursor.status_code == 422
    assert conflict.status_code == 409


def test_workflow_status_body_is_validated_and_serialized() -> None:
    workflow_id = uuid4()
    client, lifecycle = _client(workflow_id)
    path = f"/api/v1/workflows/{workflow_id}/status"
    headers = {"X-User-ID": "owner"}

    invalid = client.post(
        path,
        headers=headers,
        json={"idempotency_key": "status-one", "status": "UNKNOWN"},
    )
    response = client.post(
        path,
        headers=headers,
        json={
            "idempotency_key": "status-two",
            "status": "INACTIVE",
            "reason": "운영 점검",
        },
    )

    assert invalid.status_code == 422
    assert response.status_code == 200
    assert response.json()["action"] == "WORKFLOW_DEACTIVATED"
    assert lifecycle.calls[-1][1]["actor_user_id"] == "owner"
