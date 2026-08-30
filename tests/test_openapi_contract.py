from pathlib import Path

from ex_agent.api.app import create_app
from ex_agent.config import Settings

_OPERATIONS = {
    (
        "post",
        "/api/v1/projects/{project_id}/sessions/{session_id}/tasks",
    ): "createTask",
    ("get", "/api/v1/tasks/{task_id}"): "getTask",
    ("post", "/api/v1/tasks/{task_id}/resume"): "resumeTask",
    ("post", "/api/v1/tasks/{task_id}/cancel"): "cancelTask",
    ("get", "/api/v1/tasks/{task_id}/events"): "streamTaskEvents",
    (
        "get",
        "/api/v1/tasks/{task_id}/workflow-promotion-draft",
    ): "getWorkflowPromotionDraft",
    ("post", "/api/v1/tasks/{task_id}/workflow-promotions"): "promoteWorkflow",
    ("get", "/api/v1/workflows/{workflow_id}"): "getWorkflow",
    (
        "get",
        "/api/v1/workflows/{workflow_id}/versions",
    ): "listWorkflowVersions",
    (
        "post",
        "/api/v1/workflows/{workflow_id}/versions",
    ): "createWorkflowVersion",
    (
        "get",
        "/api/v1/workflows/{workflow_id}/versions/{workflow_version_id}",
    ): "getWorkflowVersion",
    (
        "get",
        "/api/v1/workflows/{workflow_id}/lifecycle-actions",
    ): "listWorkflowLifecycleActions",
    (
        "post",
        "/api/v1/workflows/{workflow_id}/versions/"
        "{workflow_version_id}/reviews",
    ): "reviewWorkflowVersion",
    (
        "post",
        "/api/v1/workflows/{workflow_id}/versions/"
        "{workflow_version_id}/activate",
    ): "activateWorkflowVersion",
    ("post", "/api/v1/workflows/{workflow_id}/status"): "updateWorkflowStatus",
}


def _schema() -> dict:
    settings = Settings(agent_skill_root=Path(__file__).parents[1] / "skills")
    return create_app(settings).openapi()


def test_public_operation_ids_are_explicit_unique_and_stable() -> None:
    schema = _schema()

    actual = {
        (method, path): operation["operationId"]
        for path, path_item in schema["paths"].items()
        for method, operation in path_item.items()
        if method in {"get", "post", "put", "patch", "delete"}
        and path.startswith("/api/v1")
    }

    assert actual == _OPERATIONS
    assert len(set(actual.values())) == len(actual)


def test_resource_and_cursor_page_schemas_follow_api_conventions() -> None:
    schemas = _schema()["components"]["schemas"]
    audit_fields = {
        "created_at",
        "updated_at",
        "created_by",
        "updated_by",
    }

    for name in (
        "TaskAcceptedResponse",
        "TaskResponse",
        "WorkflowOperationsView",
        "WorkflowVersionSummary",
        "WorkflowLifecycleActionView",
    ):
        assert audit_fields.issubset(schemas[name]["properties"])
        assert audit_fields.issubset(schemas[name]["required"])

    for name in ("WorkflowVersionPage", "WorkflowLifecycleActionPage"):
        assert {"items", "next_cursor", "has_more"}.issubset(
            schemas[name]["properties"]
        )
        assert {"items", "has_more"}.issubset(schemas[name]["required"])


def test_public_operations_document_common_error_contracts() -> None:
    schema = _schema()
    schemas = schema["components"]["schemas"]

    assert "ErrorResponse" in schemas
    for (method, path), _operation_id in _OPERATIONS.items():
        responses = schema["paths"][path][method]["responses"]
        assert "401" in responses
        if path != "/api/v1/tasks/{task_id}/events":
            assert "422" in responses


def test_mutation_request_schemas_include_bff_examples() -> None:
    schemas = _schema()["components"]["schemas"]

    for name in (
        "TaskCreateRequest",
        "CancelRequest",
        "WorkflowPromotionRequest",
        "WorkflowVersionCreateRequest",
        "WorkflowVersionReviewRequest",
        "WorkflowStatusRequest",
    ):
        assert "example" in schemas[name]
