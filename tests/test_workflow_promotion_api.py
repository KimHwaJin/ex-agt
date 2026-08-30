from pathlib import Path

from ex_agent.api.app import create_app
from ex_agent.config import Settings


def test_openapi_exposes_workflow_promotion_endpoints() -> None:
    settings = Settings(agent_skill_root=Path(__file__).parents[1] / "skills")

    paths = create_app(settings).openapi()["paths"]

    assert "/api/v1/tasks/{task_id}/workflow-promotion-draft" in paths
    assert "/api/v1/tasks/{task_id}/workflow-promotions" in paths
    assert "/api/v1/workflows/{workflow_id}/versions" in paths
    assert (
        "/api/v1/workflows/{workflow_id}/versions/"
        "{workflow_version_id}/reviews" in paths
    )
    assert (
        "/api/v1/workflows/{workflow_id}/versions/"
        "{workflow_version_id}/activate" in paths
    )
    assert "/api/v1/workflows/{workflow_id}/status" in paths
