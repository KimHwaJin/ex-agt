from fastapi.testclient import TestClient

from ex_agent.api.app import create_app
from ex_agent.config import Settings


def test_api_exposes_prometheus_metrics() -> None:
    with TestClient(create_app(Settings())) as client:
        response = client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "ex_agent_sse_connections" in response.text
