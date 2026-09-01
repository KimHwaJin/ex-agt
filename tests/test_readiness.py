from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Iterable
from typing import Any
from wsgiref.util import setup_testing_defaults

import pytest
from fastapi.testclient import TestClient

import ex_agent.api.routers.health as health_module
from ex_agent.api.app import create_app
from ex_agent.config import Settings
from ex_agent.metrics import _worker_http_application
from ex_agent.readiness import (
    DependencyStatus,
    ReadinessResult,
    ReadinessState,
    _probe,
)


def _result(*, ready: bool) -> ReadinessResult:
    return ReadinessResult(
        checks={
            name: DependencyStatus(
                ready=ready,
                latency_seconds=0.01,
                error=None if ready else "ConnectionError",
            )
            for name in ("postgres", "redis")
        },
        checked_at_epoch_seconds=1_800_000_000,
    )


def _wsgi_request(
    application: Callable[
        [dict[str, Any], Callable[..., Any]],
        Iterable[bytes],
    ],
    path: str,
) -> tuple[str, bytes]:
    environ: dict[str, Any] = {}
    setup_testing_defaults(environ)
    environ["PATH_INFO"] = path
    response_status = ""

    def start_response(
        status: str,
        headers: list[tuple[str, str]],
        exc_info: object | None = None,
    ) -> None:
        nonlocal response_status
        response_status = status

    body = b"".join(application(environ, start_response))
    return response_status, body


def test_api_readiness_returns_dependency_status(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def probe(*args: object, **kwargs: object) -> ReadinessResult:
        return _result(ready=True)

    monkeypatch.setattr(health_module, "probe_dependencies", probe)
    with TestClient(create_app(Settings(), start_runtime=False)) as client:
        response = client.get("/readyz")

    assert response.status_code == 200
    assert response.json()["status"] == "ready"
    assert response.json()["checks"]["postgres"]["ready"] is True


def test_api_readiness_returns_503_when_dependency_is_unavailable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def probe(*args: object, **kwargs: object) -> ReadinessResult:
        return _result(ready=False)

    monkeypatch.setattr(health_module, "probe_dependencies", probe)
    with TestClient(create_app(Settings(), start_runtime=False)) as client:
        response = client.get("/readyz")

    assert response.status_code == 503
    assert response.json()["status"] == "unready"
    assert response.json()["checks"]["redis"]["error"] == ("ConnectionError")


def test_worker_http_server_exposes_health_readiness_and_metrics() -> None:
    state = ReadinessState()
    application = _worker_http_application(
        state,
        stale_after_seconds=30,
    )
    status, body = _wsgi_request(application, "/healthz")
    assert status == "200 OK"
    assert json.loads(body) == {"status": "ok"}

    status, body = _wsgi_request(application, "/readyz")
    assert status == "503 Unavailable"
    assert json.loads(body)["ready"] is False

    state.update(_result(ready=True))
    status, body = _wsgi_request(application, "/readyz")
    assert status == "200 OK"
    assert json.loads(body)["ready"] is True

    status, body = _wsgi_request(application, "/metrics")
    assert status == "200 OK"
    assert b"ex_agent_component_ready" in body


def test_worker_readiness_rejects_stale_probe(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    state = ReadinessState()
    state.update(_result(ready=True))
    monkeypatch.setattr("ex_agent.readiness.time", lambda: 1_800_000_031)

    payload = state.payload(stale_after_seconds=30)

    assert payload["ready"] is False
    assert payload["stale"] is True


def test_stopping_readiness_is_distinct_from_starting() -> None:
    result = ReadinessResult.stopping()

    assert result.ready is False
    assert result.checks["postgres"].error == "stopping"
    assert result.checks["redis"].error == "stopping"


def test_readiness_staleness_must_exceed_refresh_interval() -> None:
    with pytest.raises(ValueError, match="must exceed"):
        Settings(
            worker_metrics_refresh_seconds=10,
            worker_readiness_stale_seconds=10,
        )


@pytest.mark.asyncio
async def test_dependency_probe_timeout_is_bounded_and_sanitized() -> None:
    async def blocked() -> None:
        await asyncio.sleep(1)

    result = await _probe(blocked, timeout_seconds=0.01)

    assert result.ready is False
    assert result.error == "TimeoutError"
    assert result.latency_seconds < 0.1
