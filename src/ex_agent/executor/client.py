from __future__ import annotations

from typing import Any, Literal
from uuid import UUID

import httpx

from ex_agent.executor.contracts import (
    ArtifactResponse,
    CommandResponse,
    ExecutionResult,
    ExecutorEvent,
    ExecutorEventPage,
)


class ExecutorClient:
    def __init__(
        self,
        base_url: str,
        *,
        timeout_seconds: float,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._owned_client = client is None
        self._client = client or httpx.AsyncClient(
            base_url=base_url.rstrip("/"),
            timeout=httpx.Timeout(timeout_seconds),
        )

    async def close(self) -> None:
        if self._owned_client:
            await self._client.aclose()

    async def submit(
        self,
        *,
        idempotency_key: str,
        mode: str,
        wait_timeout_seconds: int,
        runtime_profile: str,
        user_id: str,
        project_id: str,
        session_id: str,
        task_id: str,
        workflow_id: str | None,
        steps: list[dict[str, Any]],
    ) -> CommandResponse:
        _require_path_sources(steps)
        lifecycle: dict[str, Any] = {"operation_mode": mode}
        if mode == "MULTI":
            lifecycle["operation_wait_timeout_seconds"] = wait_timeout_seconds
        payload = {
            "idempotency_key": idempotency_key,
            "lifecycle": lifecycle,
            "trigger": {
                "type": "INTERACTIVE",
                "actor": {"type": "AGENT", "id": "ex-agent"},
            },
            "runtime": {
                "type": "JUPYTER",
                "profile": runtime_profile,
            },
            "context": {
                "user_id": user_id,
                "project_id": project_id,
                "session_id": session_id,
                "task_id": task_id,
                "workflow_id": workflow_id,
            },
            "operation": {
                "spec": {"schema_version": "1.0", "steps": steps},
                "metadata": {},
            },
            "metadata": {"agent_plan_task_id": task_id},
        }
        response = await self._request("POST", "/executions", json=payload)
        return CommandResponse.model_validate(response.json())

    async def append_operation(
        self,
        execution_id: UUID,
        *,
        idempotency_key: str,
        expected_version: int,
        steps: list[dict[str, Any]],
    ) -> CommandResponse:
        _require_path_sources(steps)
        payload = {
            "idempotency_key": idempotency_key,
            "expected_version": expected_version,
            "spec": {"schema_version": "1.0", "steps": steps},
            "metadata": {"reason": "adaptive_multi_plan"},
            "actor": {"type": "AGENT", "id": "ex-agent"},
        }
        response = await self._request(
            "POST",
            f"/executions/{execution_id}/operations",
            json=payload,
        )
        return CommandResponse.model_validate(response.json())

    async def finalize(
        self,
        execution_id: UUID,
        *,
        idempotency_key: str,
        expected_version: int,
    ) -> CommandResponse:
        response = await self._request(
            "POST",
            f"/executions/{execution_id}/finalize",
            json={
                "idempotency_key": idempotency_key,
                "expected_version": expected_version,
                "actor": {"type": "AGENT", "id": "ex-agent"},
            },
        )
        return CommandResponse.model_validate(response.json())

    async def cancel(
        self,
        execution_id: UUID,
        *,
        idempotency_key: str,
        actor_type: Literal["AGENT", "USER", "BATCH"],
        actor_id: str,
        reason: str | None,
    ) -> CommandResponse:
        response = await self._request(
            "POST",
            f"/executions/{execution_id}/cancel",
            json={
                "idempotency_key": idempotency_key,
                "reason": reason,
                "actor": {"type": actor_type, "id": actor_id},
            },
        )
        return CommandResponse.model_validate(response.json())

    async def result(self, execution_id: UUID) -> ExecutionResult:
        response = await self._request(
            "GET",
            f"/executions/{execution_id}/result",
        )
        return ExecutionResult.model_validate(response.json())

    async def events_after(
        self,
        execution_id: UUID,
        *,
        after_sequence: int,
        limit: int = 500,
    ) -> list[ExecutorEvent]:
        events: list[ExecutorEvent] = []
        cursor: str | None = None
        remaining = limit
        while remaining > 0:
            page_limit = min(remaining, 500)
            params: dict[str, Any] = {
                "after_sequence": after_sequence,
                "limit": page_limit,
            }
            if cursor is not None:
                params["cursor"] = cursor
            response = await self._request(
                "GET",
                f"/executions/{execution_id}/events",
                params=params,
            )
            page = ExecutorEventPage.model_validate(response.json())
            events.extend(page.items)
            remaining -= len(page.items)
            if not page.has_more or page.next_cursor is None:
                break
            if not page.items:
                raise ValueError("Executor event page did not advance")
            cursor = page.next_cursor
        return events

    async def materialize_report(
        self,
        execution_id: UUID,
        *,
        idempotency_key: str,
        path: str,
        sha256: str,
    ) -> ArtifactResponse:
        response = await self._request(
            "POST",
            f"/executions/{execution_id}/artifacts",
            json={
                "idempotency_key": idempotency_key,
                "type": "REPORT",
                "source": {
                    "type": "PATH",
                    "path": path,
                    "sha256": sha256,
                },
                "name": "analysis-report.md",
                "description": "Agent-generated successful execution report",
                "media_type": "text/markdown",
                "append_to_notebook": True,
                "metadata": {"producer": "ex-agent"},
                "actor": {"type": "AGENT", "id": "ex-agent"},
            },
        )
        return ArtifactResponse.model_validate(response.json())

    async def _request(
        self,
        method: str,
        path: str,
        **kwargs: Any,
    ) -> httpx.Response:
        last_error: Exception | None = None
        for attempt in range(3):
            try:
                response = await self._client.request(
                    method,
                    path,
                    **kwargs,
                )
                try:
                    response.raise_for_status()
                except httpx.HTTPStatusError as error:
                    detail = response.text[:2000]
                    raise ExecutorRequestError(
                        response.status_code,
                        detail,
                    ) from error
                return response
            except (httpx.TimeoutException, httpx.NetworkError) as error:
                last_error = error
                if attempt == 2:
                    raise
        raise RuntimeError("Executor request failed") from last_error


def _require_path_sources(steps: list[dict[str, Any]]) -> None:
    for step in steps:
        source = step.get("payload", {}).get("source", {})
        if source.get("type") != "PATH":
            raise ValueError("Executor Step source must use PATH")
        if not source.get("path") or not source.get("sha256"):
            raise ValueError("Executor PATH source requires path and sha256")


class ExecutorRequestError(RuntimeError):
    def __init__(self, status_code: int, detail: str) -> None:
        super().__init__(f"Executor HTTP {status_code}: {detail}")
        self.status_code = status_code
        self.detail = detail
