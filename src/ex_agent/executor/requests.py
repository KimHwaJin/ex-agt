"""Wire payload builders shared by direct and durably journaled calls."""

from typing import Any


def submit_payload(
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
) -> dict[str, Any]:
    require_path_sources(steps)
    lifecycle: dict[str, Any] = {"operation_mode": mode}
    if mode == "MULTI":
        lifecycle["operation_wait_timeout_seconds"] = wait_timeout_seconds
    return {
        "idempotency_key": idempotency_key,
        "lifecycle": lifecycle,
        "trigger": {"type": "INTERACTIVE", "actor": agent_actor()},
        "runtime": {"type": "JUPYTER", "profile": runtime_profile},
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


def append_payload(
    *,
    idempotency_key: str,
    expected_version: int,
    steps: list[dict[str, Any]],
) -> dict[str, Any]:
    require_path_sources(steps)
    return {
        "idempotency_key": idempotency_key,
        "expected_version": expected_version,
        "spec": {"schema_version": "1.0", "steps": steps},
        "metadata": {"reason": "adaptive_multi_plan"},
        "actor": agent_actor(),
    }


def finalize_payload(*, idempotency_key: str, expected_version: int) -> dict:
    return {
        "idempotency_key": idempotency_key,
        "expected_version": expected_version,
        "actor": agent_actor(),
    }


def cancel_payload(
    *, idempotency_key: str, actor_type: str, actor_id: str, reason: str | None
) -> dict:
    return {
        "idempotency_key": idempotency_key,
        "reason": reason,
        "actor": {"type": actor_type, "id": actor_id},
    }


def report_payload(*, idempotency_key: str, path: str, sha256: str) -> dict:
    return {
        "idempotency_key": idempotency_key,
        "type": "REPORT",
        "source": {"type": "PATH", "path": path, "sha256": sha256},
        "name": "analysis-report.md",
        "description": "Agent-generated successful execution report",
        "media_type": "text/markdown",
        "append_to_notebook": True,
        "metadata": {"producer": "ex-agent"},
        "actor": agent_actor(),
    }


def agent_actor() -> dict[str, str]:
    return {"type": "AGENT", "id": "ex-agent"}


def require_path_sources(steps: list[dict[str, Any]]) -> None:
    for step in steps:
        source = step.get("payload", {}).get("source", {})
        if source.get("type") != "PATH":
            raise ValueError("Executor Step source must use PATH")
        if not source.get("path") or not source.get("sha256"):
            raise ValueError("Executor PATH source requires path and sha256")
