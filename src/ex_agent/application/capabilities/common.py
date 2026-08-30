from __future__ import annotations

from pathlib import Path
from typing import Any
from uuid import UUID

from pydantic import BaseModel

from ex_agent.application.state import AgentGraphState
from ex_agent.domain.enums import ExecutionMode, ExecutorOutcome
from ex_agent.executor.contracts import ExecutionResult, executor_step_payload


def task_id(state: AgentGraphState) -> UUID:
    return UUID(state["active_task_id"])


def state_execution_mode(state: AgentGraphState) -> ExecutionMode:
    return ExecutionMode(state["execution_mode"])


def executor_source_path(path: Path, shared_root: Path) -> str:
    request_root = (shared_root.resolve() / "requests").resolve()
    return path.resolve().relative_to(request_root).as_posix()


def validate_model[ModelT: BaseModel](
    model_type: type[ModelT],
    value: Any,
) -> ModelT:
    if isinstance(value, model_type):
        return value
    return model_type.model_validate(value)


def message_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            str(item.get("text", "")) if isinstance(item, dict) else str(item)
            for item in content
        )
    return str(content)


def bounded_report_markdown(content: Any, max_chars: int = 6000) -> str:
    markdown = message_text(content).strip()
    if not markdown:
        raise ValueError("Report model returned empty Markdown")
    if len(markdown) <= max_chars:
        return markdown
    suffix = "\n\n> 리포트가 길이 제한에 맞게 축약되었습니다."
    return f"{markdown[: max_chars - len(suffix)].rstrip()}{suffix}"


def executor_step(step: Any) -> dict[str, Any]:
    return executor_step_payload(
        sequence=step.sequence,
        path=step.compiled_source_path,
        sha256=step.compiled_source_sha256,
        timeout_seconds=step.timeout_seconds,
        skill_name=(step.skill_ref or {}).get("name"),
        tool_name=(step.tool_ref or {}).get("name"),
        parameters=step.parameters,
    )


def executor_outcome(result: ExecutionResult) -> ExecutorOutcome:
    status = result.execution.state.status
    if status == "SUCCEEDED":
        return ExecutorOutcome.SUCCEEDED
    if status == "FAILED":
        return ExecutorOutcome.FAILED
    if status == "CANCELLED":
        return ExecutorOutcome.CANCELLED
    if status == "WAITING_FOR_OPERATION" and result.operations:
        operation_status = result.operations[-1].result.status
        if operation_status == "SUCCEEDED":
            return ExecutorOutcome.OPERATION_SUCCEEDED
        if operation_status == "FAILED":
            return ExecutorOutcome.OPERATION_FAILED
    return ExecutorOutcome.WAITING


__all__ = [
    "bounded_report_markdown",
    "executor_outcome",
    "executor_source_path",
    "executor_step",
    "message_text",
    "state_execution_mode",
    "task_id",
    "validate_model",
]
