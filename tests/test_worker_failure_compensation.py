from types import SimpleNamespace
from typing import Any
from uuid import UUID, uuid4

import pytest

from ex_agent.config import Settings
from ex_agent.worker import WorkflowWorker


def _result(execution_id: UUID, status: str) -> Any:
    return SimpleNamespace(
        execution=SimpleNamespace(
            execution_id=execution_id,
            state=SimpleNamespace(status=status, version=1),
        )
    )


class CompensationRepository:
    def __init__(self, task: Any) -> None:
        self.task = task
        self.binding_versions: list[int] = []
        self.completed: list[dict[str, Any]] = []

    async def get_task(self, task_id: UUID) -> Any:
        assert task_id == self.task.id
        return self.task

    async def update_binding(
        self,
        task_id: UUID,
        *,
        execution_version: int,
    ) -> None:
        assert task_id == self.task.id
        self.binding_versions.append(execution_version)

    async def complete_failure_compensation(
        self,
        command_id: UUID,
        task_id: UUID,
        content: str,
        *,
        failure_message: str,
        executor_status: str,
    ) -> None:
        self.completed.append(
            {
                "command_id": command_id,
                "task_id": task_id,
                "content": content,
                "failure_message": failure_message,
                "executor_status": executor_status,
            }
        )


class CompensationExecutor:
    def __init__(self, execution_id: UUID) -> None:
        self.execution_id = execution_id
        self.statuses = iter(["WAITING_FOR_OPERATION", "CANCELLED"])
        self.cancel_calls: list[dict[str, Any]] = []

    async def result(self, execution_id: UUID) -> Any:
        assert execution_id == self.execution_id
        return _result(execution_id, next(self.statuses))

    async def cancel(self, execution_id: UUID, **kwargs: Any) -> Any:
        assert execution_id == self.execution_id
        self.cancel_calls.append(kwargs)
        return SimpleNamespace(
            state=SimpleNamespace(
                status="CANCEL_REQUESTED",
                version=2,
            )
        )


def _worker(repository: Any, executor: Any) -> Any:
    worker: Any = WorkflowWorker.__new__(WorkflowWorker)
    worker._settings = Settings(
        executor_failure_cleanup_timeout_seconds=1,
        executor_failure_cleanup_poll_seconds=0.001,
        worker_metrics_enabled=False,
    )
    worker._repository = repository
    worker._executor = executor
    return worker


@pytest.mark.asyncio
async def test_failure_compensation_cancels_and_confirms_executor() -> None:
    task_id = uuid4()
    execution_id = uuid4()
    task = SimpleNamespace(id=task_id, execution_id=execution_id)
    repository = CompensationRepository(task)
    executor = CompensationExecutor(execution_id)
    worker = _worker(repository, executor)

    status = await worker._compensate_failed_execution(
        task_id,
        "RuntimeError: planning failed",
    )

    assert status == "CANCELLED"
    assert repository.binding_versions == [2]
    assert executor.cancel_calls[0]["actor_type"] == "AGENT"
    assert executor.cancel_calls[0]["actor_id"] == "ex-agent"


@pytest.mark.asyncio
async def test_compensation_finishes_message_after_confirmation() -> None:
    task_id = uuid4()
    command_id = uuid4()
    execution_id = uuid4()
    task = SimpleNamespace(id=task_id, execution_id=execution_id)
    repository = CompensationRepository(task)
    worker = _worker(repository, CompensationExecutor(execution_id))
    command = SimpleNamespace(
        id=command_id,
        task_id=task_id,
        payload={"failure_message": "IndexError: bad sequence"},
    )

    await worker._run_failure_compensation(command)

    assert repository.completed[0]["executor_status"] == "CANCELLED"
    assert "취소를 확인했습니다" in repository.completed[0]["content"]


@pytest.mark.asyncio
async def test_failure_before_executor_submission_needs_no_cancel() -> None:
    task_id = uuid4()
    task = SimpleNamespace(id=task_id, execution_id=None)
    repository = CompensationRepository(task)
    worker = _worker(repository, None)

    status = await worker._compensate_failed_execution(
        task_id,
        "ValueError: invalid plan",
    )

    assert status == "NOT_REQUIRED"
