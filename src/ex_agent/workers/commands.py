from __future__ import annotations

import asyncio
from typing import Any
from uuid import UUID

from langgraph.types import Command

from ex_agent.config import Settings
from ex_agent.domain.enums import TaskStatus
from ex_agent.executor.client import ExecutorClient, ExecutorRequestError
from ex_agent.persistence.repository import AgentRepository
from ex_agent.workers.checkpoints import interrupt_payload, task_graph_config
from ex_agent.workers.handlers import FAILURE_COMPENSATION

_EXECUTOR_TERMINAL_STATUSES = {"CANCELLED", "SUCCEEDED", "FAILED"}


class CommandProcessor:
    def __init__(
        self,
        settings: Settings,
        repository: AgentRepository,
        executor: ExecutorClient,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._executor = executor

    async def process(self, graph: Any, command_id: UUID) -> None:
        command = await self._repository.get_command(command_id)
        if command is None or command.state in {"DONE", "FAILED"}:
            return
        task = await self._repository.get_task(command.task_id)
        if task is not None and TaskStatus(task.status).is_terminal:
            await self._repository.set_command_state(command_id, "DONE")
            return
        await self._repository.set_command_state(command_id, "PROCESSING")
        if command.command_type == FAILURE_COMPENSATION:
            await self.run_failure_compensation(command)
            return
        await self.run_graph(graph, command)
        await self._repository.set_command_state(command_id, "DONE")

    async def run_graph(self, graph: Any, command: Any) -> None:
        task = await self._repository.get_task(command.task_id)
        if task is None:
            raise LookupError(f"Unknown task: {command.task_id}")
        config = task_graph_config(task.id)
        if command.command_type == "START":
            graph_input: Any = {
                "user_id": task.user_id,
                "project_id": task.project_id,
                "session_id": task.session_id,
                "active_task_id": str(task.id),
                "current_input_message_id": str(task.input_message_id),
                "user_message": task.user_message,
            }
        else:
            await self._repository.clear_interrupt(task.id)
            graph_input = Command(resume=command.payload)
        result = await graph.ainvoke(graph_input, config=config)
        interrupts = result.get("__interrupt__", ())
        if interrupts:
            payload = interrupt_payload(interrupts[0])
            await self._repository.record_interrupt(task.id, payload)

    async def run_failure_compensation(self, command: Any) -> None:
        raw_message = command.payload.get("failure_message")
        if not isinstance(raw_message, str) or not raw_message:
            raise ValueError("Failure compensation omitted failure_message")
        executor_status = await self.compensate_failed_execution(
            command.task_id,
            raw_message,
        )
        if executor_status == "NOT_REQUIRED":
            cleanup_message = "연결된 Executor 실행은 생성되지 않았습니다."
        elif executor_status == "CANCELLED":
            cleanup_message = "연결된 Executor 실행의 취소를 확인했습니다."
        else:
            cleanup_message = (
                "연결된 Executor 실행이 이미 "
                f"{executor_status} 상태로 종료된 것을 확인했습니다."
            )
        content = (
            f"Agent workflow 처리에 실패했습니다: {raw_message}. "
            f"{cleanup_message}"
        )
        await self._repository.complete_failure_compensation(
            command.id,
            command.task_id,
            content,
            failure_message=raw_message,
            executor_status=executor_status,
        )

    async def compensate_failed_execution(
        self,
        task_id: UUID,
        failure_message: str,
    ) -> str:
        task = await self._repository.get_task(task_id)
        if task is None:
            raise LookupError(f"Unknown task: {task_id}")
        if task.execution_id is None:
            return "NOT_REQUIRED"
        execution_id = task.execution_id
        async with asyncio.timeout(
            self._settings.executor_failure_cleanup_timeout_seconds
        ):
            result = await self._executor.result(execution_id)
            status = result.execution.state.status
            if status in _EXECUTOR_TERMINAL_STATUSES:
                return status
            try:
                response = await self._executor.cancel(
                    execution_id,
                    idempotency_key=(f"task:{task_id}:agent-failure-cancel"),
                    actor_type="AGENT",
                    actor_id="ex-agent",
                    reason=f"Agent workflow failed: {failure_message}",
                )
            except ExecutorRequestError as error:
                if error.status_code not in {409, 422}:
                    raise
            else:
                await self._repository.update_binding(
                    task_id,
                    execution_version=response.state.version,
                )
            while True:
                result = await self._executor.result(execution_id)
                status = result.execution.state.status
                if status in _EXECUTOR_TERMINAL_STATUSES:
                    return status
                await asyncio.sleep(
                    self._settings.executor_failure_cleanup_poll_seconds
                )


__all__ = ["CommandProcessor"]
