"""Resolve uncertain submission without replaying any code execution."""

from typing import Any
from uuid import UUID

from agent.effects.journal import EffectJournal
from agent.effects.runner import ExecutorEffectSender
from agent.effects.store import EffectStore
from agent.failure.graph import UnsafeCleanupError
from agent.failure.models import FailureCleanup
from ex_agent.executor import requests
from ex_agent.executor.client import ExecutorClient, ExecutorRequestError
from ex_agent.persistence.models import ExecutorBinding, Task

TERMINAL = {"SUCCEEDED", "FAILED", "CANCELLED"}


class CleanupPending(RuntimeError):
    """No terminal proof yet. Keep durable work and the long chat lock."""


class FailureExecutor:
    def __init__(
        self,
        effects: EffectStore,
        sender: ExecutorEffectSender,
        executor: ExecutorClient,
    ) -> None:
        self.effects, self.sender, self.executor = effects, sender, executor
        self.journal = EffectJournal(effects)

    async def resolve(
        self, record: FailureCleanup, snapshot: Any
    ) -> UUID | None:
        ids = {record.execution_id} if record.execution_id else set()
        async with self.effects.sessions() as session:
            task = await session.get(Task, record.task_id)
            assert task is not None
            binding = await session.get(ExecutorBinding, record.task_id)
            for value in (
                task.execution_id,
                binding.execution_id if binding else None,
            ):
                if value:
                    ids.add(value)
        if snapshot.values.get("active_task_id") == str(record.task_id):
            value = snapshot.values.get("execution_id")
            if value:
                ids.add(UUID(value))
        effect_ids, submissions = await self.effects.execution_references(
            record.task_id
        )
        if submissions > 1:
            raise UnsafeCleanupError(
                "Multiple submission identities need review"
            )
        ids.update(effect_ids)
        if len(ids) > 1:
            raise UnsafeCleanupError("Conflicting Execution identities")
        if ids:
            return ids.pop()
        if not submissions:
            return None  # No journaled submit means no HTTP submit was sent.
        turn = record.turn
        try:
            found = await self.executor.find_task_execution(
                user_id=turn["user_id"],
                project_id=turn["project_id"],
                session_id=record.session_id,
                task_id=str(record.task_id),
            )
        except ValueError as error:
            raise UnsafeCleanupError(str(error)) from error
        if found is None:
            # A delayed POST may still commit. An empty list is not absence
            # proof; never resend submit simply to discover its response.
            raise CleanupPending("Submitted Execution ID is not yet confirmed")
        return found

    async def confirm(self, record: FailureCleanup, execution_id: UUID) -> str:
        async def status() -> str:
            result = await self.executor.result(execution_id)
            if result.execution.execution_id != execution_id:
                raise UnsafeCleanupError("Executor result identity mismatch")
            current = result.execution.state.status
            if current not in TERMINAL | {
                "QUEUED",
                "DISPATCHED",
                "RUNNING",
                "WAITING_FOR_OPERATION",
                "FINALIZING",
                "CANCEL_REQUESTED",
            }:
                raise UnsafeCleanupError("Unknown Executor lifecycle status")
            return current

        current = await status()
        if current in TERMINAL:
            return current
        key = f"task:{record.task_id}:failure-cancel"
        inputs = {"execution_id": str(execution_id), "reason": record.reason}

        async def prepare() -> dict:
            return {
                "kind": "failure_cancel",
                "execution_id": str(execution_id),
                "path": f"/executions/{execution_id}/cancel",
                "body": requests.cancel_payload(
                    idempotency_key=key,
                    actor_type="AGENT",
                    actor_id="ex-agent",
                    reason=f"Agent workflow failed: {record.reason}",
                ),
            }

        try:
            await self.journal.run(
                task_id=record.task_id,
                key=key,
                kind="failure_cancel",
                inputs=inputs,
                prepare=prepare,
                send=self.sender.send,
            )
        except ExecutorRequestError as error:
            if error.status_code != 409:
                raise
        current = await status()
        if current not in TERMINAL:
            raise CleanupPending("Executor cancellation is not yet terminal")
        return current


def failure_message(reason: str, status: str) -> str:
    detail = (
        "Executor에 제출된 실행이 없습니다."
        if status == "NOT_REQUIRED"
        else f"연결된 Executor 실행의 {status} 종료를 확인했습니다."
    )
    return f"Agent 처리에 실패했습니다: {reason}. {detail}"
