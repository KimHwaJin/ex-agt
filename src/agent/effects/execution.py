import asyncio
from typing import Any
from uuid import UUID

from agent.effects.files import capture_files
from agent.effects.journal import EffectJournal
from agent.effects.plans import DurablePlans
from agent.effects.projections import EffectProjections
from agent.effects.runner import ExecutorEffectSender
from ex_agent.application.capabilities.common import (
    executor_step,
    state_execution_mode,
    task_id,
    validate_plan_execution_mode,
)
from ex_agent.application.state import AgentGraphState
from ex_agent.config import Settings
from ex_agent.domain.contracts import (
    MultiDecision,
    PersistedPlan,
    PlanDraft,
    SubmissionReceipt,
)
from ex_agent.domain.enums import ExecutionMode
from ex_agent.executor import requests
from ex_agent.persistence.repository import AgentRepository


class DurableExecution:
    def __init__(
        self,
        settings: Settings,
        repository: AgentRepository,
        journal: EffectJournal,
        sender: ExecutorEffectSender,
        plans: DurablePlans,
        projections: EffectProjections,
    ) -> None:
        self.settings = settings
        self.repository = repository
        self.journal = journal
        self.sender = sender
        self.plans = plans
        self.projections = projections

    async def submit(self, state: AgentGraphState) -> SubmissionReceipt:
        validate_plan_execution_mode(state, state["plan"])
        key = (
            f"task:{state['active_task_id']}:submit:"
            f"{state['plan_revision_number']}"
        )
        inputs = {
            name: state.get(name)
            for name in (
                "plan_revision_id",
                "plan_revision_number",
                "plan_public_payload_hash",
                "runtime_profile",
                "execution_mode",
                "user_id",
                "project_id",
                "session_id",
                "selected_workflow_version_id",
            )
        }
        inputs["plan"] = state["plan"].model_dump(mode="json")

        async def prepare() -> dict[str, Any]:
            rows = await self.repository.approved_steps(
                UUID(state["plan_revision_id"])
            )
            if state_execution_mode(state) is ExecutionMode.MULTI:
                rows = rows[:1]
            if not rows:
                raise ValueError("Execution requires approved Steps")
            steps = [executor_step(row) for row in rows]
            return {
                "kind": "submit",
                "path": "/executions",
                "body": requests.submit_payload(
                    idempotency_key=key,
                    mode=state_execution_mode(state).value,
                    wait_timeout_seconds=(
                        self.settings.executor_operation_wait_timeout_seconds
                    ),
                    runtime_profile=state["runtime_profile"],
                    user_id=state["user_id"],
                    project_id=state["project_id"],
                    session_id=state["session_id"],
                    task_id=state["active_task_id"],
                    workflow_id=state.get("selected_workflow_version_id"),
                    steps=steps,
                ),
                "files": await self._files(steps),
                "next_step_sequence": len(rows),
            }

        record = await self.journal.run(
            task_id=task_id(state),
            key=key,
            kind="submit",
            inputs=inputs,
            prepare=prepare,
            send=self.sender.send,
        )
        receipt = SubmissionReceipt.model_validate(record.response)
        await self.projections.binding(
            task_id(state),
            execution_id=receipt.execution_id,
            operation_id=receipt.operation_id,
            execution_version=receipt.execution_version,
            next_step_sequence=record.request["next_step_sequence"],
            create=True,
        )
        return receipt

    async def append(
        self,
        state: AgentGraphState,
        decision: MultiDecision,
    ) -> SubmissionReceipt:
        if state_execution_mode(state) is not ExecutionMode.MULTI:
            raise ValueError("Only MULTI may append an Operation")
        # The graph's plan includes any user edits made during reapproval.
        step = state["plan"].steps[0].model_copy(update={"sequence": 0})
        plan = state["plan"].model_copy(update={"steps": [step]})
        key = (
            f"task:{state['active_task_id']}:append:"
            f"{state['current_operation_id']}"
        )
        inputs = {
            "execution_id": state["execution_id"],
            "previous_operation_id": state["current_operation_id"],
            "plan_revision_id": state["plan_revision_id"],
            "plan": plan.model_dump(mode="json"),
            "decision": decision.model_dump(mode="json"),
        }

        async def prepare() -> dict[str, Any]:
            persisted = await self.plans.persist(state, plan)
            rows = await self.repository.approved_steps(
                persisted.plan_revision_id
            )
            binding = await self.repository.binding_for_task(task_id(state))
            if (
                str(binding.execution_id) != state["execution_id"]
                or str(binding.operation_id) != state["current_operation_id"]
            ):
                raise ValueError("Append input has a stale Execution binding")
            payload = executor_step(rows[0])
            payload["sequence"] = binding.next_step_sequence
            return {
                "kind": "append",
                "execution_id": str(binding.execution_id),
                "path": f"/executions/{binding.execution_id}/operations",
                "body": requests.append_payload(
                    idempotency_key=key,
                    expected_version=binding.execution_version,
                    steps=[payload],
                ),
                "files": await self._files([payload]),
                "next_step_sequence": binding.next_step_sequence + 1,
                "persisted_plan": persisted.model_dump(mode="json"),
                "plan": plan.model_dump(mode="json"),
            }

        record = await self.journal.run(
            task_id=task_id(state),
            key=key,
            kind="append",
            inputs=inputs,
            prepare=prepare,
            send=self.sender.send,
        )
        receipt = SubmissionReceipt.model_validate(record.response)
        await self.projections.binding(
            task_id(state),
            execution_id=receipt.execution_id,
            operation_id=receipt.operation_id,
            execution_version=receipt.execution_version,
            next_step_sequence=record.request["next_step_sequence"],
        )
        return receipt.model_copy(
            update={
                "persisted_plan": PersistedPlan.model_validate(
                    record.request["persisted_plan"]
                ),
                "plan": PlanDraft.model_validate(record.request["plan"]),
            }
        )

    async def lifecycle(
        self,
        state: AgentGraphState,
        *,
        kind: str,
        reason: str | None = None,
    ) -> None:
        if kind not in {"finalize", "cancel"}:
            raise ValueError("Unsupported lifecycle effect")
        key = f"task:{state['active_task_id']}:{kind}"
        inputs = {
            "execution_id": state["execution_id"],
            "user_id": state["user_id"],
            "reason": reason,
        }

        async def prepare() -> dict[str, Any]:
            binding = await self.repository.binding_for_task(task_id(state))
            if str(binding.execution_id) != state["execution_id"]:
                raise ValueError("Lifecycle input has another Execution")
            body = (
                requests.finalize_payload(
                    idempotency_key=key,
                    expected_version=binding.execution_version,
                )
                if kind == "finalize"
                else requests.cancel_payload(
                    idempotency_key=key,
                    actor_type="USER",
                    actor_id=state["user_id"],
                    reason=reason,
                )
            )
            return {
                "kind": kind,
                "execution_id": str(binding.execution_id),
                "path": f"/executions/{binding.execution_id}/{kind}",
                "body": body,
            }

        record = await self.journal.run(
            task_id=task_id(state),
            key=key,
            kind=kind,
            inputs=inputs,
            prepare=prepare,
            send=self.sender.send,
        )
        assert record.response is not None
        await self.projections.binding(
            task_id(state),
            execution_id=UUID(record.response["execution_id"]),
            execution_version=record.response["execution_version"],
        )

    async def _files(self, steps: list[dict[str, Any]]) -> list[dict]:
        return await asyncio.to_thread(
            capture_files,
            self.settings.executor_shared_storage_root,
            steps,
        )
