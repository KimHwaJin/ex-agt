from __future__ import annotations

import json
from uuid import UUID

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from ex_agent.application.capabilities.common import (
    executor_outcome,
    executor_step,
    state_execution_mode,
    task_id,
    validate_model,
)
from ex_agent.application.capabilities.planning import (
    compile_and_persist_plan,
)
from ex_agent.application.state import AgentGraphState
from ex_agent.config import Settings
from ex_agent.domain.contracts import (
    ExecutorBoundarySignal,
    ExecutorReconciliation,
    MultiDecision,
    SubmissionReceipt,
)
from ex_agent.domain.enums import (
    ExecutionMode,
    ExecutorOutcome,
    MultiAction,
)
from ex_agent.executor.client import ExecutorClient
from ex_agent.executor.results import validated_result_summaries
from ex_agent.persistence.repository import AgentRepository
from ex_agent.tools.compiler import SourceCompiler
from ex_agent.tools.registry import ToolRegistry


class ExecutionCapability:
    """Executor submission, reconciliation, adaptation, and cancellation."""

    def __init__(
        self,
        settings: Settings,
        repository: AgentRepository,
        executor: ExecutorClient,
        registry: ToolRegistry,
        model: BaseChatModel,
        compiler: SourceCompiler,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._executor = executor
        self._registry = registry
        self._model = model
        self._compiler = compiler

    async def submit_execution(
        self,
        state: AgentGraphState,
    ) -> SubmissionReceipt:
        execution_mode = state_execution_mode(state)
        steps = list(
            await self._repository.approved_steps(
                UUID(state["plan_revision_id"])
            )
        )
        if execution_mode is ExecutionMode.MULTI:
            steps = steps[:1]
        payloads = [executor_step(item) for item in steps]
        response = await self._executor.submit(
            idempotency_key=(
                f"task:{state['active_task_id']}:submit:"
                f"{state['plan_revision_number']}"
            ),
            mode=execution_mode.value,
            wait_timeout_seconds=(
                self._settings.executor_operation_wait_timeout_seconds
            ),
            runtime_profile=state["runtime_profile"],
            user_id=state["user_id"],
            project_id=state["project_id"],
            session_id=state["session_id"],
            task_id=state["active_task_id"],
            workflow_id=state.get("selected_workflow_version_id") or None,
            steps=payloads,
        )
        if response.operation is None:
            raise ValueError("Executor submit response omitted Operation")
        await self._repository.bind_execution(
            task_id=task_id(state),
            execution_id=response.execution_id,
            operation_id=response.operation.operation_id,
            execution_version=response.state.version,
            next_step_sequence=len(steps),
        )
        return SubmissionReceipt(
            execution_id=response.execution_id,
            operation_id=response.operation.operation_id,
            execution_version=response.state.version,
        )

    async def reconcile_executor(
        self,
        state: AgentGraphState,
        signal: ExecutorBoundarySignal,
    ) -> ExecutorReconciliation:
        result = await self._executor.result(signal.execution_id)
        outcome = executor_outcome(result)
        latest_operation = result.operations[-1] if result.operations else None
        error_message = (
            latest_operation.result.error_message if latest_operation else None
        )
        refs = [
            str(step.result.result_ref.get("relative_path"))
            for operation in result.operations
            for step in operation.steps
            if step.result.result_ref
        ]
        result_summaries = await validated_result_summaries(
            result,
            self._settings.executor_shared_storage_root,
            max_context_chars=(
                self._settings.executor_result_context_max_chars
            ),
            max_manifest_bytes=(
                self._settings.executor_result_manifest_max_bytes
            ),
        )
        await self._repository.update_binding(
            task_id(state),
            execution_version=result.execution.state.version,
            last_event_sequence=signal.event_sequence,
        )
        return ExecutorReconciliation(
            outcome=outcome,
            execution_id=signal.execution_id,
            execution_version=result.execution.state.version,
            operation_id=(
                latest_operation.operation_id if latest_operation else None
            ),
            error_message=error_message,
            result_refs=refs,
            result_summaries=result_summaries,
        )

    async def adapt_multi_plan(
        self,
        state: AgentGraphState,
        reconciliation: ExecutorReconciliation,
    ) -> MultiDecision:
        if (
            reconciliation.outcome is ExecutorOutcome.OPERATION_FAILED
            and state.get("correction_count", 0)
            >= self._settings.correction_limit
        ):
            return MultiDecision(
                action=MultiAction.FAIL,
                rationale="The automatic correction limit was reached.",
            )
        adapter = self._model.with_structured_output(MultiDecision)
        raw = await adapter.ainvoke(
            [
                SystemMessage(
                    content=(
                        "Decide the next MULTI execution action from the user "
                        "objective and latest operation result. APPEND_STEP "
                        "must include exactly one function definition plus "
                        "its call. Use REQUIRE_REAPPROVAL if the next step "
                        "materially changes scope, data access, cost, runtime "
                        "profile, or risk. FINALIZE when the analysis is "
                        "complete. FAIL when correction is not reasonable. "
                        "For analysis, preserve registered Skill/Tool "
                        "lineage; for free code, use CUSTOM_CODE."
                    )
                ),
                HumanMessage(
                    content=json.dumps(
                        {
                            "request": state["user_message"],
                            "plan": state["plan"].model_dump(mode="json"),
                            "result": reconciliation.model_dump(mode="json"),
                        },
                        ensure_ascii=False,
                    )
                ),
            ]
        )
        decision = validate_model(MultiDecision, raw)
        if decision.next_step is None:
            return decision
        return decision.model_copy(
            update={
                "next_step": self._registry.canonicalize_step_lineage(
                    decision.next_step
                )
            }
        )

    async def append_operation(
        self,
        state: AgentGraphState,
        decision: MultiDecision,
    ) -> SubmissionReceipt:
        if decision.next_step is None:
            raise ValueError("Adaptive append requires next_step")
        next_step = decision.next_step.model_copy(update={"sequence": 0})
        plan = state["plan"].model_copy(update={"steps": [next_step]})
        persisted = await compile_and_persist_plan(
            self._settings,
            self._repository,
            self._registry,
            self._compiler,
            state,
            plan,
        )
        steps = await self._repository.approved_steps(
            persisted.plan_revision_id
        )
        binding = await self._repository.binding_for_task(task_id(state))
        step = steps[0]
        step.sequence = binding.next_step_sequence
        response = await self._executor.append_operation(
            binding.execution_id,
            idempotency_key=(
                f"task:{state['active_task_id']}:operation:"
                f"{binding.next_step_sequence}"
            ),
            expected_version=binding.execution_version,
            steps=[executor_step(step)],
        )
        if response.operation is None:
            raise ValueError("Executor append response omitted Operation")
        await self._repository.update_binding(
            task_id(state),
            operation_id=response.operation.operation_id,
            execution_version=response.state.version,
            next_step_sequence=binding.next_step_sequence + 1,
        )
        return SubmissionReceipt(
            execution_id=response.execution_id,
            operation_id=response.operation.operation_id,
            execution_version=response.state.version,
        )

    async def finalize_execution(self, state: AgentGraphState) -> None:
        binding = await self._repository.binding_for_task(task_id(state))
        response = await self._executor.finalize(
            binding.execution_id,
            idempotency_key=f"task:{state['active_task_id']}:finalize",
            expected_version=binding.execution_version,
        )
        await self._repository.update_binding(
            task_id(state),
            execution_version=response.state.version,
        )

    async def cancel_execution(
        self,
        state: AgentGraphState,
        reason: str | None,
    ) -> None:
        binding = await self._repository.binding_for_task(task_id(state))
        response = await self._executor.cancel(
            binding.execution_id,
            idempotency_key=f"task:{state['active_task_id']}:cancel",
            actor_type="USER",
            actor_id=state["user_id"],
            reason=reason,
        )
        await self._repository.update_binding(
            task_id(state),
            execution_version=response.state.version,
        )


__all__ = ["ExecutionCapability"]
