from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any
from uuid import UUID

from langchain_core.embeddings import Embeddings
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel

from ex_agent.application.state import AgentGraphState
from ex_agent.config import Settings
from ex_agent.domain.contracts import (
    ExecutorBoundarySignal,
    ExecutorReconciliation,
    IntentDecision,
    MultiDecision,
    PersistedPlan,
    PlanDraft,
    ReportResult,
    RiskReview,
    SubmissionReceipt,
    WorkflowCandidate,
)
from ex_agent.domain.enums import (
    ExecutionMode,
    ExecutorOutcome,
    MultiAction,
    PlanningKind,
    TaskStatus,
)
from ex_agent.executor.client import ExecutorClient
from ex_agent.executor.contracts import ExecutionResult, executor_step_payload
from ex_agent.executor.files import materialize_input_file
from ex_agent.executor.results import validated_result_summaries
from ex_agent.models import build_chat_model, build_embeddings
from ex_agent.persistence.repository import AgentRepository
from ex_agent.planners.agent import PlannerAgent
from ex_agent.tools.compiler import SourceCompiler
from ex_agent.tools.registry import ToolRegistry

logger = logging.getLogger(__name__)


class DefaultWorkflowServices:
    """Concrete Agent domain service used by the background worker."""

    def __init__(
        self,
        settings: Settings,
        repository: AgentRepository,
        executor: ExecutorClient,
        registry: ToolRegistry,
        *,
        model: BaseChatModel | None = None,
        embeddings: Embeddings | None = None,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._executor = executor
        self._registry = registry
        self._compiler = SourceCompiler(registry)
        self._model = model or build_chat_model(settings)
        self._embeddings = embeddings or build_embeddings(settings)
        self._planner = PlannerAgent(
            settings,
            registry,
            model=self._model,
            audit_sink=repository,
        )

    async def update_status(
        self,
        state: AgentGraphState,
        status: TaskStatus,
    ) -> None:
        await self._repository.update_status(_task_id(state), status)

    async def classify_intent(
        self,
        state: AgentGraphState,
    ) -> IntentDecision:
        classifier = self._model.with_structured_output(IntentDecision)
        raw = await classifier.ainvoke(
            [
                SystemMessage(
                    content=(
                        "Classify the request semantically without keyword "
                        "or rule-based routing. Distinguish questions from "
                        "requests that require code execution. Ask one "
                        "clarification only "
                        "when execution intent cannot safely be determined. "
                        "CODE_EXECUTION requires the user to choose SINGLE or "
                        "MULTI; DATA_ANALYSIS_EXECUTION does not."
                    )
                ),
                HumanMessage(content=state["user_message"]),
            ]
        )
        return _validate_model(IntentDecision, raw)

    async def answer_question(
        self,
        state: AgentGraphState,
        *,
        data_analysis: bool,
    ) -> str:
        domain = (
            "Answer as a practical data-analysis expert."
            if data_analysis
            else "Answer the general question accurately and concisely."
        )
        response = await self._model.ainvoke(
            [
                SystemMessage(content=domain),
                HumanMessage(content=state["user_message"]),
            ]
        )
        return _message_text(response.content)

    async def commit_answer(
        self,
        state: AgentGraphState,
        answer: str,
    ) -> None:
        await self._repository.commit_message(
            _task_id(state),
            answer,
            status=TaskStatus.SUCCEEDED,
        )

    async def review_request_risk(
        self,
        state: AgentGraphState,
    ) -> RiskReview:
        reviewer = self._model.with_structured_output(RiskReview)
        raw = await reviewer.ainvoke(
            [
                SystemMessage(
                    content=(
                        "Review the requested code or analysis before "
                        "planning. ALLOW low/medium risk, WARN or "
                        "REQUIRE_CONFIRMATION for high risk, and BLOCK only "
                        "critical destructive, credential exfiltration, "
                        "privilege escalation, or clearly malicious "
                        "requests. Provide evidence grounded in the request."
                    )
                ),
                HumanMessage(content=state["user_message"]),
            ]
        )
        return _validate_model(RiskReview, raw)

    async def search_workflows(
        self,
        state: AgentGraphState,
    ) -> list[WorkflowCandidate]:
        try:
            embedding = await self._embeddings.aembed_query(
                state["user_message"]
            )
            if len(embedding) != self._settings.agent_embedding_dimensions:
                raise ValueError(
                    "Embedding dimension does not match the configured "
                    "pgvector dimension"
                )
        except Exception as error:
            logger.warning(
                "Workflow retrieval degraded to dynamic planning",
                exc_info=True,
            )
            await self._repository.append_task_event(
                _task_id(state),
                "workflow.search_degraded",
                {
                    "reason": f"{type(error).__name__}: {error}",
                    "fallback": "DYNAMIC_MULTI_PLAN",
                },
            )
            return []
        candidates = await self._repository.workflow_candidates(
            embedding,
            limit=3,
        )
        reviewer = self._model.with_structured_output(RiskReview)
        reviewed: list[WorkflowCandidate] = []
        for candidate in candidates:
            raw = await reviewer.ainvoke(
                [
                    SystemMessage(
                        content=(
                            "Assess the proposed fixed analysis Workflow risk "
                            "from its plan and parameters. Use HIGH when "
                            "explicit user acknowledgement is warranted and "
                            "CRITICAL only "
                            "for a plan that must not execute."
                        )
                    ),
                    HumanMessage(
                        content=json.dumps(
                            candidate.plan.model_dump(mode="json"),
                            ensure_ascii=False,
                        )
                    ),
                ]
            )
            reviewed.append(
                candidate.model_copy(
                    update={"risk": _validate_model(RiskReview, raw)}
                )
            )
        return reviewed

    async def load_selected_workflow(
        self,
        state: AgentGraphState,
    ) -> tuple[PlanDraft, PersistedPlan]:
        version_id = UUID(state["selected_workflow_version_id"])
        candidate = next(
            (
                item
                for item in state.get("workflow_candidates", [])
                if item.workflow_version_id == version_id
            ),
            None,
        )
        if candidate is None:
            raise ValueError("Selected Workflow was not in the proposed set")
        version = await self._repository.workflow_version(version_id)
        if version.public_payload_hash != candidate.public_payload_hash:
            raise ValueError(
                "Selected Workflow version changed after proposal"
            )
        plan = PlanDraft.model_validate(version.plan_payload).model_copy(
            update={"execution_mode": ExecutionMode.SINGLE}
        )
        persisted = await self.compile_and_persist_plan(state, plan)
        return plan, persisted

    async def build_plan(
        self,
        state: AgentGraphState,
    ) -> PlanDraft:
        request_review = state["request_risk"]
        context = self._planner_context(state, request_review)
        return await self._planner.plan(context)

    async def compile_and_persist_plan(
        self,
        state: AgentGraphState,
        plan: PlanDraft,
    ) -> PersistedPlan:
        next_revision = state.get("plan_revision_number", 0) + 1
        compiled = []
        for step in plan.steps:
            item = self._compiler.compile(step)
            path = self._compiler.materialize(
                item,
                self._settings.executor_shared_storage_root,
                state["active_task_id"],
                next_revision,
            )
            relative = _executor_source_path(
                path,
                self._settings.executor_shared_storage_root,
            )
            compiled.append((item, relative))
        return await self._repository.persist_plan(
            _task_id(state),
            plan,
            compiled,
            self._registry.registry_snapshot_hash(),
            state.get("revision_feedback"),
        )

    async def review_compiled_code_risk(
        self,
        state: AgentGraphState,
    ) -> RiskReview:
        steps = await self._repository.approved_steps(
            UUID(state["plan_revision_id"])
        )
        review_input = [
            {
                "sequence": item.sequence,
                "purpose": item.purpose,
                "skill": item.skill_ref,
                "tool": item.tool_ref,
                "parameters": item.parameters,
                "source_sha256": item.compiled_source_sha256,
            }
            for item in steps
        ]
        reviewer = self._model.with_structured_output(RiskReview)
        raw = await reviewer.ainvoke(
            [
                SystemMessage(
                    content=(
                        "Review the compiled execution bundle metadata before "
                        "execution. BLOCK critical destructive or malicious "
                        "code. "
                        "Require acknowledgement for high risk. Do not invent "
                        "source code that is not included."
                    )
                ),
                HumanMessage(
                    content=json.dumps(review_input, ensure_ascii=False)
                ),
            ]
        )
        return _validate_model(RiskReview, raw)

    async def verify_approval_and_lock(
        self,
        state: AgentGraphState,
    ) -> None:
        if state.get("review_action") != "APPROVE":
            raise ValueError("Plan approval is required")
        await self._repository.lock_session(_task_id(state))
        await self._repository.update_status(
            _task_id(state),
            TaskStatus.QUEUED_FOR_EXECUTION,
        )

    async def submit_execution(
        self,
        state: AgentGraphState,
    ) -> SubmissionReceipt:
        execution_mode = _state_execution_mode(state)
        steps = list(
            await self._repository.approved_steps(
                UUID(state["plan_revision_id"])
            )
        )
        if execution_mode is ExecutionMode.MULTI:
            steps = steps[:1]
        payloads = [_executor_step(item) for item in steps]
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
            task_id=_task_id(state),
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
        outcome = _executor_outcome(result)
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
            _task_id(state),
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
        decision = _validate_model(MultiDecision, raw)
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
        persisted = await self.compile_and_persist_plan(state, plan)
        steps = await self._repository.approved_steps(
            persisted.plan_revision_id
        )
        binding = await self._repository.binding_for_task(_task_id(state))
        step = steps[0]
        step.sequence = binding.next_step_sequence
        response = await self._executor.append_operation(
            binding.execution_id,
            idempotency_key=(
                f"task:{state['active_task_id']}:operation:"
                f"{binding.next_step_sequence}"
            ),
            expected_version=binding.execution_version,
            steps=[_executor_step(step)],
        )
        if response.operation is None:
            raise ValueError("Executor append response omitted Operation")
        await self._repository.update_binding(
            _task_id(state),
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
        binding = await self._repository.binding_for_task(_task_id(state))
        response = await self._executor.finalize(
            binding.execution_id,
            idempotency_key=f"task:{state['active_task_id']}:finalize",
            expected_version=binding.execution_version,
        )
        await self._repository.update_binding(
            _task_id(state),
            execution_version=response.state.version,
        )

    async def cancel_execution(
        self,
        state: AgentGraphState,
        reason: str | None,
    ) -> None:
        binding = await self._repository.binding_for_task(_task_id(state))
        response = await self._executor.cancel(
            binding.execution_id,
            idempotency_key=f"task:{state['active_task_id']}:cancel",
            actor_type="USER",
            actor_id=state["user_id"],
            reason=reason,
        )
        await self._repository.update_binding(
            _task_id(state),
            execution_version=response.state.version,
        )

    async def build_report_evidence(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        execution_id = UUID(state["execution_id"])
        result = await self._executor.result(execution_id)
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
        return {
            "request": state["user_message"],
            "plan": state["plan"].model_dump(mode="json"),
            "execution_id": str(execution_id),
            "executor_result": result.model_dump(mode="json"),
            "validated_result_summaries": result_summaries,
        }

    async def generate_and_materialize_report(
        self,
        state: AgentGraphState,
        evidence: dict[str, Any],
    ) -> ReportResult:
        raw = await self._model.ainvoke(
            [
                SystemMessage(
                    content=(
                        "Write a Korean Markdown report grounded only in the "
                        "evidence. Include objective, approved plan and "
                        "why each Skill/Tool was selected, execution results, "
                        "limitations, and recommended next work. Do not claim "
                        "failed steps as successful. Keep the entire report "
                        "under 1,800 Korean characters. Use short sections "
                        "and at most three bullets per section. Return only "
                        "Markdown, without a JSON wrapper or code fence."
                    )
                ),
                HumanMessage(content=json.dumps(evidence, ensure_ascii=False)),
            ]
        )
        markdown = _bounded_report_markdown(raw.content)
        report_input = materialize_input_file(
            self._settings.executor_shared_storage_root,
            (f"{state['active_task_id']}/reports/analysis-report.md"),
            markdown,
        )
        artifact = await self._executor.materialize_report(
            UUID(state["execution_id"]),
            idempotency_key=f"task:{state['active_task_id']}:report",
            path=report_input.relative_path,
            sha256=report_input.sha256,
        )
        return ReportResult(
            markdown=markdown,
            artifact_id=artifact.artifact_id,
        )

    async def commit_terminal(
        self,
        state: AgentGraphState,
        *,
        status: TaskStatus,
        message: str,
    ) -> None:
        await self._repository.commit_message(
            _task_id(state),
            message,
            status=status,
            metadata={
                "execution_id": state.get("execution_id"),
                "report_artifact_id": state.get("report_artifact_id"),
            },
        )

    def _planner_context(
        self,
        state: AgentGraphState,
        review: RiskReview,
    ) -> Any:
        from ex_agent.middleware.planning import PlannerContext

        return PlannerContext(
            task_id=state["active_task_id"],
            user_request=state["user_message"],
            planning_kind=PlanningKind(state["planning_kind"]),
            execution_mode=_state_execution_mode(state),
            runtime_profile=state["runtime_profile"],
            request_risk_review_id=(f"request-risk:{state['active_task_id']}"),
            request_risk_allowed=review.recommended_action != "BLOCK",
            revision_feedback=state.get("revision_feedback"),
            registry_snapshot_hash=self._registry.registry_snapshot_hash(),
        )


def _task_id(state: AgentGraphState) -> UUID:
    return UUID(state["active_task_id"])


def _state_execution_mode(state: AgentGraphState) -> ExecutionMode:
    return ExecutionMode(state["execution_mode"])


def _executor_source_path(path: Path, shared_root: Path) -> str:
    request_root = (shared_root.resolve() / "requests").resolve()
    return path.resolve().relative_to(request_root).as_posix()


def _validate_model[ModelT: BaseModel](
    model_type: type[ModelT],
    value: Any,
) -> ModelT:
    if isinstance(value, model_type):
        return value
    return model_type.model_validate(value)


def _message_text(content: Any) -> str:
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        return "\n".join(
            str(item.get("text", "")) if isinstance(item, dict) else str(item)
            for item in content
        )
    return str(content)


def _bounded_report_markdown(content: Any, max_chars: int = 6000) -> str:
    markdown = _message_text(content).strip()
    if not markdown:
        raise ValueError("Report model returned empty Markdown")
    if len(markdown) <= max_chars:
        return markdown
    suffix = "\n\n> 리포트가 길이 제한에 맞게 축약되었습니다."
    return f"{markdown[: max_chars - len(suffix)].rstrip()}{suffix}"


def _executor_step(step: Any) -> dict[str, Any]:
    return executor_step_payload(
        sequence=step.sequence,
        path=step.compiled_source_path,
        sha256=step.compiled_source_sha256,
        timeout_seconds=step.timeout_seconds,
        skill_name=(step.skill_ref or {}).get("name"),
        tool_name=(step.tool_ref or {}).get("name"),
        parameters=step.parameters,
    )


def _executor_outcome(result: ExecutionResult) -> ExecutorOutcome:
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
