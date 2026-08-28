from __future__ import annotations

from typing import Any

from langgraph.types import interrupt
from pydantic import TypeAdapter

from ex_agent.application.ports import WorkflowServices
from ex_agent.domain.contracts import (
    CancelRequestedSignal,
    ClarificationAnswer,
    ExecutionModeDecision,
    ExecutorBoundarySignal,
    ExecutorReconciliation,
    MultiDecision,
    PlanReviewDecision,
    ResumeSignal,
    RiskConfirmationDecision,
    WorkflowSelectionDecision,
)
from ex_agent.domain.enums import (
    ExecutionMode,
    PlanDecisionType,
    PlanningKind,
    RiskLevel,
    TaskStatus,
)
from ex_agent.graph.state import AgentGraphState

_resume_adapter = TypeAdapter(ResumeSignal)


class WorkflowNodes:
    def __init__(self, services: WorkflowServices) -> None:
        self._services = services

    async def hydrate_turn(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        await self._services.update_status(state, TaskStatus.CLASSIFYING)
        return {
            "schema_version": "1.0",
            "phase": TaskStatus.CLASSIFYING,
            "clarification_count": state.get("clarification_count", 0),
            "correction_count": 0,
            "risk_acknowledged": False,
            "runtime_profile": state.get("runtime_profile", "basic"),
        }

    async def classify_intent(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        decision = await self._services.classify_intent(state)
        return {"intent_decision": decision}

    def clarify_request(self, state: AgentGraphState) -> dict[str, Any]:
        decision = state["intent_decision"]
        raw = interrupt(
            {
                "kind": "CLARIFICATION",
                "task_id": state["active_task_id"],
                "question": decision.clarification_question,
            }
        )
        signal = _resume_adapter.validate_python(raw)
        if not isinstance(signal, ClarificationAnswer):
            raise TypeError("ClarificationAnswer is required")
        return {
            "user_message": (
                f"{state['user_message']}\n\nClarification: {signal.answer}"
            ),
            "clarification_count": state.get("clarification_count", 0) + 1,
        }

    async def answer_general(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        await self._services.update_status(state, TaskStatus.ANSWERING)
        answer = await self._services.answer_question(
            state,
            data_analysis=False,
        )
        return {"phase": TaskStatus.ANSWERING, "answer": answer}

    async def answer_data_question(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        await self._services.update_status(state, TaskStatus.ANSWERING)
        answer = await self._services.answer_question(
            state,
            data_analysis=True,
        )
        return {"phase": TaskStatus.ANSWERING, "answer": answer}

    async def commit_answer(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        await self._services.commit_answer(state, state["answer"])
        return {"phase": TaskStatus.SUCCEEDED}

    def choose_execution_mode(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        raw = interrupt(
            {
                "kind": "EXECUTION_MODE",
                "task_id": state["active_task_id"],
                "options": ["SINGLE", "MULTI"],
            }
        )
        signal = _resume_adapter.validate_python(raw)
        if not isinstance(signal, ExecutionModeDecision):
            raise TypeError("ExecutionModeDecision is required")
        return {
            "execution_mode": signal.mode,
            "planning_kind": PlanningKind.CUSTOM_CODE,
        }

    async def review_request_risk(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        review = await self._services.review_request_risk(state)
        return {"request_risk": review}

    def confirm_request_risk(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        raw = interrupt(
            {
                "kind": "REQUEST_RISK_CONFIRMATION",
                "task_id": state["active_task_id"],
                "risk": state["request_risk"].model_dump(mode="json"),
            }
        )
        signal = _resume_adapter.validate_python(raw)
        if not isinstance(signal, RiskConfirmationDecision):
            raise TypeError("RiskConfirmationDecision is required")
        if not signal.confirmed:
            return {
                "request_risk": state["request_risk"].model_copy(
                    update={"level": RiskLevel.CRITICAL}
                )
            }
        return {"risk_acknowledged": True}

    async def search_workflows(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        candidates = await self._services.search_workflows(state)
        return {
            "workflow_candidates": candidates,
            "workflow_proposal_version": 1,
            "execution_mode": ExecutionMode.MULTI,
            "planning_kind": PlanningKind.TOOL_PLAN,
        }

    def choose_workflow(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        raw = interrupt(
            {
                "kind": "WORKFLOW_SELECTION",
                "task_id": state["active_task_id"],
                "proposal_version": state["workflow_proposal_version"],
                "candidates": [
                    item.model_dump(mode="json")
                    for item in state["workflow_candidates"]
                ],
            }
        )
        signal = _resume_adapter.validate_python(raw)
        if not isinstance(signal, WorkflowSelectionDecision):
            raise TypeError("WorkflowSelectionDecision is required")
        if signal.proposal_version != state["workflow_proposal_version"]:
            raise ValueError("Stale Workflow proposal decision")
        if signal.workflow_version_id:
            candidate = next(
                (
                    item
                    for item in state["workflow_candidates"]
                    if item.workflow_version_id == signal.workflow_version_id
                ),
                None,
            )
            if candidate is None:
                raise ValueError("Workflow was not in the proposed set")
            if signal.public_payload_hash != candidate.public_payload_hash:
                raise ValueError("Stale Workflow payload decision")
            if (
                candidate.risk is not None
                and candidate.risk.level is RiskLevel.HIGH
                and not signal.risk_acknowledged
            ):
                raise ValueError("HIGH risk Workflow requires acknowledgement")
        return {
            "selected_workflow_version_id": (
                str(signal.workflow_version_id)
                if signal.workflow_version_id
                else ""
            ),
            "execution_mode": (
                ExecutionMode.SINGLE
                if signal.workflow_version_id
                else ExecutionMode.MULTI
            ),
            "planning_kind": (
                PlanningKind.FIXED_WORKFLOW
                if signal.workflow_version_id
                else PlanningKind.TOOL_PLAN
            ),
            "review_action": (
                PlanDecisionType.APPROVE.value
                if signal.workflow_version_id
                else ""
            ),
            "risk_acknowledged": signal.risk_acknowledged,
        }

    async def load_selected_workflow(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        plan, persisted = await self._services.load_selected_workflow(state)
        return _persisted_plan_updates(plan, persisted)

    async def build_plan(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        await self._services.update_status(state, TaskStatus.PLANNING)
        plan = await self._services.build_plan(state)
        return {"phase": TaskStatus.PLANNING, "plan": plan}

    async def compile_and_persist_plan(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        persisted = await self._services.compile_and_persist_plan(
            state,
            state["plan"],
        )
        return _persisted_plan_updates(state["plan"], persisted)

    async def review_compiled_code_risk(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        review = await self._services.review_compiled_code_risk(state)
        return {"code_risk": review}

    def review_plan(self, state: AgentGraphState) -> dict[str, Any]:
        raw = interrupt(
            {
                "kind": "PLAN_REVIEW",
                "task_id": state["active_task_id"],
                "plan_revision_id": state["plan_revision_id"],
                "plan_revision_number": state["plan_revision_number"],
                "public_payload_hash": (state["plan_public_payload_hash"]),
                "plan": state["plan"].model_dump(mode="json"),
                "risk": state["code_risk"].model_dump(mode="json"),
            }
        )
        signal = _resume_adapter.validate_python(raw)
        if not isinstance(signal, PlanReviewDecision):
            raise TypeError("PlanReviewDecision is required")
        if str(signal.plan_revision_id) != state["plan_revision_id"]:
            raise ValueError("Stale Plan revision decision")
        if signal.plan_revision_number != state["plan_revision_number"]:
            raise ValueError("Stale Plan revision number")
        if signal.public_payload_hash != state["plan_public_payload_hash"]:
            raise ValueError("Stale Plan payload decision")
        if (
            signal.decision is PlanDecisionType.APPROVE
            and state["code_risk"].level is RiskLevel.HIGH
            and not signal.risk_acknowledged
        ):
            raise ValueError("HIGH risk Plan requires acknowledgement")
        return {
            "review_action": signal.decision.value,
            "revision_feedback": signal.feedback or "",
            "risk_acknowledged": signal.risk_acknowledged,
        }

    async def verify_approval(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        await self._services.verify_approval_and_lock(state)
        return {"phase": TaskStatus.QUEUED_FOR_EXECUTION}

    async def submit_execution(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        receipt = await self._services.submit_execution(state)
        return {
            "phase": TaskStatus.WAITING_FOR_EXECUTOR_EVENT,
            "execution_id": str(receipt.execution_id),
            "current_operation_id": str(receipt.operation_id),
        }

    def wait_external_signal(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        raw = interrupt(
            {
                "kind": "EXECUTOR_EVENT",
                "task_id": state["active_task_id"],
                "execution_id": state["execution_id"],
                "last_event_sequence": state.get(
                    "last_executor_event_sequence",
                    0,
                ),
                "cancellable": True,
            }
        )
        signal = _resume_adapter.validate_python(raw)
        if not isinstance(
            signal,
            ExecutorBoundarySignal | CancelRequestedSignal,
        ):
            raise TypeError("Executor boundary or cancel signal is required")
        return {"external_signal": signal.model_dump(mode="json")}

    async def reconcile_executor(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        signal = ExecutorBoundarySignal.model_validate(
            state["external_signal"]
        )
        result = await self._services.reconcile_executor(state, signal)
        return {
            "executor_reconciliation": result.model_dump(mode="json"),
            "last_executor_event_sequence": signal.event_sequence,
        }

    async def adapt_multi_plan(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        reconciliation = ExecutorReconciliation.model_validate(
            state["executor_reconciliation"]
        )
        decision = await self._services.adapt_multi_plan(
            state,
            reconciliation,
        )
        updates: dict[str, Any] = {
            "multi_action": decision.action.value,
            "multi_decision": decision.model_dump(mode="json"),
        }
        if reconciliation.outcome.value == "OPERATION_FAILED":
            updates["correction_count"] = state.get("correction_count", 0) + 1
        if decision.next_step is not None:
            plan = state["plan"].model_copy(
                update={"steps": [decision.next_step]}
            )
            updates["plan"] = plan
        return updates

    async def append_operation(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        decision = MultiDecision.model_validate(state["multi_decision"])
        receipt = await self._services.append_operation(state, decision)
        return {"current_operation_id": str(receipt.operation_id)}

    async def finalize_execution(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        await self._services.finalize_execution(state)
        return {"execution_finalized": True}

    async def cancel_execution(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        signal = CancelRequestedSignal.model_validate(state["external_signal"])
        await self._services.cancel_execution(state, signal.reason)
        return {"phase": TaskStatus.CANCEL_REQUESTED}

    async def build_report_evidence(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        evidence = await self._services.build_report_evidence(state)
        return {
            "phase": TaskStatus.GENERATING_REPORT,
            "report_evidence": evidence,
        }

    async def generate_report(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        report = await self._services.generate_and_materialize_report(
            state,
            state["report_evidence"],
        )
        return {
            "report_markdown": report.markdown,
            "report_artifact_id": str(report.artifact_id),
        }

    async def commit_success(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        await self._services.commit_terminal(
            state,
            status=TaskStatus.SUCCEEDED,
            message=state["report_markdown"],
        )
        return {"phase": TaskStatus.SUCCEEDED}

    async def commit_rejected(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        message = "실행 계획이 거절되어 작업을 종료했습니다."
        await self._services.commit_terminal(
            state,
            status=TaskStatus.REJECTED,
            message=message,
        )
        return {"phase": TaskStatus.REJECTED, "terminal_message": message}

    async def commit_blocked(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        review = state.get("code_risk") or state["request_risk"]
        message = f"위험 판정으로 작업을 차단했습니다: {review.summary}"
        await self._services.commit_terminal(
            state,
            status=TaskStatus.FAILED,
            message=message,
        )
        return {"phase": TaskStatus.FAILED, "terminal_message": message}

    async def commit_failed(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        result = ExecutorReconciliation.model_validate(
            state["executor_reconciliation"]
        )
        message = result.error_message or "코드 실행에 실패했습니다."
        await self._services.commit_terminal(
            state,
            status=TaskStatus.FAILED,
            message=message,
        )
        return {"phase": TaskStatus.FAILED, "terminal_message": message}

    async def commit_cancelled(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        message = "Executor 취소 완료를 확인했습니다."
        await self._services.commit_terminal(
            state,
            status=TaskStatus.CANCELLED,
            message=message,
        )
        return {
            "phase": TaskStatus.CANCELLED,
            "terminal_message": message,
        }


def _persisted_plan_updates(
    plan: Any,
    persisted: Any,
) -> dict[str, Any]:
    return {
        "plan": plan,
        "plan_id": str(persisted.plan_id),
        "plan_revision_id": str(persisted.plan_revision_id),
        "plan_revision_number": persisted.plan_revision_number,
        "plan_public_payload_hash": persisted.public_payload_hash,
        "compiled_bundle_id": str(persisted.compiled_bundle_id),
    }
