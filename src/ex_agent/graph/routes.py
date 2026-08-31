from typing import Literal

from ex_agent.domain.contracts import ExecutorReconciliation
from ex_agent.domain.enums import (
    ExecutionMode,
    ExecutorOutcome,
    Intent,
    MultiAction,
    PlanningKind,
    RiskLevel,
)
from ex_agent.graph.state import AgentGraphState


def route_intent(
    state: AgentGraphState,
) -> Literal[
    "clarify_request",
    "answer_general",
    "answer_data_question",
    "review_request_risk",
    "choose_execution_mode",
]:
    decision = state["intent_decision"]
    if decision.requires_clarification:
        return "clarify_request"
    if decision.intent is Intent.GENERAL_QA:
        return "answer_general"
    if decision.intent is Intent.DATA_ANALYSIS_QA:
        return "answer_data_question"
    if decision.intent is Intent.DATA_ANALYSIS_EXECUTION:
        return "review_request_risk"
    if decision.requested_execution_mode is not None:
        return "review_request_risk"
    return "choose_execution_mode"


def route_request_risk(
    state: AgentGraphState,
) -> Literal[
    "commit_blocked",
    "confirm_request_risk",
    "search_workflows",
    "build_plan",
]:
    review = state["request_risk"]
    if review.level is RiskLevel.CRITICAL:
        return "commit_blocked"
    if review.level is RiskLevel.HIGH and not state.get(
        "risk_acknowledged",
        False,
    ):
        return "confirm_request_risk"
    if state["intent_decision"].intent is Intent.DATA_ANALYSIS_EXECUTION:
        return "search_workflows"
    return "build_plan"


def route_workflow_search(
    state: AgentGraphState,
) -> Literal["choose_workflow", "build_plan"]:
    if state.get("workflow_candidates"):
        return "choose_workflow"
    return "build_plan"


def route_workflow_decision(
    state: AgentGraphState,
) -> Literal["load_selected_workflow", "build_plan"]:
    if state.get("selected_workflow_version_id"):
        return "load_selected_workflow"
    return "build_plan"


def route_code_risk(
    state: AgentGraphState,
) -> Literal["commit_blocked", "review_plan", "verify_approval"]:
    if state["code_risk"].level is RiskLevel.CRITICAL:
        return "commit_blocked"
    if PlanningKind(state["planning_kind"]) is PlanningKind.FIXED_WORKFLOW:
        if state["code_risk"].level is RiskLevel.HIGH and not state.get(
            "risk_acknowledged", False
        ):
            return "review_plan"
        return "verify_approval"
    return "review_plan"


def route_plan_review(
    state: AgentGraphState,
) -> Literal["build_plan", "commit_rejected", "verify_approval"]:
    if state["review_action"] == "REVISE":
        return "build_plan"
    if state["review_action"] == "REJECT":
        return "commit_rejected"
    return "verify_approval"


def route_approved_execution(
    state: AgentGraphState,
) -> Literal["submit_execution", "append_operation"]:
    if state.get("execution_id"):
        return "append_operation"
    return "submit_execution"


def route_external_signal(
    state: AgentGraphState,
) -> Literal["cancel_execution", "reconcile_executor"]:
    if state["external_signal"]["type"] == "CANCEL_REQUESTED":
        return "cancel_execution"
    return "reconcile_executor"


def route_reconciliation(
    state: AgentGraphState,
) -> Literal[
    "wait_external_signal",
    "adapt_multi_plan",
    "build_report_evidence",
    "commit_failed",
    "commit_cancelled",
]:
    result = ExecutorReconciliation.model_validate(
        state["executor_reconciliation"]
    )
    if result.outcome is ExecutorOutcome.WAITING:
        return "wait_external_signal"
    if result.outcome is ExecutorOutcome.SUCCEEDED:
        return "build_report_evidence"
    if result.outcome is ExecutorOutcome.CANCELLED:
        return "commit_cancelled"
    if ExecutionMode(state["execution_mode"]) is ExecutionMode.MULTI and (
        result.outcome
        in {
            ExecutorOutcome.OPERATION_SUCCEEDED,
            ExecutorOutcome.OPERATION_FAILED,
        }
    ):
        return "adapt_multi_plan"
    return "commit_failed"


def route_multi_action(
    state: AgentGraphState,
) -> Literal[
    "append_operation",
    "compile_and_persist_plan",
    "finalize_execution",
    "commit_failed",
]:
    action = MultiAction(state["multi_action"])
    if action is MultiAction.APPEND_STEP:
        return "append_operation"
    if action is MultiAction.REQUIRE_REAPPROVAL:
        return "compile_and_persist_plan"
    if action is MultiAction.FINALIZE:
        return "finalize_execution"
    return "commit_failed"
