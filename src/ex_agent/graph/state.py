from typing import Any, TypedDict

from ex_agent.domain.contracts import (
    IntentDecision,
    PlanDraft,
    RiskReview,
    WorkflowCandidate,
)
from ex_agent.domain.enums import ExecutionMode, PlanningKind, TaskStatus


class AgentGraphState(TypedDict, total=False):
    schema_version: str
    user_id: str
    project_id: str
    session_id: str
    active_task_id: str
    current_input_message_id: str
    user_message: str
    phase: TaskStatus
    intent_decision: IntentDecision
    clarification_count: int
    planning_kind: PlanningKind
    execution_mode: ExecutionMode
    runtime_profile: str
    workflow_candidates: list[WorkflowCandidate]
    workflow_proposal_version: int
    selected_workflow_version_id: str
    plan: PlanDraft
    plan_id: str
    plan_revision_id: str
    plan_revision_number: int
    plan_public_payload_hash: str
    request_risk: RiskReview
    code_risk: RiskReview
    risk_acknowledged: bool
    compiled_bundle_id: str
    execution_id: str
    current_operation_id: str
    last_executor_event_sequence: int
    correction_count: int
    execution_finalized: bool
    executor_reconciliation: dict[str, Any]
    answer: str
    report_markdown: str
    report_artifact_id: str
    report_evidence: dict[str, Any]
    revision_feedback: str
    review_action: str
    external_signal: dict[str, Any]
    multi_action: str
    multi_decision: dict[str, Any]
    terminal_reason_code: str
    terminal_message: str
