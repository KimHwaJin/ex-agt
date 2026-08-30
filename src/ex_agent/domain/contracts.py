from __future__ import annotations

from typing import Annotated, Any, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, model_validator

from ex_agent.domain.enums import (
    ExecutionMode,
    ExecutorOutcome,
    Intent,
    MultiAction,
    PlanDecisionType,
    PlanningKind,
    ResumeSignalType,
    RiskLevel,
)


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class IntentDecision(ContractModel):
    intent: Intent
    confidence: float = Field(ge=0, le=1)
    decision_summary: str = Field(min_length=1, max_length=500)
    requires_clarification: bool = False
    clarification_question: str | None = Field(
        default=None,
        max_length=1000,
    )
    requires_execution_mode: bool = False

    @model_validator(mode="after")
    def validate_clarification(self) -> IntentDecision:
        if self.requires_clarification and not self.clarification_question:
            raise ValueError(
                "clarification_question is required when clarification is "
                "requested"
            )
        return self


class RiskReview(ContractModel):
    level: RiskLevel
    categories: list[str] = Field(default_factory=list)
    summary: str = Field(min_length=1, max_length=2000)
    evidence: list[str] = Field(default_factory=list)
    recommended_action: Literal[
        "ALLOW",
        "WARN",
        "REQUIRE_CONFIRMATION",
        "BLOCK",
    ]


class SkillReference(ContractModel):
    name: str = Field(min_length=1, max_length=128)
    version: str = Field(min_length=1, max_length=64)
    content_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class ToolReference(ContractModel):
    name: str = Field(min_length=1, max_length=128)
    version: str = Field(min_length=1, max_length=64)
    source_sha256: str = Field(pattern=r"^[0-9a-f]{64}$")


class PlanStepDraft(ContractModel):
    sequence: int = Field(ge=0)
    title: str = Field(min_length=1, max_length=200)
    purpose: str = Field(min_length=1, max_length=2000)
    planning_kind: PlanningKind
    skill: SkillReference | None = None
    tool: ToolReference | None = None
    parameters: dict[str, Any] = Field(default_factory=dict)
    selection_rationale: str = Field(min_length=1, max_length=2000)
    expected_outputs: list[str] = Field(default_factory=list)
    validation_criteria: list[str] = Field(default_factory=list)
    timeout_seconds: int = Field(default=300, ge=1, le=432000)
    custom_code: str | None = None

    @model_validator(mode="after")
    def validate_lineage(self) -> PlanStepDraft:
        if self.planning_kind is PlanningKind.TOOL_PLAN:
            if self.skill is None or self.tool is None:
                raise ValueError(
                    "Tool plan steps require skill and tool references"
                )
            if self.custom_code is not None:
                raise ValueError("Tool plan steps cannot include custom code")
        if self.planning_kind is PlanningKind.CUSTOM_CODE:
            if not self.custom_code:
                raise ValueError("Custom code steps require source code")
            if self.skill is not None or self.tool is not None:
                raise ValueError(
                    "Custom code steps cannot claim registered lineage"
                )
        return self


class PlanDraft(ContractModel):
    objective: str = Field(min_length=1, max_length=2000)
    strategy_summary: str = Field(min_length=1, max_length=4000)
    execution_mode: ExecutionMode
    runtime_profile: str = Field(default="basic", min_length=1)
    steps: list[PlanStepDraft] = Field(min_length=1, max_length=100)
    assumptions: list[str] = Field(default_factory=list)
    expected_artifacts: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_sequences(self) -> PlanDraft:
        sequences = [step.sequence for step in self.steps]
        if sequences != list(range(len(self.steps))):
            raise ValueError("Plan step sequences must start at zero")
        return self


class CompiledStep(ContractModel):
    sequence: int
    source: str
    source_sha256: str
    skill_name: str | None
    tool_name: str | None
    parameters: dict[str, Any]


class WorkflowCandidate(ContractModel):
    workflow_version_id: UUID
    name: str
    description: str
    score: float
    plan: PlanDraft
    input_contract: dict[str, dict[str, Any]] = Field(default_factory=dict)
    public_payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    risk: RiskReview | None = None


class WorkflowPromotionDraft(ContractModel):
    task_id: UUID
    eligible: bool
    reason: str | None = None
    suggested_name: str | None = Field(default=None, max_length=255)
    suggested_description: str | None = Field(default=None, max_length=4000)
    suggested_request_examples: list[str] = Field(default_factory=list)
    suggested_tags: list[str] = Field(default_factory=list)
    steps: list[PlanStepDraft] = Field(default_factory=list)
    parameter_inputs: dict[str, dict[str, Any]] = Field(default_factory=dict)


class WorkflowPromotionRequest(ContractModel):
    idempotency_key: str = Field(min_length=1, max_length=255)
    name: str = Field(min_length=1, max_length=255)
    description: str = Field(min_length=1, max_length=4000)
    request_examples: list[
        Annotated[str, Field(min_length=1, max_length=1000)]
    ] = Field(default_factory=list, max_length=20)
    tags: list[Annotated[str, Field(min_length=1, max_length=128)]] = Field(
        default_factory=list, max_length=50
    )
    public_parameter_defaults: dict[str, Any] = Field(default_factory=dict)


class WorkflowPromotionResult(ContractModel):
    workflow_id: UUID
    workflow_version_id: UUID
    version: int = Field(ge=1)
    created: bool
    public_payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class WorkflowVersionCreateRequest(ContractModel):
    idempotency_key: str = Field(min_length=1, max_length=255)
    source_task_id: UUID
    request_examples: list[
        Annotated[str, Field(min_length=1, max_length=1000)]
    ] = Field(default_factory=list, max_length=20)
    tags: list[Annotated[str, Field(min_length=1, max_length=128)]] = Field(
        default_factory=list, max_length=50
    )
    public_parameter_defaults: dict[str, Any] = Field(default_factory=dict)


class WorkflowVersionReviewRequest(ContractModel):
    idempotency_key: str = Field(min_length=1, max_length=255)
    decision: Literal["APPROVE", "REJECT"]
    reason: str | None = Field(default=None, max_length=4000)


class WorkflowVersionActivationRequest(ContractModel):
    idempotency_key: str = Field(min_length=1, max_length=255)
    reason: str | None = Field(default=None, max_length=4000)


class WorkflowStatusRequest(ContractModel):
    idempotency_key: str = Field(min_length=1, max_length=255)
    status: Literal["ACTIVE", "INACTIVE"]
    reason: str | None = Field(default=None, max_length=4000)


class WorkflowLifecycleResult(ContractModel):
    workflow_id: UUID
    workflow_version_id: UUID | None = None
    version: int | None = Field(default=None, ge=1)
    public_payload_hash: str | None = Field(
        default=None,
        pattern=r"^[0-9a-f]{64}$",
    )
    action: Literal[
        "VERSION_CREATED",
        "VERSION_APPROVED",
        "VERSION_REJECTED",
        "VERSION_ACTIVATED",
        "WORKFLOW_ACTIVATED",
        "WORKFLOW_DEACTIVATED",
    ]
    workflow_status: Literal["ACTIVE", "INACTIVE"]
    review_status: Literal["PENDING_REVIEW", "APPROVED", "REJECTED"] | None
    version_active: bool | None
    applied: bool


class PlanReviewDecision(ContractModel):
    type: Literal[ResumeSignalType.PLAN_REVIEW] = ResumeSignalType.PLAN_REVIEW
    decision: PlanDecisionType
    plan_revision_id: UUID
    plan_revision_number: int = Field(ge=1)
    public_payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    feedback: str | None = Field(default=None, max_length=5000)
    risk_acknowledged: bool = False


class WorkflowSelectionDecision(ContractModel):
    type: Literal[ResumeSignalType.WORKFLOW_SELECTION] = (
        ResumeSignalType.WORKFLOW_SELECTION
    )
    workflow_version_id: UUID | None = None
    proposal_version: int = Field(ge=1)
    public_payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    input_values: dict[str, Any] = Field(default_factory=dict)
    risk_acknowledged: bool = False


class ExecutionModeDecision(ContractModel):
    type: Literal[ResumeSignalType.EXECUTION_MODE] = (
        ResumeSignalType.EXECUTION_MODE
    )
    mode: ExecutionMode


class RiskConfirmationDecision(ContractModel):
    type: Literal[ResumeSignalType.REQUEST_RISK_CONFIRMATION] = (
        ResumeSignalType.REQUEST_RISK_CONFIRMATION
    )
    confirmed: bool


class ClarificationAnswer(ContractModel):
    type: Literal[ResumeSignalType.CLARIFICATION] = (
        ResumeSignalType.CLARIFICATION
    )
    answer: str = Field(min_length=1, max_length=10000)


class ExecutorBoundarySignal(ContractModel):
    type: Literal[ResumeSignalType.EXECUTOR_BOUNDARY] = (
        ResumeSignalType.EXECUTOR_BOUNDARY
    )
    execution_id: UUID
    event_id: UUID
    event_sequence: int = Field(ge=1)
    event_type: Literal[
        "execution.operation_completed",
        "execution.completed",
    ]


class CancelRequestedSignal(ContractModel):
    type: Literal[ResumeSignalType.CANCEL_REQUESTED] = (
        ResumeSignalType.CANCEL_REQUESTED
    )
    task_id: UUID
    reason: str | None = Field(default=None, max_length=1000)


ResumeSignal = Annotated[
    ClarificationAnswer
    | WorkflowSelectionDecision
    | ExecutionModeDecision
    | RiskConfirmationDecision
    | PlanReviewDecision
    | ExecutorBoundarySignal
    | CancelRequestedSignal,
    Field(discriminator="type"),
]


class ExecutorReconciliation(ContractModel):
    outcome: ExecutorOutcome
    execution_id: UUID
    execution_version: int = Field(ge=0)
    operation_id: UUID | None = None
    error_code: str | None = None
    error_message: str | None = None
    result_refs: list[str] = Field(default_factory=list)
    result_summaries: list[dict[str, Any]] = Field(default_factory=list)


class MultiDecision(ContractModel):
    action: MultiAction
    rationale: str = Field(min_length=1, max_length=2000)
    next_step: PlanStepDraft | None = None
    requires_reapproval_reasons: list[str] = Field(default_factory=list)

    @model_validator(mode="after")
    def validate_action(self) -> MultiDecision:
        if self.action is MultiAction.APPEND_STEP and self.next_step is None:
            raise ValueError("APPEND_STEP requires next_step")
        return self


class PersistedPlan(ContractModel):
    plan_id: UUID
    plan_revision_id: UUID
    plan_revision_number: int = Field(ge=1)
    public_payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    compiled_bundle_id: UUID


class SubmissionReceipt(ContractModel):
    execution_id: UUID
    operation_id: UUID
    execution_version: int = Field(ge=0)


class ReportResult(ContractModel):
    markdown: str = Field(min_length=1)
    artifact_id: UUID
