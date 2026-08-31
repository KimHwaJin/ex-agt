from __future__ import annotations

from datetime import datetime
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


class ResourceAuditFields(ContractModel):
    created_at: datetime
    updated_at: datetime
    created_by: str
    updated_by: str


class IntentDecision(ContractModel):
    """Semantic intent, including conversation that needs no execution."""

    intent: Intent = Field(
        description=(
            "GENERAL_QA: chat, greetings, general answers/code examples; "
            "DATA_ANALYSIS_QA: analysis explanations/advice without running; "
            "DATA_ANALYSIS_EXECUTION: actually analyze/retrieve data; "
            "CODE_EXECUTION: actually run other code."
        )
    )
    confidence: float = Field(ge=0, le=1)
    decision_summary: str = Field(min_length=1, max_length=500)
    requires_clarification: bool = Field(
        default=False,
        description=(
            "True only if the intended work is genuinely ambiguous. "
            "Greetings, concept questions and missing execution mode "
            "do not require clarification."
        ),
    )
    clarification_question: str | None = Field(
        default=None,
        max_length=1000,
        description=(
            "One question in the user's language about ambiguous intent, "
            "otherwise null. Never ask for SINGLE/MULTI here."
        ),
    )
    requires_execution_mode: bool = Field(
        default=False,
        description=(
            "True only for CODE_EXECUTION. A separate workflow node asks "
            "the user to select the mode, not the classifier."
        ),
    )

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
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "idempotency_key": "workflow-promote-0001",
                "name": "월별 매출 분석",
                "description": "월별 매출 추이와 변동을 분석합니다.",
                "request_examples": ["지난달 매출을 분석해줘"],
                "tags": ["revenue", "monthly"],
                "public_parameter_defaults": {},
            }
        },
    )

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
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "idempotency_key": "workflow-version-0002",
                "source_task_id": ("00000000-0000-4000-8000-000000000003"),
                "request_examples": ["최신 매출을 분석해줘"],
                "tags": ["revenue"],
                "public_parameter_defaults": {},
            }
        },
    )

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
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "idempotency_key": "workflow-review-0002",
                "decision": "APPROVE",
                "reason": "검증 완료",
            }
        },
    )

    idempotency_key: str = Field(min_length=1, max_length=255)
    decision: Literal["APPROVE", "REJECT"]
    reason: str | None = Field(default=None, max_length=4000)


class WorkflowVersionActivationRequest(ContractModel):
    idempotency_key: str = Field(min_length=1, max_length=255)
    reason: str | None = Field(default=None, max_length=4000)


class WorkflowStatusRequest(ContractModel):
    model_config = ConfigDict(
        extra="forbid",
        json_schema_extra={
            "example": {
                "idempotency_key": "workflow-status-0001",
                "status": "INACTIVE",
                "reason": "운영 점검",
            }
        },
    )

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


class WorkflowOperationsView(ResourceAuditFields):
    workflow_id: UUID
    name: str
    description: str
    owner_user_id: str | None
    owner_project_id: str | None
    visibility: str
    status: Literal["ACTIVE", "INACTIVE"]
    latest_version: int = Field(ge=1)
    active_workflow_version_id: UUID | None
    active_version: int | None = Field(default=None, ge=1)
    access_policy: dict[str, Any]
    required_permission: str | None


class WorkflowVersionSummary(ResourceAuditFields):
    workflow_version_id: UUID
    workflow_id: UUID
    version: int = Field(ge=1)
    source_task_id: UUID | None
    source_plan_id: UUID | None
    source_plan_revision_id: UUID | None
    source_execution_id: UUID | None
    objective: str | None
    strategy_summary: str | None
    runtime_profile: str | None
    tool_registry_snapshot_hash: str | None
    embedding_model: str | None
    embedding_dimension: int | None
    request_examples: list[str]
    tags: list[str]
    promotion_policy_version: str | None
    public_payload_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    promoted_by: str
    active: bool
    review_status: Literal["PENDING_REVIEW", "APPROVED", "REJECTED"]
    reviewed_by: str | None
    reviewed_at: datetime | None
    review_reason: str | None


class WorkflowStepView(ContractModel):
    sequence: int = Field(ge=0)
    source_plan_revision_id: UUID | None
    skill_ref: dict[str, Any]
    tool_ref: dict[str, Any]
    purpose: str
    selection_rationale: str
    parameter_template: dict[str, Any]
    expected_outputs: list[str]
    validation_criteria: list[str]
    timeout_seconds: int = Field(gt=0)


class WorkflowVersionDetail(WorkflowVersionSummary):
    input_contract: dict[str, dict[str, Any]]
    output_contract: dict[str, Any]
    plan: PlanDraft
    steps: list[WorkflowStepView]


class WorkflowVersionPage(ContractModel):
    items: list[WorkflowVersionSummary]
    next_cursor: str | None = None
    has_more: bool


class WorkflowLifecycleActionView(ResourceAuditFields):
    action_id: UUID
    workflow_id: UUID
    workflow_version_id: UUID | None
    actor_user_id: str
    action: str
    idempotency_key: str
    request_hash: str = Field(pattern=r"^[0-9a-f]{64}$")
    reason: str | None
    policy_version: str
    result: WorkflowLifecycleResult


class WorkflowLifecycleActionPage(ContractModel):
    items: list[WorkflowLifecycleActionView]
    next_cursor: str | None = None
    has_more: bool


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
