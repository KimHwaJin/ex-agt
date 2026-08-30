from __future__ import annotations

from typing import Protocol

from ex_agent.application.state import AgentGraphState
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
from ex_agent.domain.enums import TaskStatus


class WorkflowServices(Protocol):
    async def update_status(
        self,
        state: AgentGraphState,
        status: TaskStatus,
    ) -> None: ...

    async def classify_intent(
        self,
        state: AgentGraphState,
    ) -> IntentDecision: ...

    async def answer_question(
        self,
        state: AgentGraphState,
        *,
        data_analysis: bool,
    ) -> str: ...

    async def commit_answer(
        self,
        state: AgentGraphState,
        answer: str,
    ) -> None: ...

    async def review_request_risk(
        self,
        state: AgentGraphState,
    ) -> RiskReview: ...

    async def search_workflows(
        self,
        state: AgentGraphState,
    ) -> list[WorkflowCandidate]: ...

    async def load_selected_workflow(
        self,
        state: AgentGraphState,
    ) -> tuple[PlanDraft, PersistedPlan]: ...

    async def build_plan(
        self,
        state: AgentGraphState,
    ) -> PlanDraft: ...

    async def compile_and_persist_plan(
        self,
        state: AgentGraphState,
        plan: PlanDraft,
    ) -> PersistedPlan: ...

    async def review_compiled_code_risk(
        self,
        state: AgentGraphState,
    ) -> RiskReview: ...

    async def verify_approval_and_lock(
        self,
        state: AgentGraphState,
    ) -> None: ...

    async def submit_execution(
        self,
        state: AgentGraphState,
    ) -> SubmissionReceipt: ...

    async def reconcile_executor(
        self,
        state: AgentGraphState,
        signal: ExecutorBoundarySignal,
    ) -> ExecutorReconciliation: ...

    async def adapt_multi_plan(
        self,
        state: AgentGraphState,
        reconciliation: ExecutorReconciliation,
    ) -> MultiDecision: ...

    async def append_operation(
        self,
        state: AgentGraphState,
        decision: MultiDecision,
    ) -> SubmissionReceipt: ...

    async def finalize_execution(
        self,
        state: AgentGraphState,
    ) -> None: ...

    async def cancel_execution(
        self,
        state: AgentGraphState,
        reason: str | None,
    ) -> None: ...

    async def build_report_evidence(
        self,
        state: AgentGraphState,
    ) -> dict: ...

    async def generate_and_materialize_report(
        self,
        state: AgentGraphState,
        evidence: dict,
    ) -> ReportResult: ...

    async def commit_terminal(
        self,
        state: AgentGraphState,
        *,
        status: TaskStatus,
        message: str,
    ) -> None: ...
