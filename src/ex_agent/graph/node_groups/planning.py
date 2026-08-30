from __future__ import annotations

from typing import Any

from langgraph.types import interrupt

from ex_agent.domain.contracts import (
    ExecutionModeDecision,
    PlanReviewDecision,
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
from ex_agent.graph.node_groups.common import (
    WorkflowNodeGroup,
    persisted_plan_updates,
    validate_resume_signal,
)
from ex_agent.graph.state import AgentGraphState


class PlanningNodes(WorkflowNodeGroup):
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
        signal = validate_resume_signal(raw)
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
        signal = validate_resume_signal(raw)
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
        signal = validate_resume_signal(raw)
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
        return persisted_plan_updates(plan, persisted)

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
        return persisted_plan_updates(state["plan"], persisted)

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
                "public_payload_hash": state["plan_public_payload_hash"],
                "plan": state["plan"].model_dump(mode="json"),
                "risk": state["code_risk"].model_dump(mode="json"),
            }
        )
        signal = validate_resume_signal(raw)
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


__all__ = ["PlanningNodes"]
