from __future__ import annotations

import json
import logging
from typing import Any
from uuid import UUID

from langchain_core.embeddings import Embeddings
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from ex_agent.application.capabilities.common import (
    executor_source_path,
    state_execution_mode,
    task_id,
    validate_model,
)
from ex_agent.application.promotions import bind_workflow_inputs
from ex_agent.application.state import AgentGraphState
from ex_agent.config import Settings
from ex_agent.domain.contracts import (
    PersistedPlan,
    PlanDraft,
    RiskReview,
    WorkflowCandidate,
)
from ex_agent.domain.enums import PlanningKind, TaskStatus
from ex_agent.persistence.repository import AgentRepository
from ex_agent.planners.agent import PlannerAgent
from ex_agent.tools.compiler import SourceCompiler
from ex_agent.tools.registry import ToolRegistry

logger = logging.getLogger(__name__)


class PlanningCapability:
    """Workflow retrieval, planning, compilation, and approval."""

    def __init__(
        self,
        settings: Settings,
        repository: AgentRepository,
        registry: ToolRegistry,
        model: BaseChatModel,
        embeddings: Embeddings,
        planner: PlannerAgent,
        compiler: SourceCompiler,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._registry = registry
        self._model = model
        self._embeddings = embeddings
        self._planner = planner
        self._compiler = compiler

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
                task_id(state),
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
                            "Assess the proposed fixed analysis Workflow "
                            "risk from its plan and parameters. Use HIGH "
                            "when explicit user acknowledgement is warranted "
                            "and CRITICAL only for a plan that must not "
                            "execute."
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
                    update={"risk": validate_model(RiskReview, raw)}
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
        plan = bind_workflow_inputs(
            PlanDraft.model_validate(version.plan_payload),
            version.input_contract,
            state.get("workflow_input_values", {}),
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
        return await compile_and_persist_plan(
            self._settings,
            self._repository,
            self._registry,
            self._compiler,
            state,
            plan,
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
                        "code. Require acknowledgement for high risk. Do not "
                        "invent source code that is not included."
                    )
                ),
                HumanMessage(
                    content=json.dumps(review_input, ensure_ascii=False)
                ),
            ]
        )
        return validate_model(RiskReview, raw)

    async def verify_approval_and_lock(
        self,
        state: AgentGraphState,
    ) -> None:
        if state.get("review_action") != "APPROVE":
            raise ValueError("Plan approval is required")
        await self._repository.lock_session(task_id(state))
        await self._repository.update_status(
            task_id(state),
            TaskStatus.QUEUED_FOR_EXECUTION,
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
            execution_mode=state_execution_mode(state),
            runtime_profile=state["runtime_profile"],
            request_risk_review_id=(f"request-risk:{state['active_task_id']}"),
            request_risk_allowed=review.recommended_action != "BLOCK",
            revision_feedback=state.get("revision_feedback"),
            registry_snapshot_hash=self._registry.registry_snapshot_hash(),
        )


async def compile_and_persist_plan(
    settings: Settings,
    repository: AgentRepository,
    registry: ToolRegistry,
    compiler: SourceCompiler,
    state: AgentGraphState,
    plan: PlanDraft,
) -> PersistedPlan:
    next_revision = state.get("plan_revision_number", 0) + 1
    compiled = []
    for step in plan.steps:
        item = compiler.compile(step)
        path = compiler.materialize(
            item,
            settings.executor_shared_storage_root,
            state["active_task_id"],
            next_revision,
        )
        relative = executor_source_path(
            path,
            settings.executor_shared_storage_root,
        )
        compiled.append((item, relative))
    return await repository.persist_plan(
        task_id(state),
        plan,
        compiled,
        registry.registry_snapshot_hash(),
        state.get("revision_feedback"),
    )


__all__ = ["PlanningCapability", "compile_and_persist_plan"]
