"""Session-graph business services; production entrypoints remain opt-in."""

import logging
from typing import Any
from uuid import UUID

from langchain_core.embeddings import Embeddings
from langchain_core.language_models import BaseChatModel
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from agent.effects.execution import DurableExecution
from agent.effects.journal import EffectJournal
from agent.effects.plans import DurablePlans
from agent.effects.projections import EffectProjections
from agent.effects.reporting import DurableReporting
from agent.effects.runner import ExecutorEffectSender
from agent.effects.store import EffectStore
from ex_agent.application.capabilities.common import task_id
from ex_agent.application.services import DefaultWorkflowServices
from ex_agent.application.state import AgentGraphState
from ex_agent.config import Settings
from ex_agent.domain.contracts import (
    MultiDecision,
    PersistedPlan,
    PlanDraft,
    PlanStepDraft,
    ReportResult,
    SubmissionReceipt,
)
from ex_agent.domain.enums import PlanningKind, TaskStatus
from ex_agent.executor.client import ExecutorClient
from ex_agent.persistence.repository import AgentRepository
from ex_agent.tools.registry import ToolRegistry

logger = logging.getLogger(__name__)


class SessionWorkflowServices(DefaultWorkflowServices):
    """Same workflow contract, with stable external request recovery.

    All calls must use the shared session guard. Agent schema migration
    0007 is required; worker-only migrations do not install this journal.
    """

    def __init__(
        self,
        settings: Settings,
        repository: AgentRepository,
        executor: ExecutorClient,
        registry: ToolRegistry,
        *,
        sessions: async_sessionmaker[AsyncSession],
        model: BaseChatModel | None = None,
        embeddings: Embeddings | None = None,
    ) -> None:
        super().__init__(
            settings,
            repository,
            executor,
            registry,
            model=model,
            embeddings=embeddings,
        )
        journal = EffectJournal(EffectStore(sessions))
        sender = ExecutorEffectSender(settings, executor)
        self.projections = EffectProjections(sessions)
        self.plans = DurablePlans(
            settings, repository, registry, self._compiler
        )
        self.execution = DurableExecution(
            settings,
            repository,
            journal,
            sender,
            self.plans,
            self.projections,
        )
        self.reporting = DurableReporting(
            journal, sender, self.generate_report_markdown
        )

    async def compile_and_persist_plan(
        self,
        state: AgentGraphState,
        plan: PlanDraft,
    ) -> PersistedPlan:
        return await self.plans.persist(state, plan)

    async def submit_execution(
        self,
        state: AgentGraphState,
    ) -> SubmissionReceipt:
        return await self.execution.submit(state)

    async def append_operation(
        self,
        state: AgentGraphState,
        decision: MultiDecision,
    ) -> SubmissionReceipt:
        return await self.execution.append(state, decision)

    async def finalize_execution(self, state: AgentGraphState) -> None:
        await self.execution.lifecycle(state, kind="finalize")

    async def cancel_execution(
        self,
        state: AgentGraphState,
        reason: str | None,
    ) -> None:
        await self.execution.lifecycle(state, kind="cancel", reason=reason)

    async def generate_and_materialize_report(
        self,
        state: AgentGraphState,
        evidence: dict[str, Any],
    ) -> ReportResult:
        return await self.reporting.report(state, evidence)

    async def commit_terminal(
        self,
        state: AgentGraphState,
        *,
        status: TaskStatus,
        message: str,
    ) -> None:
        await self.projections.terminal(
            task_id(state),
            status=status,
            message=message,
            metadata={
                "execution_id": state.get("execution_id"),
                "report_artifact_id": state.get("report_artifact_id"),
            },
        )
        if status is TaskStatus.SUCCEEDED:
            await self._offer_workflow_promotion(task_id(state))

    async def commit_answer(
        self,
        state: AgentGraphState,
        answer: str,
    ) -> None:
        await self.projections.terminal(
            task_id(state),
            status=TaskStatus.SUCCEEDED,
            message=answer,
            metadata={},
        )

    async def _offer_workflow_promotion(self, source_task_id: UUID) -> None:
        try:
            source = await self._repository.workflow_promotion_source(
                source_task_id
            )
            if source.steps and all(
                PlanStepDraft.model_validate(row.step_payload).planning_kind
                is PlanningKind.TOOL_PLAN
                for row in source.steps
            ):
                await self.projections.promotion(source_task_id)
        except Exception:
            logger.warning(
                "Workflow promotion suggestion could not be created",
                exc_info=True,
            )
