from __future__ import annotations

import json
import logging
from typing import Any
from uuid import UUID

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from ex_agent.application.capabilities.common import (
    bounded_report_markdown,
    task_id,
)
from ex_agent.application.state import AgentGraphState
from ex_agent.config import Settings
from ex_agent.domain.contracts import PlanStepDraft, ReportResult
from ex_agent.domain.enums import PlanningKind, TaskStatus
from ex_agent.executor.client import ExecutorClient
from ex_agent.executor.files import materialize_input_file
from ex_agent.executor.results import validated_result_summaries
from ex_agent.persistence.repository import AgentRepository

logger = logging.getLogger(__name__)


class ReportingCapability:
    """Validated report evidence, notebook Markdown, and terminal commit."""

    def __init__(
        self,
        settings: Settings,
        repository: AgentRepository,
        executor: ExecutorClient,
        model: BaseChatModel,
    ) -> None:
        self._settings = settings
        self._repository = repository
        self._executor = executor
        self._model = model

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
        markdown = await self.generate_report_markdown(evidence)
        report_input = materialize_input_file(
            self._settings.executor_shared_storage_root,
            f"{state['active_task_id']}/reports/analysis-report.md",
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

    async def generate_report_markdown(
        self,
        evidence: dict[str, Any],
    ) -> str:
        raw = await self._model.ainvoke(
            [
                SystemMessage(
                    content=(
                        "Write a Korean Markdown report grounded only in the "
                        "evidence. Include objective, approved plan and why "
                        "each Skill/Tool was selected, execution results, "
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
        return bounded_report_markdown(raw.content)

    async def commit_terminal(
        self,
        state: AgentGraphState,
        *,
        status: TaskStatus,
        message: str,
    ) -> None:
        await self._repository.commit_message(
            task_id(state),
            message,
            status=status,
            metadata={
                "execution_id": state.get("execution_id"),
                "report_artifact_id": state.get("report_artifact_id"),
            },
        )
        if status is TaskStatus.SUCCEEDED:
            await self._offer_workflow_promotion(task_id(state))

    async def _offer_workflow_promotion(self, source_task_id: UUID) -> None:
        try:
            source = await self._repository.workflow_promotion_source(
                source_task_id
            )
            if not source.steps:
                return
            steps = [
                PlanStepDraft.model_validate(row.step_payload)
                for row in source.steps
            ]
            if any(
                step.planning_kind is not PlanningKind.TOOL_PLAN
                for step in steps
            ):
                return
            await self._repository.append_task_event(
                source_task_id,
                "workflow.promotion_available",
                {
                    "task_id": str(source_task_id),
                    "draft_path": (
                        f"/api/v1/tasks/{source_task_id}/"
                        "workflow-promotion-draft"
                    ),
                },
            )
        except Exception:
            logger.warning(
                "Workflow promotion suggestion could not be created",
                exc_info=True,
            )


__all__ = ["ReportingCapability"]
