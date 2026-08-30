from __future__ import annotations

import json
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
from ex_agent.domain.contracts import ReportResult
from ex_agent.domain.enums import TaskStatus
from ex_agent.executor.client import ExecutorClient
from ex_agent.executor.files import materialize_input_file
from ex_agent.executor.results import validated_result_summaries
from ex_agent.persistence.repository import AgentRepository


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
        markdown = bounded_report_markdown(raw.content)
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


__all__ = ["ReportingCapability"]
