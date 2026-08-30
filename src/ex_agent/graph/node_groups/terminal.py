from __future__ import annotations

from typing import Any

from ex_agent.domain.contracts import ExecutorReconciliation
from ex_agent.domain.enums import TaskStatus
from ex_agent.graph.node_groups.common import WorkflowNodeGroup
from ex_agent.graph.state import AgentGraphState


class TerminalNodes(WorkflowNodeGroup):
    async def build_report_evidence(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        evidence = await self._services.build_report_evidence(state)
        return {
            "phase": TaskStatus.GENERATING_REPORT,
            "report_evidence": evidence,
        }

    async def generate_report(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        report = await self._services.generate_and_materialize_report(
            state,
            state["report_evidence"],
        )
        return {
            "report_markdown": report.markdown,
            "report_artifact_id": str(report.artifact_id),
        }

    async def commit_success(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        await self._services.commit_terminal(
            state,
            status=TaskStatus.SUCCEEDED,
            message=state["report_markdown"],
        )
        return {"phase": TaskStatus.SUCCEEDED}

    async def commit_rejected(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        message = "실행 계획이 거절되어 작업을 종료했습니다."
        await self._services.commit_terminal(
            state,
            status=TaskStatus.REJECTED,
            message=message,
        )
        return {"phase": TaskStatus.REJECTED, "terminal_message": message}

    async def commit_blocked(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        review = state.get("code_risk") or state["request_risk"]
        message = f"위험 판정으로 작업을 차단했습니다: {review.summary}"
        await self._services.commit_terminal(
            state,
            status=TaskStatus.FAILED,
            message=message,
        )
        return {"phase": TaskStatus.FAILED, "terminal_message": message}

    async def commit_failed(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        result = ExecutorReconciliation.model_validate(
            state["executor_reconciliation"]
        )
        message = result.error_message or "코드 실행에 실패했습니다."
        await self._services.commit_terminal(
            state,
            status=TaskStatus.FAILED,
            message=message,
        )
        return {"phase": TaskStatus.FAILED, "terminal_message": message}

    async def commit_cancelled(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        message = "Executor 취소 완료를 확인했습니다."
        await self._services.commit_terminal(
            state,
            status=TaskStatus.CANCELLED,
            message=message,
        )
        return {
            "phase": TaskStatus.CANCELLED,
            "terminal_message": message,
        }


__all__ = ["TerminalNodes"]
