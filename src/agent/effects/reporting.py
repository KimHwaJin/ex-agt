from collections.abc import Awaitable, Callable
from typing import Any
from uuid import UUID

from agent.effects.files import input_file
from agent.effects.journal import EffectJournal
from agent.effects.runner import ExecutorEffectSender
from ex_agent.application.capabilities.common import task_id
from ex_agent.application.state import AgentGraphState
from ex_agent.domain.contracts import ReportResult
from ex_agent.executor import requests
from ex_agent.executor.contracts import ExecutionResult


class DurableReporting:
    def __init__(
        self,
        journal: EffectJournal,
        sender: ExecutorEffectSender,
        generate: Callable[[dict[str, Any]], Awaitable[str]],
    ) -> None:
        self.journal = journal
        self.sender = sender
        self.generate = generate

    async def report(
        self,
        state: AgentGraphState,
        evidence: dict[str, Any],
    ) -> ReportResult:
        result = ExecutionResult.model_validate(evidence["executor_result"])
        if (
            result.execution.state.status != "SUCCEEDED"
            or str(result.execution.execution_id) != state["execution_id"]
        ):
            raise ValueError("Report requires this Execution's success result")
        key = f"task:{state['active_task_id']}:report"

        async def prepare() -> dict[str, Any]:
            markdown = await self.generate(evidence)
            source = input_file(state["active_task_id"], markdown, "md")
            return {
                "kind": "report",
                "path": f"/executions/{state['execution_id']}/artifacts",
                "body": requests.report_payload(
                    idempotency_key=key,
                    path=source["path"],
                    sha256=source["sha256"],
                ),
                "files": [source],
                "markdown": markdown,
            }

        record = await self.journal.run(
            task_id=task_id(state),
            key=key,
            kind="report",
            inputs={
                "execution_id": state["execution_id"],
                "evidence": evidence,
            },
            prepare=prepare,
            send=self.sender.send,
        )
        assert record.response is not None
        return ReportResult(
            markdown=record.request["markdown"],
            artifact_id=UUID(record.response["artifact_id"]),
        )
