from typing import Any, cast

import pytest

from ex_agent.domain.contracts import IntentDecision
from ex_agent.domain.enums import Intent, TaskStatus
from ex_agent.graph.builder import build_workflow_graph


class GeneralQaServices:
    def __init__(self) -> None:
        self.committed: str | None = None

    async def update_status(self, state: Any, status: TaskStatus) -> None:
        del state, status

    async def classify_intent(self, state: Any) -> IntentDecision:
        del state
        return IntentDecision(
            intent=Intent.GENERAL_QA,
            confidence=0.99,
            decision_summary="일반 지식 질문",
        )

    async def answer_question(
        self,
        state: Any,
        *,
        data_analysis: bool,
    ) -> str:
        del state
        assert not data_analysis
        return "응답"

    async def commit_answer(self, state: Any, answer: str) -> None:
        del state
        self.committed = answer


@pytest.mark.asyncio
async def test_general_question_finishes_without_interrupt() -> None:
    services = GeneralQaServices()
    graph = build_workflow_graph(cast(Any, services))
    result = await graph.ainvoke(
        {
            "user_id": "user-1",
            "project_id": "project-1",
            "session_id": "session-1",
            "active_task_id": "6a35ab4f-ca25-4ce3-9cb5-7d51ff65646b",
            "current_input_message_id": (
                "b6fcb828-3e4a-40ea-8941-1c62267bf7b3"
            ),
            "user_message": "서울의 수도는 어디야?",
        }
    )

    assert result["phase"] is TaskStatus.SUCCEEDED
    assert services.committed == "응답"
    assert "__interrupt__" not in result
