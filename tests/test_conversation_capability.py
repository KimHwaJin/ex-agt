from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.messages import AIMessage

from ex_agent.application.capabilities.conversation import (
    ConversationCapability,
)
from ex_agent.domain.contracts import IntentDecision
from ex_agent.domain.enums import Intent
from ex_agent.graph.routes import route_intent
from ex_agent.llm.conversation import (
    ANSWER_SYSTEM_PROMPT,
    INTENT_SYSTEM_PROMPT,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("intent", list(Intent))
async def test_classifier_uses_model_decision_without_keyword_override(
    intent: Intent,
) -> None:
    decision = IntentDecision(
        intent=intent, confidence=0.98, decision_summary="모델 판단"
    )
    model = MagicMock()
    classifier = model.with_structured_output.return_value
    classifier.ainvoke = AsyncMock(return_value=decision)
    capability = ConversationCapability(cast(Any, None), model)
    result = await capability.classify_intent({"user_message": "ㅎㅇㅎㅇ"})
    assert result == decision
    model.with_structured_output.assert_called_once_with(IntentDecision)
    messages = classifier.ainvoke.call_args.args[0]
    assert messages[0].content == INTENT_SYSTEM_PROMPT
    assert messages[1].content == "ㅎㅇㅎㅇ"


@pytest.mark.asyncio
@pytest.mark.parametrize("data_analysis", [False, True])
async def test_answer_uses_conversation_policy_without_tool_calls(
    data_analysis: bool,
) -> None:
    repository = MagicMock()
    model = MagicMock()
    model.ainvoke = AsyncMock(return_value=AIMessage(content="직접 답변"))
    capability = ConversationCapability(repository, model)
    answer = await capability.answer_question(
        {"user_message": "질문"}, data_analysis=data_analysis
    )
    assert answer == "직접 답변"
    messages = model.ainvoke.call_args.args[0]
    assert messages[0].content.startswith(ANSWER_SYSTEM_PROMPT)
    assert messages[1].content == "질문"
    model.bind_tools.assert_not_called()
    assert repository.mock_calls == []


@pytest.mark.parametrize(
    ("intent", "route"),
    [
        (Intent.GENERAL_QA, "answer_general"),
        (Intent.DATA_ANALYSIS_QA, "answer_data_question"),
        (Intent.DATA_ANALYSIS_EXECUTION, "review_request_risk"),
        (Intent.CODE_EXECUTION, "choose_execution_mode"),
    ],
)
def test_intent_routes_preserve_execution_approval_boundary(
    intent: Intent, route: str
) -> None:
    decision = IntentDecision(
        intent=intent, confidence=0.99, decision_summary="명확한 요청"
    )
    assert route_intent({"intent_decision": decision}) == route
    unclear = decision.model_copy(
        update={
            "requires_clarification": True,
            "clarification_question": "어떤 작업을 원하시나요?",
        }
    )
    assert route_intent({"intent_decision": unclear}) == "clarify_request"
