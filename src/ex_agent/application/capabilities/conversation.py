from __future__ import annotations

from langchain_core.language_models import BaseChatModel
from langchain_core.messages import HumanMessage, SystemMessage

from ex_agent.application.capabilities.common import (
    message_text,
    task_id,
    validate_model,
)
from ex_agent.application.state import AgentGraphState
from ex_agent.domain.contracts import IntentDecision, RiskReview
from ex_agent.domain.enums import TaskStatus
from ex_agent.llm.conversation import (
    ANSWER_SYSTEM_PROMPT,
    INTENT_SYSTEM_PROMPT,
)
from ex_agent.persistence.repository import AgentRepository


class ConversationCapability:
    """Intent, question answering, request risk, and simple lifecycle."""

    def __init__(
        self,
        repository: AgentRepository,
        model: BaseChatModel,
    ) -> None:
        self._repository = repository
        self._model = model

    async def update_status(
        self,
        state: AgentGraphState,
        status: TaskStatus,
    ) -> None:
        await self._repository.update_status(task_id(state), status)

    async def classify_intent(
        self,
        state: AgentGraphState,
    ) -> IntentDecision:
        classifier = self._model.with_structured_output(IntentDecision)
        raw = await classifier.ainvoke(
            [
                SystemMessage(content=INTENT_SYSTEM_PROMPT),
                HumanMessage(content=state["user_message"]),
            ]
        )
        return validate_model(IntentDecision, raw)

    async def answer_question(
        self,
        state: AgentGraphState,
        *,
        data_analysis: bool,
    ) -> str:
        domain = (
            "Answer as a practical data-analysis expert."
            if data_analysis
            else "Answer the general question accurately and concisely."
        )
        response = await self._model.ainvoke(
            [
                SystemMessage(content=ANSWER_SYSTEM_PROMPT + "\n" + domain),
                HumanMessage(content=state["user_message"]),
            ]
        )
        return message_text(response.content)

    async def commit_answer(
        self,
        state: AgentGraphState,
        answer: str,
    ) -> None:
        await self._repository.commit_message(
            task_id(state),
            answer,
            status=TaskStatus.SUCCEEDED,
        )

    async def review_request_risk(
        self,
        state: AgentGraphState,
    ) -> RiskReview:
        reviewer = self._model.with_structured_output(RiskReview)
        raw = await reviewer.ainvoke(
            [
                SystemMessage(
                    content=(
                        "Review the requested code or analysis before "
                        "planning. ALLOW low/medium risk, WARN or "
                        "REQUIRE_CONFIRMATION for high risk, and BLOCK only "
                        "critical destructive, credential exfiltration, "
                        "privilege escalation, or clearly malicious "
                        "requests. Provide evidence grounded in the request."
                    )
                ),
                HumanMessage(content=state["user_message"]),
            ]
        )
        return validate_model(RiskReview, raw)


__all__ = ["ConversationCapability"]
