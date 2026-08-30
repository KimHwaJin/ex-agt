from __future__ import annotations

from typing import Any

from langgraph.types import interrupt

from ex_agent.domain.contracts import ClarificationAnswer
from ex_agent.domain.enums import TaskStatus
from ex_agent.graph.node_groups.common import (
    WorkflowNodeGroup,
    validate_resume_signal,
)
from ex_agent.graph.state import AgentGraphState


class ConversationNodes(WorkflowNodeGroup):
    async def hydrate_turn(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        await self._services.update_status(state, TaskStatus.CLASSIFYING)
        return {
            "schema_version": "1.0",
            "phase": TaskStatus.CLASSIFYING,
            "clarification_count": state.get("clarification_count", 0),
            "correction_count": 0,
            "risk_acknowledged": False,
            "runtime_profile": state.get("runtime_profile", "basic"),
        }

    async def classify_intent(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        decision = await self._services.classify_intent(state)
        return {"intent_decision": decision}

    def clarify_request(self, state: AgentGraphState) -> dict[str, Any]:
        decision = state["intent_decision"]
        raw = interrupt(
            {
                "kind": "CLARIFICATION",
                "task_id": state["active_task_id"],
                "question": decision.clarification_question,
            }
        )
        signal = validate_resume_signal(raw)
        if not isinstance(signal, ClarificationAnswer):
            raise TypeError("ClarificationAnswer is required")
        return {
            "user_message": (
                f"{state['user_message']}\n\nClarification: {signal.answer}"
            ),
            "clarification_count": state.get("clarification_count", 0) + 1,
        }

    async def answer_general(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        await self._services.update_status(state, TaskStatus.ANSWERING)
        answer = await self._services.answer_question(
            state,
            data_analysis=False,
        )
        return {"phase": TaskStatus.ANSWERING, "answer": answer}

    async def answer_data_question(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        await self._services.update_status(state, TaskStatus.ANSWERING)
        answer = await self._services.answer_question(
            state,
            data_analysis=True,
        )
        return {"phase": TaskStatus.ANSWERING, "answer": answer}

    async def commit_answer(
        self,
        state: AgentGraphState,
    ) -> dict[str, Any]:
        await self._services.commit_answer(state, state["answer"])
        return {"phase": TaskStatus.SUCCEEDED}


__all__ = ["ConversationNodes"]
