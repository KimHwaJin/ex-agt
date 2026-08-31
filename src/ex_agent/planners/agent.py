from __future__ import annotations

from typing import Any

from langchain.agents import create_agent
from langchain_core.language_models import BaseChatModel

from ex_agent.config import Settings
from ex_agent.domain.contracts import PlanDraft
from ex_agent.llm.factory import build_chat_model
from ex_agent.middleware.planning import (
    ModelAuditMiddleware,
    ModelAuditSink,
    NullModelAuditSink,
    PlannerBudgetMiddleware,
    PlannerContext,
    PlanOutputMiddleware,
    RiskPrerequisiteMiddleware,
    SkillContextMiddleware,
)
from ex_agent.tools.registry import ToolRegistry


class PlannerAgent:
    def __init__(
        self,
        settings: Settings,
        registry: ToolRegistry,
        *,
        model: BaseChatModel | None = None,
        audit_sink: ModelAuditSink | None = None,
    ) -> None:
        resolved_model = model or build_chat_model(settings)
        self._agent = create_agent(
            model=resolved_model,
            tools=[],
            middleware=[
                RiskPrerequisiteMiddleware(),
                # Audit and bound Skill selection as well as plan generation.
                ModelAuditMiddleware(audit_sink or NullModelAuditSink()),
                PlannerBudgetMiddleware(settings.planner_timeout_seconds),
                SkillContextMiddleware(
                    registry,
                    context_max_chars=(settings.planner_context_max_chars),
                ),
                PlanOutputMiddleware(registry),
            ],
            response_format=PlanDraft,
            context_schema=PlannerContext,
            name="execution_planner",
        )

    async def plan(self, context: PlannerContext) -> PlanDraft:
        result: dict[str, Any] = await self._agent.ainvoke(
            {
                "messages": [
                    {
                        "role": "user",
                        "content": context.user_request,
                    }
                ]
            },
            context=context,
            config={"recursion_limit": 8},
        )
        raw = result.get("structured_response")
        if isinstance(raw, PlanDraft):
            return raw
        return PlanDraft.model_validate(raw)
