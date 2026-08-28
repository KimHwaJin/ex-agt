from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Protocol

from langchain.agents.middleware import AgentMiddleware
from langchain.agents.middleware.types import (
    AgentState,
    ModelRequest,
    ModelResponse,
)
from langchain_core.messages import HumanMessage, SystemMessage
from pydantic import BaseModel, ConfigDict, Field

from ex_agent.domain.contracts import PlanDraft
from ex_agent.domain.enums import ExecutionMode, PlanningKind
from ex_agent.tools.registry import SkillDocument, ToolRegistry


class SkillSelection(BaseModel):
    model_config = ConfigDict(extra="forbid")

    skill_names: list[str] = Field(default_factory=list)
    rationale: str = Field(min_length=1, max_length=2000)


@dataclass(slots=True)
class PlannerContext:
    task_id: str
    user_request: str
    planning_kind: PlanningKind
    execution_mode: ExecutionMode
    runtime_profile: str
    request_risk_review_id: str
    request_risk_allowed: bool
    previous_result_summaries: list[str] = field(default_factory=list)
    revision_feedback: str | None = None
    registry_snapshot_hash: str = ""
    selected_skill_names: list[str] = field(default_factory=list)


class ModelAuditSink(Protocol):
    async def record_model_call(
        self,
        *,
        task_id: str,
        component: str,
        duration_ms: int,
        succeeded: bool,
        metadata: dict[str, Any],
    ) -> None: ...


class NullModelAuditSink:
    async def record_model_call(
        self,
        *,
        task_id: str,
        component: str,
        duration_ms: int,
        succeeded: bool,
        metadata: dict[str, Any],
    ) -> None:
        del task_id, component, duration_ms, succeeded, metadata


PlannerMiddleware = AgentMiddleware[
    AgentState[PlanDraft],
    PlannerContext,
    PlanDraft,
]


class RiskPrerequisiteMiddleware(PlannerMiddleware):
    async def abefore_agent(
        self,
        state: AgentState[PlanDraft],
        runtime: Any,
    ) -> dict[str, Any] | None:
        del state
        context = _planner_context(runtime)
        if not context.request_risk_review_id:
            raise ValueError(
                "Planner requires a persisted request risk review"
            )
        if not context.request_risk_allowed:
            raise PermissionError("Planner request risk gate is not satisfied")
        return None


class SkillContextMiddleware(PlannerMiddleware):
    def __init__(
        self,
        registry: ToolRegistry,
        *,
        context_max_chars: int,
    ) -> None:
        self._registry = registry
        self._context_max_chars = context_max_chars

    async def awrap_model_call(
        self,
        request: ModelRequest[PlannerContext],
        handler: Callable[
            [ModelRequest[PlannerContext]],
            Awaitable[ModelResponse[PlanDraft]],
        ],
    ) -> ModelResponse[PlanDraft]:
        context = _planner_context(request.runtime)
        skills: list[SkillDocument] = []
        if context.planning_kind is PlanningKind.TOOL_PLAN:
            skills = await self._select_skills(request, context)
            context.selected_skill_names[:] = [item.name for item in skills]
        system_message = _planning_system_message(
            context,
            skills,
            max_chars=self._context_max_chars,
        )
        return await handler(request.override(system_message=system_message))

    async def _select_skills(
        self,
        request: ModelRequest[PlannerContext],
        context: PlannerContext,
    ) -> list[SkillDocument]:
        available = self._registry.list_skills()
        descriptions = "\n".join(
            f"- {item.name}@{item.version}: {item.description}"
            for item in available
        )
        selector = request.model.with_structured_output(SkillSelection)
        selection = await selector.ainvoke(
            [
                SystemMessage(
                    content=(
                        "Select only the analysis Skills relevant to the "
                        "request. Return public, concise rationale."
                    )
                ),
                HumanMessage(
                    content=(
                        f"User request:\n{context.user_request}\n\n"
                        f"Available Skills:\n{descriptions}"
                    )
                ),
            ]
        )
        if not isinstance(selection, SkillSelection):
            selection = SkillSelection.model_validate(selection)
        unknown = sorted(
            set(selection.skill_names) - {item.name for item in available}
        )
        if unknown:
            raise ValueError(
                f"Skill selector returned unknown Skills: {unknown}"
            )
        return [
            self._registry.get_skill(name) for name in selection.skill_names
        ]


class ModelAuditMiddleware(PlannerMiddleware):
    def __init__(self, sink: ModelAuditSink) -> None:
        self._sink = sink

    async def awrap_model_call(
        self,
        request: ModelRequest[PlannerContext],
        handler: Callable[
            [ModelRequest[PlannerContext]],
            Awaitable[ModelResponse[PlanDraft]],
        ],
    ) -> ModelResponse[PlanDraft]:
        context = _planner_context(request.runtime)
        started = time.monotonic()
        succeeded = False
        try:
            response = await handler(request)
            succeeded = True
            return response
        finally:
            duration = int((time.monotonic() - started) * 1000)
            await self._sink.record_model_call(
                task_id=context.task_id,
                component="planner",
                duration_ms=duration,
                succeeded=succeeded,
                metadata={
                    "registry_snapshot_hash": (context.registry_snapshot_hash),
                    "selected_skill_names": (context.selected_skill_names),
                },
            )


class PlannerBudgetMiddleware(PlannerMiddleware):
    def __init__(self, timeout_seconds: float) -> None:
        self._timeout_seconds = timeout_seconds

    async def awrap_model_call(
        self,
        request: ModelRequest[PlannerContext],
        handler: Callable[
            [ModelRequest[PlannerContext]],
            Awaitable[ModelResponse[PlanDraft]],
        ],
    ) -> ModelResponse[PlanDraft]:
        async with asyncio.timeout(self._timeout_seconds):
            return await handler(request)


class PlanOutputMiddleware(PlannerMiddleware):
    def __init__(self, registry: ToolRegistry) -> None:
        self._registry = registry

    async def aafter_agent(
        self,
        state: AgentState[PlanDraft],
        runtime: Any,
    ) -> dict[str, Any] | None:
        del runtime
        raw = state.get("structured_response")
        plan = (
            raw
            if isinstance(raw, PlanDraft)
            else PlanDraft.model_validate(raw)
        )
        if plan.execution_mode is ExecutionMode.MULTI and len(plan.steps) != 1:
            raise ValueError(
                "MULTI planning must produce exactly one next Step"
            )
        for step in plan.steps:
            if step.planning_kind is not PlanningKind.TOOL_PLAN:
                continue
            if step.skill is None or step.tool is None:
                raise ValueError("Tool Step is missing Skill/Tool lineage")
            manifest = self._registry.get_tool(step.tool.name)
            if manifest.skill != step.skill or manifest.tool != step.tool:
                raise ValueError(
                    "Planner returned stale or mismatched Tool lineage"
                )
        return {"structured_response": plan}


def _planner_context(runtime: Any) -> PlannerContext:
    context = runtime.context
    if not isinstance(context, PlannerContext):
        raise TypeError("PlannerContext is required")
    return context


def _planning_system_message(
    context: PlannerContext,
    skills: list[SkillDocument],
    *,
    max_chars: int,
) -> SystemMessage:
    skill_context = []
    for skill in skills:
        tool_payloads = [
            {
                "skill": tool.skill.model_dump(mode="json"),
                "tool": tool.tool.model_dump(mode="json"),
                "description": tool.description,
                "parameters": {
                    name: spec.model_dump(mode="json")
                    for name, spec in tool.parameters.items()
                },
            }
            for tool in skill.tools
        ]
        skill_context.append(
            f"## {skill.name}@{skill.version}\n"
            f"{skill.content}\n"
            f"Tool manifests:\n"
            f"{json.dumps(tool_payloads, ensure_ascii=False)}"
        )
    previous = "\n".join(context.previous_result_summaries)
    content = (
        "You create an auditable execution plan. Return only the configured "
        "structured PlanDraft. Never claim that a Tool was executed. "
        "Every rationale must be suitable for display to the user.\n\n"
        f"Planning kind: {context.planning_kind}\n"
        f"Execution mode: {context.execution_mode}\n"
        f"Runtime profile: {context.runtime_profile}\n"
        f"User request:\n{context.user_request}\n\n"
        f"Revision feedback:\n{context.revision_feedback or ''}\n\n"
        f"Previous result summaries:\n{previous}\n\n"
        f"Available Skill context:\n{''.join(skill_context)}"
    )
    if len(content) > max_chars:
        raise ValueError(
            "Planner context exceeds the configured character budget"
        )
    return SystemMessage(content=content)
