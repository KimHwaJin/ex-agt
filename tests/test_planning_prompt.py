from hashlib import sha256
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import pytest

from ex_agent.domain.contracts import PlanDraft
from ex_agent.domain.enums import ExecutionMode, PlanningKind
from ex_agent.middleware.planning import (
    PlannerContext,
    PlanOutputMiddleware,
    _planning_system_message,
)
from ex_agent.tools.registry import ToolRegistry

_SKILL_ROOT = Path(__file__).resolve().parents[1] / "skills"


def runtime(mode: ExecutionMode = ExecutionMode.MULTI) -> Any:
    return SimpleNamespace(
        context=PlannerContext(
            task_id="test",
            user_request="분석 실행",
            planning_kind=PlanningKind.TOOL_PLAN,
            execution_mode=mode,
            runtime_profile="basic",
            request_risk_review_id="risk-test",
            request_risk_allowed=True,
        )
    )


def test_multi_prompt_requires_only_immediate_next_cell() -> None:
    context = PlannerContext(
        task_id="task-1",
        user_request="두 셀로 분석해줘",
        planning_kind=PlanningKind.TOOL_PLAN,
        execution_mode=ExecutionMode.MULTI,
        runtime_profile="basic",
        request_risk_review_id="risk-1",
        request_risk_allowed=True,
    )

    message = _planning_system_message(context, [], max_chars=10000)

    assert "MULTI, return exactly one Step" in str(message.content)
    assert "immediate next Jupyter cell" in str(message.content)


@pytest.mark.asyncio
async def test_output_middleware_hydrates_registry_lineage() -> None:
    registry = ToolRegistry(_SKILL_ROOT)
    registry.load()
    middleware = PlanOutputMiddleware(registry)
    plan = PlanDraft.model_validate(
        {
            "objective": "샘플 데이터를 생성한다",
            "strategy_summary": "첫 셀만 실행한다",
            "execution_mode": "MULTI",
            "steps": [
                {
                    "sequence": 0,
                    "title": "데이터 생성",
                    "purpose": "샘플 데이터 준비",
                    "planning_kind": "TOOL_PLAN",
                    "skill": {
                        "name": "data-access",
                        "version": "invented",
                        "content_sha256": "0" * 64,
                    },
                    "tool": {
                        "name": "fetch_dataset",
                        "version": "invented",
                        "source_sha256": "0" * 64,
                    },
                    "parameters": {
                        "query": "SELECT 1",
                        "dataset_name": "sample",
                    },
                    "selection_rationale": "데이터가 필요하다",
                }
            ],
        }
    )
    state = {"structured_response": plan}

    update = await middleware.aafter_agent(cast(Any, state), runtime())

    assert update is not None
    normalized = update["structured_response"]
    manifest = registry.get_tool("fetch_dataset")
    assert normalized.steps[0].skill == manifest.skill
    assert normalized.steps[0].tool == manifest.tool
    assert (
        manifest.tool.source_sha256
        == sha256(manifest.source.encode()).hexdigest()
    )


@pytest.mark.asyncio
async def test_output_middleware_rejects_wrong_skill_name() -> None:
    registry = ToolRegistry(_SKILL_ROOT)
    registry.load()
    middleware = PlanOutputMiddleware(registry)
    manifest = registry.get_tool("fetch_dataset")
    plan = PlanDraft.model_validate(
        {
            "objective": "샘플 데이터를 생성한다",
            "strategy_summary": "첫 셀만 실행한다",
            "execution_mode": "MULTI",
            "steps": [
                {
                    "sequence": 0,
                    "title": "데이터 생성",
                    "purpose": "샘플 데이터 준비",
                    "planning_kind": "TOOL_PLAN",
                    "skill": {
                        **manifest.skill.model_dump(mode="json"),
                        "name": "data-inspection",
                    },
                    "tool": manifest.tool.model_dump(mode="json"),
                    "parameters": {
                        "query": "SELECT 1",
                        "dataset_name": "sample",
                    },
                    "selection_rationale": "데이터가 필요하다",
                }
            ],
        }
    )

    with pytest.raises(ValueError, match="mismatched Skill"):
        await middleware.aafter_agent(
            cast(Any, {"structured_response": plan}),
            runtime(),
        )
