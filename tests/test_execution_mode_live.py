"""Opt-in real planning validation, without tasks or Executor execution."""

import os
from pathlib import Path
from typing import cast

import pytest

from ex_agent.config import Settings
from ex_agent.domain.enums import ExecutionMode, PlanningKind
from ex_agent.middleware.planning import PlannerContext
from ex_agent.planners.agent import PlannerAgent
from ex_agent.tools.compiler import SourceCompiler
from ex_agent.tools.registry import ToolRegistry

_MODEL_URL = os.getenv("EX_AGENT_TEST_LIVE_MODEL_URL")
_SKILL_ROOT = Path(__file__).resolve().parents[1] / "skills"
pytestmark = [
    pytest.mark.llm,
    pytest.mark.skipif(
        not _MODEL_URL, reason="Live model evaluation disabled"
    ),
]


@pytest.mark.parametrize("mode", list(ExecutionMode))
async def test_live_analysis_plan_uses_selected_mode(mode: ExecutionMode):
    registry = ToolRegistry(_SKILL_ROOT)
    registry.load()
    planner = PlannerAgent(
        Settings(agent_model_base_url=cast(str, _MODEL_URL)), registry
    )
    draft = await planner.plan(
        PlannerContext(
            task_id="live-mode-check-no-execution",
            user_request=(
                f"{mode.value} 모드로 샘플 데이터를 만들고 "
                "데이터 구조를 확인하고 숫자 컬럼 분포 플롯을 만들어줘. "
                "데이터 생성, 검사, 시각화를 각각 별도 셀에서 실행해줘."
            ),
            planning_kind=PlanningKind.TOOL_PLAN,
            execution_mode=mode,
            runtime_profile="basic",
            request_risk_review_id="test-only-no-submission",
            request_risk_allowed=True,
        )
    )
    assert draft.execution_mode is mode
    names = [step.tool.name for step in draft.steps if step.tool]
    if mode is ExecutionMode.SINGLE:
        expected = {"fetch_dataset", "inspect_dataset", "plot_distribution"}
        assert expected <= set(names)
    else:
        assert len(draft.steps) == 1
        assert names == ["fetch_dataset"]
    compiler = SourceCompiler(registry)
    for step in draft.steps:
        compiler.compile(step)
    print(mode.value, names)
