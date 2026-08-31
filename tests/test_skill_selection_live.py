"""Opt-in real model selection regression, without Executor or database."""

import os
from pathlib import Path
from typing import cast

import pytest

from ex_agent.config import Settings
from ex_agent.llm.factory import build_chat_model
from ex_agent.middleware.skill_selection import select_skills
from ex_agent.tools.registry import ToolRegistry

_MODEL_URL = os.getenv("EX_AGENT_TEST_LIVE_MODEL_URL")
_SKILL_ROOT = Path(__file__).resolve().parents[1] / "skills"
pytestmark = [
    pytest.mark.llm,
    pytest.mark.skipif(
        not _MODEL_URL, reason="Live model evaluation disabled"
    ),
]


@pytest.mark.asyncio
@pytest.mark.parametrize("attempt", range(3))
async def test_live_sample_eda_selects_known_skills(attempt: int) -> None:
    registry = ToolRegistry(_SKILL_ROOT)
    registry.load()
    result = await select_skills(
        build_chat_model(Settings(agent_model_base_url=cast(str, _MODEL_URL))),
        registry.list_skills(),
        user_request=(
            "실제 데이터 분석 코드 만들어서 실행도 해보려고 하는데 가능해? "
            "예시로 데이터만들어서 해당 데이터 EDA하고, 플롯도 생성한 뒤 "
            "결과 리포트를 받고싶어"
        ),
    )
    print(attempt, result.model_dump())
    assert {"data-access", "visualization"} <= set(result.skill_names)
    assert set(result.skill_names) <= {
        item.name for item in registry.list_skills()
    }
