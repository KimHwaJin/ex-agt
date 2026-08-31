import asyncio
import json
from pathlib import Path
from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest
from langchain_core.exceptions import OutputParserException
from langchain_core.language_models.fake_chat_models import FakeListChatModel
from langchain_core.runnables import RunnableLambda

from ex_agent.config import Settings
from ex_agent.domain.enums import ExecutionMode, PlanningKind
from ex_agent.middleware.planning import PlannerContext, SkillContextMiddleware
from ex_agent.middleware.skill_selection import (
    InvalidSkillSelection,
    select_skills,
)
from ex_agent.planners.agent import PlannerAgent
from ex_agent.tools.registry import ToolRegistry

_SKILL_ROOT = Path(__file__).resolve().parents[1] / "skills"


@pytest.fixture
def registry() -> ToolRegistry:
    result = ToolRegistry(_SKILL_ROOT)
    result.load()
    return result


def context(kind: PlanningKind = PlanningKind.TOOL_PLAN) -> PlannerContext:
    return PlannerContext(
        task_id="selector-test",
        user_request="샘플 데이터를 만들어 EDA하고 플롯도 생성해줘",
        planning_kind=kind,
        execution_mode=ExecutionMode.MULTI,
        runtime_profile="basic",
        request_risk_review_id="risk-1",
        request_risk_allowed=True,
    )


def response(names: list[str]) -> dict[str, Any]:
    return {"skill_names": names, "rationale": "공개 선택 이유"}


def model_with(*responses: Any) -> tuple[Any, AsyncMock]:
    model = MagicMock()
    invoke = AsyncMock(side_effect=responses)
    model.with_structured_output.return_value.ainvoke = invoke
    return model, invoke


@pytest.mark.asyncio
@pytest.mark.parametrize("versioned", [False, True])
async def test_observed_selection_resolves_registry_identities(
    registry: ToolRegistry, versioned: bool
) -> None:
    names = [
        "data-inspection",
        "data-quality",
        "descriptive-analysis",
        "visualization",
    ]
    selected = [f"{name}@0.1.0" for name in names] if versioned else names
    model, invoke = model_with(response(selected))
    result = await select_skills(
        model, registry.list_skills(), user_request="EDA"
    )
    assert result.skill_names == names
    assert result.rationale == "공개 선택 이유"
    assert invoke.await_count == 1


@pytest.mark.asyncio
async def test_catalog_separates_version_and_schema_enumerates_names(
    registry: ToolRegistry,
) -> None:
    model, invoke = model_with(response(["data-access"]))
    await select_skills(
        model,
        registry.list_skills(),
        user_request="EDA",
        revision_feedback="플롯 추가",
        previous_result_summaries=["sample.csv 생성"],
    )
    schema = model.with_structured_output.call_args.args[0]
    assert schema["properties"]["skill_names"]["items"]["enum"] == [
        item.name for item in registry.list_skills()
    ]
    payload = json.loads(invoke.call_args.args[0][1].content)
    assert payload["available_skills"][0]["name"] == "data-access"
    assert payload["available_skills"][0]["version"] == "0.1.0"
    assert payload["revision_feedback"] == "플롯 추가"
    assert payload["previous_results"] == ["sample.csv 생성"]


@pytest.mark.asyncio
async def test_duplicate_aliases_are_deduplicated_in_selected_order(
    registry: ToolRegistry,
) -> None:
    model, _ = model_with(
        response(["visualization@0.1.0", "data-access", "visualization"])
    )
    result = await select_skills(
        model, registry.list_skills(), user_request="EDA"
    )
    assert result.skill_names == ["visualization", "data-access"]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "invalid",
    [
        "visualization@0.2.0",
        "visualization@0.1.0@other",
        "unknown",
        "UNKNOWN@0.1.0",
        "Visualization",
        " visualization ",
    ],
)
async def test_invalid_names_get_one_correction_then_fail_closed(
    registry: ToolRegistry, invalid: str
) -> None:
    model, invoke = model_with(response([invalid]), response([invalid]))
    with pytest.raises(InvalidSkillSelection, match="after 2 attempts"):
        await select_skills(model, registry.list_skills(), user_request="EDA")
    assert invoke.await_count == 2
    assert invalid in invoke.call_args.args[0][-1].content


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "bad_response",
    [
        response([]),
        response(["unknown"]),
        {"skill_names": ["visualization"]},
        {"skill_names": [123], "rationale": "wrong type"},
        OutputParserException("malformed JSON"),
    ],
)
async def test_invalid_output_can_be_corrected(
    registry: ToolRegistry, bad_response: Any
) -> None:
    model, invoke = model_with(bad_response, response(["visualization"]))
    result = await select_skills(
        model, registry.list_skills(), user_request="EDA"
    )
    assert result.skill_names == ["visualization"]
    assert invoke.await_count == 2
    assert "previous selection was invalid" in (
        invoke.call_args.args[0][-1].content
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error", [ConnectionError(), asyncio.CancelledError()]
)
async def test_transport_errors_and_cancellation_are_not_selection_retries(
    registry: ToolRegistry, error: BaseException
) -> None:
    model, invoke = model_with(error)
    with pytest.raises(type(error)):
        await select_skills(model, registry.list_skills(), user_request="EDA")
    assert invoke.await_count == 1


@pytest.mark.asyncio
async def test_empty_registry_fails_without_model_call() -> None:
    model, invoke = model_with()
    with pytest.raises(InvalidSkillSelection, match="No analysis Skills"):
        await select_skills(model, [], user_request="EDA")
    invoke.assert_not_awaited()


@pytest.mark.asyncio
async def test_custom_code_bypasses_selection_and_clears_stale_context(
    registry: ToolRegistry,
) -> None:
    planner_context = context(PlanningKind.CUSTOM_CODE)
    planner_context.selected_skill_names = ["data-access"]
    planner_context.skill_selection_rationale = "old"
    request = MagicMock()
    request.runtime.context = planner_context
    handler = AsyncMock(return_value="plan")
    middleware = SkillContextMiddleware(registry, context_max_chars=10000)
    await middleware.awrap_model_call(request, handler)
    request.model.with_structured_output.assert_not_called()
    assert planner_context.selected_skill_names == []
    assert planner_context.skill_selection_rationale == ""
    handler.assert_awaited_once()


@pytest.mark.asyncio
@pytest.mark.parametrize("stall", [False, True])
async def test_selector_is_inside_planner_audit_and_timeout(
    registry: ToolRegistry, monkeypatch: Any, stall: bool
) -> None:
    async def selection(_: Any) -> dict[str, Any]:
        if stall:
            await asyncio.Event().wait()
        return response(["unknown"])

    monkeypatch.setattr(
        FakeListChatModel,
        "with_structured_output",
        lambda *args, **kwargs: RunnableLambda(selection),
    )
    sink = MagicMock()
    sink.record_model_call = AsyncMock()
    planner = PlannerAgent(
        Settings(planner_timeout_seconds=0.02 if stall else 10),
        registry,
        model=FakeListChatModel(responses=["unused"]),
        audit_sink=sink,
    )
    with pytest.raises(TimeoutError if stall else InvalidSkillSelection):
        await planner.plan(context())
    sink.record_model_call.assert_awaited_once()
    assert sink.record_model_call.call_args.kwargs["succeeded"] is False
    assert (
        sink.record_model_call.call_args.kwargs["metadata"][
            "selected_skill_names"
        ]
        == []
    )
