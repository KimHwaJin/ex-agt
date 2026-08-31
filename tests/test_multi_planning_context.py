import json
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

import pytest

from ex_agent.application.capabilities.execution import ExecutionCapability
from ex_agent.config import Settings
from ex_agent.domain.contracts import (
    ExecutorReconciliation,
    PlanDraft,
    PlanStepDraft,
)
from ex_agent.domain.enums import ExecutionMode, ExecutorOutcome, PlanningKind
from ex_agent.persistence.repository import AgentRepository
from ex_agent.tools.registry import ToolRegistry

_SKILL_ROOT = Path(__file__).resolve().parents[1] / "skills"


@pytest.fixture
def registry() -> ToolRegistry:
    registry = ToolRegistry(_SKILL_ROOT)
    registry.load()
    return registry


def step(registry: ToolRegistry, name: str) -> PlanStepDraft:
    manifest = registry.get_tool(name)
    return PlanStepDraft(
        sequence=0,
        title=name,
        purpose="샘플 EDA",
        planning_kind=PlanningKind.TOOL_PLAN,
        skill=manifest.skill,
        tool=manifest.tool,
        parameters={"path": "artifacts/datasets/sample.csv"},
        selection_rationale="등록된 도구",
    )


def state(registry: ToolRegistry, kind: PlanningKind) -> Any:
    return {
        "user_message": "샘플 데이터 EDA와 플롯",
        "planning_kind": kind,
        "plan": PlanDraft(
            objective="샘플 EDA",
            strategy_summary="생성, 검사, 통계, 플롯",
            execution_mode=ExecutionMode.MULTI,
            steps=[step(registry, "fetch_dataset")],
        ),
    }


def reconciliation() -> ExecutorReconciliation:
    return ExecutorReconciliation(
        outcome=ExecutorOutcome.OPERATION_SUCCEEDED,
        execution_id=uuid4(),
        execution_version=5,
        result_summaries=[{"output": "artifacts/datasets/sample.csv"}],
    )


def capability(
    registry: ToolRegistry, raw: Any, *, context_limit: int = 50000
) -> tuple[ExecutionCapability, AsyncMock]:
    model = MagicMock()
    invoke = AsyncMock(return_value=raw)
    model.with_structured_output.return_value.ainvoke = invoke
    services = ExecutionCapability(
        Settings(planner_context_max_chars=context_limit),
        cast(Any, None),
        cast(Any, None),
        registry,
        model,
        cast(Any, None),
    )
    return services, invoke


@pytest.mark.asyncio
async def test_multi_receives_skill_instructions_and_exact_tool_contracts(
    registry: ToolRegistry,
) -> None:
    next_step = step(registry, "inspect_dataset")
    services, invoke = capability(
        registry,
        {
            "action": "APPEND_STEP",
            "rationale": "생성 후 데이터 검사",
            "next_step": next_step.model_dump(mode="json"),
        },
    )
    result = await services.adapt_multi_plan(
        state(registry, PlanningKind.TOOL_PLAN), reconciliation()
    )
    assert result.next_step == next_step
    payload = json.loads(invoke.call_args.args[0][1].content)
    assert payload["result"]["result_summaries"]
    catalog = payload["available_skills"]
    assert len(catalog) == len(registry.list_skills())
    inspection = next(x for x in catalog if x["name"] == "data-inspection")
    assert inspection["instructions"]
    tool = inspection["tools"][0]
    assert tool["skill"]["name"] == "data-inspection"
    assert tool["tool"]["name"] == "inspect_dataset"
    assert "path" in tool["parameters"]
    assert all("source" not in t for x in catalog for t in x["tools"])


@pytest.mark.asyncio
async def test_multi_still_rejects_mismatched_skill_and_tool(
    registry: ToolRegistry,
) -> None:
    wrong = step(registry, "inspect_dataset").model_copy(
        update={"skill": registry.get_tool("fetch_dataset").skill}
    )
    services, _ = capability(
        registry,
        {
            "action": "APPEND_STEP",
            "rationale": "invalid lineage",
            "next_step": wrong.model_dump(mode="json"),
        },
    )
    with pytest.raises(ValueError, match="mismatched Skill/Tool"):
        await services.adapt_multi_plan(
            state(registry, PlanningKind.TOOL_PLAN), reconciliation()
        )


@pytest.mark.asyncio
async def test_free_code_does_not_receive_analysis_catalog(
    registry: ToolRegistry,
) -> None:
    services, invoke = capability(
        registry, {"action": "FINALIZE", "rationale": "완료"}
    )
    await services.adapt_multi_plan(
        state(registry, PlanningKind.CUSTOM_CODE), reconciliation()
    )
    payload = json.loads(invoke.call_args.args[0][1].content)
    assert payload["available_skills"] == []


@pytest.mark.asyncio
async def test_multi_context_budget_fails_before_model(
    registry: ToolRegistry,
) -> None:
    services, invoke = capability(registry, {}, context_limit=1000)
    with pytest.raises(ValueError, match="character budget"):
        await services.adapt_multi_plan(
            state(registry, PlanningKind.TOOL_PLAN), reconciliation()
        )
    invoke.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("serialized", [False, True])
async def test_successful_step_history_restores_checkpoint_dicts(
    registry: ToolRegistry, serialized: bool
) -> None:
    repository = AgentRepository(cast(Any, None))
    repository.promotions = MagicMock()
    record = AsyncMock()
    repository.promotions.record_successful_steps = record
    expected = step(registry, "inspect_dataset")
    raw = expected.model_dump(mode="json") if serialized else expected
    await repository.record_successful_execution_steps(
        task_id=uuid4(),
        operation_id=uuid4(),
        plan_id=uuid4(),
        plan_revision_id=uuid4(),
        registry_snapshot_hash="snapshot",
        start_sequence=1,
        steps=[raw],
    )
    saved = record.call_args.kwargs["steps"][0]
    assert isinstance(saved, PlanStepDraft)
    assert saved == expected
    assert saved.skill == registry.get_tool("inspect_dataset").skill


@pytest.mark.asyncio
async def test_invalid_checkpoint_step_is_rejected_before_history_write() -> (
    None
):
    repository = AgentRepository(cast(Any, None))
    repository.promotions = MagicMock()
    record = AsyncMock()
    repository.promotions.record_successful_steps = record
    with pytest.raises(ValueError):
        await repository.record_successful_execution_steps(
            task_id=uuid4(),
            operation_id=uuid4(),
            plan_id=uuid4(),
            plan_revision_id=uuid4(),
            registry_snapshot_hash="snapshot",
            start_sequence=1,
            steps=[{"title": "incomplete"}],
        )
    record.assert_not_awaited()
