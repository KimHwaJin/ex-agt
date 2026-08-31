from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast
from unittest.mock import AsyncMock, MagicMock, patch
from uuid import uuid4

import httpx
import pytest
import respx
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from ex_agent.application.capabilities.execution import ExecutionCapability
from ex_agent.application.capabilities.planning import compile_and_persist_plan
from ex_agent.config import Settings
from ex_agent.dev_chat.decisions import decision_signal
from ex_agent.dev_chat.presentation import describe
from ex_agent.domain.contracts import (
    IntentDecision,
    PersistedPlan,
    PlanDraft,
    RiskReview,
    WorkflowCandidate,
)
from ex_agent.domain.enums import (
    ExecutionMode,
    Intent,
    PlanningKind,
    RiskLevel,
)
from ex_agent.executor.client import ExecutorClient
from ex_agent.graph.builder import build_workflow_graph
from ex_agent.graph.node_groups.conversation import ConversationNodes
from ex_agent.graph.node_groups.planning import PlanningNodes
from ex_agent.graph.routes import route_intent
from ex_agent.middleware.planning import (
    PlanModeMismatchError,
    PlannerContext,
    PlanOutputMiddleware,
    _planning_system_message,
)
from ex_agent.planners.agent import PlannerAgent
from ex_agent.tools.registry import ToolRegistry


def plan(mode: ExecutionMode, count: int = 1) -> PlanDraft:
    return PlanDraft.model_validate(
        {
            "objective": "mode regression",
            "strategy_summary": "Generate the requested plan",
            "execution_mode": mode,
            "steps": [
                {
                    "sequence": index,
                    "title": f"Step {index}",
                    "purpose": "test",
                    "planning_kind": "CUSTOM_CODE",
                    "custom_code": f"def step_{index}():\n    return 1\n"
                    f"step_{index}()",
                    "selection_rationale": "test fixture",
                }
                for index in range(count)
            ],
        }
    )


def context(mode: ExecutionMode) -> PlannerContext:
    return PlannerContext(
        task_id="mode-test",
        user_request="complete analysis",
        planning_kind=PlanningKind.CUSTOM_CODE,
        execution_mode=mode,
        runtime_profile="basic",
        request_risk_review_id="risk-test",
        request_risk_allowed=True,
    )


@pytest.mark.parametrize("intent", list(Intent))
@pytest.mark.parametrize("mode", [None, *ExecutionMode])
async def test_classification_preserves_explicit_mode_only_for_execution(
    intent: Intent, mode: ExecutionMode | None
):
    services = MagicMock()
    decision = IntentDecision(
        intent=intent,
        confidence=1,
        decision_summary="model decision",
        requested_execution_mode=mode,
    )
    services.classify_intent = AsyncMock(return_value=decision)
    updates = await ConversationNodes(services).classify_intent({})
    if intent in {Intent.CODE_EXECUTION, Intent.DATA_ANALYSIS_EXECUTION}:
        assert updates.get("execution_mode") is mode
        assert updates["planning_kind"] is (
            PlanningKind.CUSTOM_CODE
            if intent is Intent.CODE_EXECUTION
            else PlanningKind.TOOL_PLAN
        )
        assert route_intent(cast(Any, updates)) == (
            "choose_execution_mode"
            if intent is Intent.CODE_EXECUTION and mode is None
            else "review_request_risk"
        )
    else:
        assert "execution_mode" not in updates


@pytest.mark.parametrize("mode", [None, "SINGLE", "MULTI"])
async def test_workflow_search_and_decline_preserve_dynamic_mode(mode):
    services = MagicMock()
    services.search_workflows = AsyncMock(return_value=[])
    state: Any = {"active_task_id": "mode-test"}
    if mode:
        state["execution_mode"] = mode
    nodes = PlanningNodes(services)
    state.update(await nodes.search_workflows(state))
    expected = mode or "MULTI"
    assert state["execution_mode"] == expected
    captured = {}

    def resume(payload):
        captured.update(payload)
        return {
            "type": "WORKFLOW_SELECTION",
            "proposal_version": 1,
            "public_payload_hash": "0" * 64,
        }

    with patch(
        "ex_agent.graph.node_groups.planning.interrupt", side_effect=resume
    ):
        result = nodes.choose_workflow(state)
    assert result["execution_mode"] == expected
    assert captured["dynamic_execution_mode"] == expected
    task = {"current_interrupt": captured}
    assert f"동적 {expected} 계획" in describe(task, "WORKFLOW_SELECTION")
    signal = decision_signal(
        {"decisions": [{"type": "approve"}]}, task, "WORKFLOW_SELECTION"
    )
    assert signal is not None and signal["workflow_version_id"] is None


def test_selected_workflow_explicitly_approves_single():
    candidate = WorkflowCandidate(
        workflow_version_id=uuid4(),
        name="fixed",
        description="fixed plan",
        score=1,
        plan=plan(ExecutionMode.SINGLE),
        public_payload_hash="a" * 64,
    )
    state: Any = {
        "active_task_id": "test",
        "workflow_proposal_version": 1,
        "workflow_candidates": [candidate],
        "execution_mode": ExecutionMode.MULTI,
    }
    with patch(
        "ex_agent.graph.node_groups.planning.interrupt",
        return_value={
            "type": "WORKFLOW_SELECTION",
            "workflow_version_id": str(candidate.workflow_version_id),
            "proposal_version": 1,
            "public_payload_hash": candidate.public_payload_hash,
        },
    ):
        result = PlanningNodes(MagicMock()).choose_workflow(state)
    assert result["execution_mode"] is ExecutionMode.SINGLE
    assert result["planning_kind"] is PlanningKind.FIXED_WORKFLOW


@pytest.mark.parametrize("selected", list(ExecutionMode))
async def test_middleware_rejects_opposite_mode_instead_of_relabeling(
    selected,
):
    returned = (
        ExecutionMode.MULTI
        if selected is ExecutionMode.SINGLE
        else ExecutionMode.SINGLE
    )
    draft = plan(returned)
    middleware = PlanOutputMiddleware(ToolRegistry(Path("skills")))
    with pytest.raises(PlanModeMismatchError, match="selected execution mode"):
        await middleware.aafter_agent(
            cast(Any, {"structured_response": draft}),
            SimpleNamespace(context=context(selected)),
        )
    assert draft.execution_mode is returned


async def test_single_plan_keeps_all_steps_and_prompt_requires_completeness():
    draft = plan(ExecutionMode.SINGLE, 3)
    planner_context = context(ExecutionMode.SINGLE)
    middleware = PlanOutputMiddleware(ToolRegistry(Path("skills")))
    result = await middleware.aafter_agent(
        cast(Any, {"structured_response": draft}),
        SimpleNamespace(context=planner_context),
    )
    assert result is not None
    assert len(result["structured_response"].steps) == 3
    message = _planning_system_message(planner_context, [], max_chars=10000)
    assert "include ALL ordered Steps" in message.content
    assert "authoritative" in message.content


@pytest.mark.parametrize("fail_again", [False, True])
async def test_mode_correction_is_bounded_and_retains_authoritative_context(
    fail_again, monkeypatch
):
    graph = MagicMock()
    mismatch = PlanModeMismatchError("selected execution mode is SINGLE")
    graph.ainvoke = AsyncMock(
        side_effect=[
            mismatch,
            mismatch
            if fail_again
            else {"structured_response": plan(ExecutionMode.SINGLE, 3)},
        ]
    )
    monkeypatch.setattr(
        "ex_agent.planners.agent.create_agent", lambda **kwargs: graph
    )
    agent = PlannerAgent(
        Settings(), ToolRegistry(Path("skills")), model=MagicMock()
    )
    selected = context(ExecutionMode.SINGLE)
    if fail_again:
        with pytest.raises(PlanModeMismatchError):
            await agent.plan(selected)
    else:
        result = await agent.plan(selected)
        assert result.execution_mode is ExecutionMode.SINGLE
        assert len(result.steps) == 3
    assert graph.ainvoke.await_count == 2
    assert graph.ainvoke.call_args.kwargs["context"] is selected
    correction = graph.ainvoke.call_args.args[0]["messages"][-1]["content"]
    assert "SINGLE" in correction


@pytest.mark.parametrize("mode", list(ExecutionMode))
async def test_graph_preserves_mode_through_search_and_revision(mode):
    services = MagicMock()
    services.update_status = AsyncMock()
    services.classify_intent = AsyncMock(
        return_value=IntentDecision(
            intent=Intent.DATA_ANALYSIS_EXECUTION,
            confidence=1,
            decision_summary="explicit strategy",
            requested_execution_mode=mode,
        )
    )
    risk = RiskReview(
        level=RiskLevel.LOW, summary="test", recommended_action="ALLOW"
    )
    services.review_request_risk = AsyncMock(return_value=risk)
    services.review_compiled_code_risk = AsyncMock(return_value=risk)
    services.search_workflows = AsyncMock(return_value=[])
    services.build_plan = AsyncMock(
        side_effect=lambda state: plan(
            state["execution_mode"], 3 if mode is ExecutionMode.SINGLE else 1
        )
    )
    services.compile_and_persist_plan = AsyncMock(
        side_effect=lambda state, draft: PersistedPlan(
            plan_id=uuid4(),
            plan_revision_id=uuid4(),
            plan_revision_number=state.get("plan_revision_number", 0) + 1,
            public_payload_hash="a" * 64,
            compiled_bundle_id=uuid4(),
        )
    )
    graph = build_workflow_graph(services, checkpointer=InMemorySaver())
    config = {"configurable": {"thread_id": str(uuid4())}}
    state = await graph.ainvoke(
        {"active_task_id": str(uuid4()), "user_message": "explicit mode"},
        config,
    )
    boundary = state["__interrupt__"][0].value
    assert boundary["kind"] == "PLAN_REVIEW"
    assert boundary["plan"]["execution_mode"] == mode.value
    revised = await graph.ainvoke(
        Command(
            resume={
                "type": "PLAN_REVIEW",
                "decision": "REVISE",
                "feedback": "Add output validation",
                **{
                    key: boundary[key]
                    for key in (
                        "plan_revision_id",
                        "plan_revision_number",
                        "public_payload_hash",
                    )
                },
            }
        ),
        config,
    )
    assert revised["execution_mode"] == mode
    assert revised["plan"].execution_mode is mode
    assert revised["plan_revision_number"] == 2
    services.submit_execution.assert_not_called()


async def test_mismatched_mode_cannot_be_compiled_or_submitted():
    state: Any = {
        "execution_mode": ExecutionMode.SINGLE,
        "plan": plan(ExecutionMode.MULTI),
    }
    repository, compiler, executor = MagicMock(), MagicMock(), MagicMock()
    with pytest.raises(ValueError, match="does not match"):
        await compile_and_persist_plan(
            Settings(), repository, MagicMock(), compiler, state, state["plan"]
        )
    capability = ExecutionCapability(
        Settings(), repository, executor, MagicMock(), MagicMock(), compiler
    )
    with pytest.raises(ValueError, match="does not match"):
        await capability.submit_execution(state)
    assert not repository.mock_calls
    assert not compiler.mock_calls
    assert not executor.mock_calls


@pytest.mark.parametrize("mode", list(ExecutionMode))
@respx.mock
async def test_executor_body_preserves_mode_and_single_submits_all_steps(mode):
    repository = MagicMock()
    repository.approved_steps = AsyncMock(
        return_value=[
            SimpleNamespace(
                sequence=index,
                compiled_source_path=f"test/1/step-{index}.py",
                compiled_source_sha256="a" * 64,
                timeout_seconds=300,
                skill_ref=None,
                tool_ref=None,
                parameters={},
            )
            for index in range(3)
        ]
    )
    repository.bind_execution = AsyncMock()
    route = respx.post("http://executor/api/v1/executions").mock(
        return_value=httpx.Response(
            202,
            json={
                "execution_id": str(uuid4()),
                "operation": {"operation_id": str(uuid4()), "steps": []},
                "state": {"status": "QUEUED", "version": 1},
            },
        )
    )
    client = ExecutorClient("http://executor/api/v1", timeout_seconds=1)
    capability = ExecutionCapability(
        Settings(), repository, client, MagicMock(), MagicMock(), MagicMock()
    )
    try:
        await capability.submit_execution(
            {
                "execution_mode": mode,
                "plan": plan(mode, 3),
                "plan_revision_id": str(uuid4()),
                "plan_revision_number": 1,
                "active_task_id": str(uuid4()),
                "user_id": "user",
                "project_id": "project",
                "session_id": "session",
                "runtime_profile": "basic",
            }
        )
    finally:
        await client.close()
    payload = json.loads(route.calls.last.request.content)
    assert payload["lifecycle"]["operation_mode"] == mode.value
    assert len(payload["operation"]["spec"]["steps"]) == (
        3 if mode is ExecutionMode.SINGLE else 1
    )
