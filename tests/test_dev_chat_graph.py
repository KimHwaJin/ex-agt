from __future__ import annotations

import asyncio
import json
from copy import deepcopy
from pathlib import Path
from typing import Any
from uuid import UUID, uuid4

import pytest
from langchain_core.messages import AIMessage, HumanMessage
from langgraph.checkpoint.memory import InMemorySaver
from langgraph.types import Command

from ex_agent.api.contracts import TaskResponse
from ex_agent.dev_chat.client import AgentApiClient
from ex_agent.dev_chat.graph import build_chat_graph
from ex_agent.dev_chat.settings import ChatSettings


def task(status: str, version: int, **updates: Any) -> dict[str, Any]:
    return {
        "task_id": str(uuid4()),
        "user_id": "test",
        "project_id": "test",
        "session_id": "test",
        "status": status,
        "execution_id": None,
        "current_interrupt": None,
        "terminal_message": None,
        "version": version,
        "created_at": "2026-08-31T00:00:00Z",
        "updated_at": "2026-08-31T00:00:00Z",
        "created_by": "test",
        "updated_by": "test",
        **updates,
    }


class ScriptedApi(AgentApiClient):
    def __init__(self, stages: list[dict[str, Any]]) -> None:
        super().__init__(ChatSettings(_env_file=None))
        self.stages = stages
        self.created: list[dict[str, Any]] = []
        self.signals: list[dict[str, Any]] = []
        self.current: TaskResponse | None = None
        self.ignored_versions: list[int] = []

    async def create_task(self, session_id: str, body: dict[str, Any]) -> None:
        self.created.append(deepcopy(body))

    async def get_task(self, task_id: str) -> TaskResponse:
        assert self.current is not None
        return self.current

    async def send_signal(
        self,
        task_id: str,
        signal: dict[str, Any],
        idempotency_key: str,
        expected_version: int,
    ) -> None:
        self.signals.append(deepcopy(signal))

    async def watch(
        self,
        task_id: str,
        *,
        after_event_id: int,
        ignored_version: int,
        known_version: int | None = None,
    ) -> tuple[TaskResponse, int]:
        self.ignored_versions.append(ignored_version)
        stage = {**self.stages.pop(0), "task_id": task_id}
        self.current = TaskResponse.model_validate(stage)
        return self.current, after_event_id + 1


class PausingApi(ScriptedApi):
    def __init__(self, stages: list[dict[str, Any]]) -> None:
        super().__init__(stages)
        self.waiting = asyncio.Event()

    async def watch(
        self,
        task_id: str,
        *,
        after_event_id: int,
        ignored_version: int,
        known_version: int | None = None,
    ) -> tuple[TaskResponse, int]:
        if not self.stages:
            self.waiting.set()
            await asyncio.Event().wait()
        return await super().watch(
            task_id,
            after_event_id=after_event_id,
            ignored_version=ignored_version,
            known_version=known_version,
        )


def approve() -> Command:
    return Command(resume={"decisions": [{"type": "approve"}]})


@pytest.mark.asyncio
async def test_chat_input_answers_and_next_turn_gets_a_new_task() -> None:
    api = ScriptedApi(
        [
            task("SUCCEEDED", 3, terminal_message="첫 답변"),
            task("SUCCEEDED", 3, terminal_message="둘째 답변"),
        ]
    )
    graph = build_chat_graph(api, checkpointer=InMemorySaver())
    config = {"configurable": {"thread_id": str(uuid4())}}
    result = await graph.ainvoke(
        {"messages": [HumanMessage(id="first", content="안녕")]}, config
    )
    assert result["messages"][-1].content == "첫 답변"
    assert len(result["messages"]) == 2
    assert result["snapshot"]["task_id"] == result["task_id"]
    assert str(UUID(result["session_id"])) == result["session_id"]
    assert "__interrupt__" not in result
    result = await graph.ainvoke(
        {"messages": [HumanMessage(id="second", content="통계란?")]}, config
    )
    assert result["messages"][-1].content == "둘째 답변"
    assert len(result["messages"]) == 4
    assert len(api.created) == 2
    assert api.created[0]["task_id"] != api.created[1]["task_id"]


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ["ACCEPTED", "CLASSIFYING", "ANSWERING"])
async def test_slow_answer_finishes_without_refresh_or_approval(
    status: str,
) -> None:
    api = ScriptedApi(
        [task(status, 1), task("SUCCEEDED", 3, terminal_message="안녕하세요!")]
    )
    graph = build_chat_graph(api, checkpointer=InMemorySaver())
    config = {"configurable": {"thread_id": str(uuid4())}}
    result = await graph.ainvoke(
        {"messages": [HumanMessage(content="ㅎㅇㅎㅇ")]}, config
    )
    assert "Worker" not in result["messages"][-1].content
    assert "Task ID" not in result["messages"][-1].content
    assert "__interrupt__" not in result
    assert result["messages"][-1].content == "안녕하세요!"
    assert len(result["messages"]) == 2
    assert api.signals == []
    assert len(api.created) == 1


@pytest.mark.asyncio
async def test_qa_stream_does_not_emit_an_acceptance_message() -> None:
    api = ScriptedApi([task("SUCCEEDED", 3, terminal_message="안녕!")])
    graph = build_chat_graph(api, checkpointer=InMemorySaver())
    answers = []
    async for update in graph.astream(
        {"messages": [HumanMessage(content="ㅎㅇㅎㅇ")]},
        {"configurable": {"thread_id": str(uuid4())}},
        stream_mode="updates",
    ):
        for values in update.values():
            answers.extend(
                message.content
                for message in (values or {}).get("messages", [])
                if isinstance(message, AIMessage)
            )
    assert answers == ["안녕!"]


@pytest.mark.asyncio
async def test_mode_and_plan_approval_then_automatic_completion() -> None:
    execution_id = str(uuid4())
    plan = {
        "kind": "PLAN_REVIEW",
        "plan_revision_id": str(uuid4()),
        "plan_revision_number": 1,
        "public_payload_hash": "a" * 64,
        "risk": {"level": "LOW"},
        "plan": {"objective": "test", "steps": [], "execution_mode": "SINGLE"},
    }
    api = ScriptedApi(
        [
            task("PLANNING", 2, current_interrupt={"kind": "EXECUTION_MODE"}),
            task("WAITING_FOR_APPROVAL", 4, current_interrupt=plan),
            task(
                "EXECUTING",
                6,
                execution_id=execution_id,
                current_interrupt={"kind": "EXECUTOR_EVENT"},
            ),
            task(
                "SUCCEEDED",
                8,
                execution_id=execution_id,
                terminal_message="성공 리포트",
            ),
        ]
    )
    graph = build_chat_graph(api, checkpointer=InMemorySaver())
    config = {"configurable": {"thread_id": str(uuid4())}}
    result = await graph.ainvoke(
        {"messages": [{"type": "human", "content": "코드 실행", "id": "one"}]},
        config,
    )
    assert result["__interrupt__"][0].value["action_requests"][0]["name"] == (
        "EXECUTION_MODE"
    )
    result = await graph.ainvoke(approve(), config)
    assert result["review_kind"] == "PLAN_REVIEW"
    result = await graph.ainvoke(approve(), config)
    assert len(api.created) == 1
    assert "__interrupt__" not in result
    assert "성공 리포트" in result["messages"][-1].content
    assert execution_id in result["messages"][-1].content
    assert [signal["type"] for signal in api.signals] == [
        "EXECUTION_MODE",
        "PLAN_REVIEW",
    ]
    assert api.ignored_versions == [-1, 2, 4, 4]


@pytest.mark.asyncio
async def test_invalid_human_input_reinterrupts_without_backend_write() -> (
    None
):
    api = ScriptedApi(
        [
            task("PLANNING", 2, current_interrupt={"kind": "CLARIFICATION"}),
            task("SUCCEEDED", 3, terminal_message="완료"),
        ]
    )
    graph = build_chat_graph(api, checkpointer=InMemorySaver())
    config = {"configurable": {"thread_id": str(uuid4())}}
    await graph.ainvoke({"messages": [HumanMessage(content="분석")]}, config)
    invalid = await graph.ainvoke(approve(), config)
    assert "입력 오류" in str(invalid["__interrupt__"][0].value)
    assert api.signals == []
    result = await graph.ainvoke(
        Command(
            resume={
                "decisions": [
                    {
                        "type": "edit",
                        "edited_action": {
                            "name": "CLARIFICATION",
                            "args": {"answer": "매출"},
                        },
                    }
                ]
            }
        ),
        config,
    )
    assert "__interrupt__" not in result
    assert api.signals[0]["answer"] == "매출"


@pytest.mark.asyncio
async def test_external_cancel_waits_for_backend_confirmation() -> None:
    execution_id = str(uuid4())
    api = ScriptedApi(
        [
            task("EXECUTING", 4, execution_id=execution_id),
            task("CANCEL_REQUESTED", 5, execution_id=execution_id),
            task(
                "CANCELLED",
                6,
                execution_id=execution_id,
                terminal_message="Executor 취소 확인",
            ),
        ]
    )
    graph = build_chat_graph(api, checkpointer=InMemorySaver())
    config = {"configurable": {"thread_id": str(uuid4())}}
    statuses = []
    async for state in graph.astream(
        {"messages": [HumanMessage(content="실행")]},
        config,
        stream_mode="values",
    ):
        assert "__interrupt__" not in state
        if state.get("snapshot"):
            statuses.append(state["snapshot"]["status"])
            if statuses[-1] == "CANCEL_REQUESTED":
                assert "확인 대기" in state["messages"][-1].content
    assert statuses == ["EXECUTING", "CANCEL_REQUESTED", "CANCELLED"]
    assert api.signals == []


@pytest.mark.asyncio
@pytest.mark.parametrize("resume_with_input", [True, False])
async def test_stopped_observer_resumes_without_starting_another_task(
    resume_with_input: bool,
) -> None:
    execution_id = str(uuid4())
    api = PausingApi([task("EXECUTING", 4, execution_id=execution_id)])
    graph = build_chat_graph(api, checkpointer=InMemorySaver())
    config = {"configurable": {"thread_id": str(uuid4())}}
    observing = asyncio.create_task(
        graph.ainvoke({"messages": [HumanMessage(content="작업")]}, config)
    )
    await asyncio.wait_for(api.waiting.wait(), timeout=2)
    observing.cancel()
    with pytest.raises(asyncio.CancelledError):
        await observing
    saved = await graph.aget_state(config)
    assert saved.next == ("observe",)
    assert saved.values["review_kind"] == "RUNNING"
    assert not saved.tasks[0].interrupts
    api.stages.append(task("SUCCEEDED", 8, execution_id=execution_id))
    result = await graph.ainvoke(
        {"messages": [HumanMessage(content="다른 작업")]}
        if resume_with_input
        else None,
        config,
    )
    assert result["review_kind"] == "TERMINAL"
    assert len(api.created) == 1
    assert not api.signals


@pytest.mark.asyncio
async def test_many_observation_windows_stream_one_status_without_hitl() -> (
    None
):
    execution_id = str(uuid4())
    api = ScriptedApi(
        [task("EXECUTING", 5, execution_id=execution_id) for _ in range(100)]
        + [
            task("GENERATING_REPORT", 8, execution_id=execution_id),
            task("SUCCEEDED", 9, terminal_message="리포트 완료"),
        ]
    )
    graph = build_chat_graph(api, checkpointer=InMemorySaver())
    config = {"configurable": {"thread_id": str(uuid4())}}
    progress_ids = set()
    async for state in graph.astream(
        {"messages": [HumanMessage(content="EDA")]},
        config,
        stream_mode="values",
    ):
        assert "__interrupt__" not in state
        assert len(state["messages"]) <= 2
        if state.get("review_kind") == "RUNNING":
            progress_ids.add(state["messages"][-1].id)
            assert "Approve" not in state["messages"][-1].content
            if state["snapshot"]["status"] == "GENERATING_REPORT":
                assert "리포트" in state["messages"][-1].content
    saved = await graph.aget_state(config)
    assert saved.values["messages"][-1].content == "리포트 완료"
    assert len(progress_ids) == 1
    assert len(api.created) == 1
    assert not api.signals


@pytest.mark.asyncio
async def test_multi_material_change_still_requires_explicit_approval() -> (
    None
):
    plan = {
        "kind": "PLAN_REVIEW",
        "plan_revision_id": str(uuid4()),
        "plan_revision_number": 2,
        "public_payload_hash": "b" * 64,
        "risk": {"level": "LOW"},
        "plan": {"objective": "변경 계획", "steps": []},
    }
    api = ScriptedApi(
        [
            task("EXECUTING", 5, execution_id=str(uuid4())),
            task("WAITING_FOR_APPROVAL", 7, current_interrupt=plan),
            task("SUCCEEDED", 10, terminal_message="완료"),
        ]
    )
    graph = build_chat_graph(api, checkpointer=InMemorySaver())
    config = {"configurable": {"thread_id": str(uuid4())}}
    result = await graph.ainvoke(
        {"messages": [HumanMessage(content="EDA")]}, config
    )
    assert result["review_kind"] == "PLAN_REVIEW"
    assert result["__interrupt__"][0].value["action_requests"][0]["name"] == (
        "PLAN_REVIEW"
    )
    assert not api.signals
    result = await graph.ainvoke(approve(), config)
    assert result["review_kind"] == "TERMINAL"
    assert len(api.signals) == 1
    assert api.signals[0]["plan_revision_number"] == 2


@pytest.mark.asyncio
async def test_task_submission_identity_is_stable_on_replay() -> None:
    api = ScriptedApi([task("SUCCEEDED", 3), task("SUCCEEDED", 3)])
    config = {"configurable": {"thread_id": str(uuid4())}}
    ids = []
    for _ in range(2):
        graph = build_chat_graph(api, checkpointer=InMemorySaver())
        await graph.ainvoke(
            {"messages": [HumanMessage(id="stable", content="same")]}, config
        )
        ids.append(api.created[-1]["idempotency_key"])
    assert ids[0] == ids[1]


def test_graph_input_schema_and_registration() -> None:
    graph = build_chat_graph(ScriptedApi([]))
    assert set(graph.get_input_jsonschema()["properties"]) == {"messages"}
    root = Path(__file__).parents[1]
    config = json.loads((root / "langgraph.json").read_text())
    assert config["graphs"]["agent"].endswith("entrypoint.py:graph")
    assert config["env"] == ".env.chat-ui"
