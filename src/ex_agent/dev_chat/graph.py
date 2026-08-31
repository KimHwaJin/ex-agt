from __future__ import annotations

import hashlib
from typing import Any, cast
from uuid import NAMESPACE_URL, uuid5

import httpx
from langchain_core.messages import AIMessage, HumanMessage, RemoveMessage
from langchain_core.runnables import RunnableConfig
from langgraph.graph import END, START, MessagesState, StateGraph
from langgraph.types import interrupt

from ex_agent.dev_chat.client import (
    HUMAN_INTERRUPTS,
    AgentApiClient,
    needs_input,
)
from ex_agent.dev_chat.decisions import decision_signal, signal_key
from ex_agent.dev_chat.presentation import (
    describe,
    describe_progress,
    public_value,
    review_card,
)
from ex_agent.domain.enums import TaskStatus


class ChatState(MessagesState):
    owner_thread_id: str
    task_id: str
    create_body: dict[str, Any]
    session_id: str
    should_submit: bool
    event_cursor: int
    ignored_version: int
    snapshot: dict[str, Any]
    review_kind: str
    signal: dict[str, Any] | None


class ChatNodes:
    def __init__(self, api: AgentApiClient) -> None:
        self.api = api

    async def prepare(
        self, state: ChatState, config: RunnableConfig
    ) -> dict[str, Any]:
        thread_id = str(config.get("configurable", {}).get("thread_id", ""))
        if not thread_id:
            raise ValueError("Agent Chat UI thread_id is required")
        messages = state.get("messages", [])
        if not messages or not isinstance(messages[-1], HumanMessage):
            if state.get("task_id"):
                return {"should_submit": False}
            raise ValueError("텍스트 질문을 입력해주세요.")
        message = messages[-1]
        text = _text_input(message)
        if state.get("owner_thread_id") == thread_id and state.get("task_id"):
            try:
                active = await self.api.get_task(state["task_id"])
            except httpx.HTTPStatusError as error:
                if error.response.status_code != 404:
                    raise
            else:
                if not TaskStatus(active.status).is_terminal:
                    # Never turn ordinary input into implicit approval or
                    # start a second task while this task is still active.
                    return {"should_submit": False}
        identity = (
            f"ex-agent-chat:{self.api.settings.user_id}:"
            f"{self.api.settings.project_id}:{thread_id}:"
            f"{message.id}:{hashlib.sha256(text.encode()).hexdigest()}"
        )
        task_id = str(uuid5(NAMESPACE_URL, identity))
        body = {
            "task_id": task_id,
            "input_message_id": str(uuid5(NAMESPACE_URL, identity + ":input")),
            "content": text,
            "idempotency_key": f"chat-ui:start:{task_id}",
        }
        return {
            "owner_thread_id": thread_id,
            "task_id": task_id,
            "session_id": str(uuid5(NAMESPACE_URL, f"chat-ui:{thread_id}")),
            "create_body": body,
            "should_submit": True,
            "event_cursor": 0,
            "ignored_version": -1,
            "signal": None,
            "snapshot": {},
        }

    async def submit(self, state: ChatState) -> dict[str, Any]:
        await self.api.create_task(state["session_id"], state["create_body"])
        # Keep task IDs in state for tracing, not in conversational answers.
        return {"should_submit": False}

    async def observe(self, state: ChatState) -> dict[str, Any]:
        task, cursor = await self.api.watch(
            state["task_id"],
            after_event_id=state.get("event_cursor", 0),
            ignored_version=state.get("ignored_version", -1),
            known_version=state.get("snapshot", {}).get("version", -1),
        )
        snapshot = public_value(task.model_dump(mode="json"))
        status_id = f"{task.task_id}:status"
        messages: list[AIMessage | RemoveMessage] = []
        if TaskStatus(task.status).is_terminal:
            kind = "TERMINAL"
            content = task.terminal_message or f"작업 상태: {task.status}"
            if task.execution_id:
                content += f"\n\nExecution ID: `{task.execution_id}`"
            message_id = f"{task.task_id}:result"
            if any(
                message.id == status_id
                for message in state.get("messages", [])
            ):
                messages.append(RemoveMessage(id=status_id))
        else:
            kind = (
                str(task.current_interrupt["kind"])
                if task.current_interrupt
                and needs_input(task, state.get("ignored_version", -1))
                else "RUNNING"
            )
            content = (
                describe_progress(snapshot)
                if kind == "RUNNING"
                else describe(snapshot, kind)
            )
            message_id = status_id
        messages.append(AIMessage(id=message_id, content=content))
        return {
            "snapshot": snapshot,
            "event_cursor": cursor,
            "review_kind": kind,
            "messages": messages,
        }

    def review(self, state: ChatState) -> dict[str, Any]:
        error = ""
        while True:
            raw = interrupt(
                review_card(state["snapshot"], state["review_kind"], error)
            )
            try:
                signal = decision_signal(
                    raw, state["snapshot"], state["review_kind"]
                )
                return {"signal": signal}
            except ValueError as invalid:
                error = str(invalid)

    async def send(self, state: ChatState) -> dict[str, Any]:
        signal = state["signal"]
        if signal is None:
            return {}
        snapshot = state["snapshot"]
        await self.api.send_signal(
            state["task_id"],
            signal,
            signal_key(snapshot, signal),
            snapshot["version"],
        )
        return {
            "ignored_version": snapshot["version"],
            "signal": None,
            "messages": [
                AIMessage(
                    id=f"{state['task_id']}:status",
                    content=(
                        "취소를 요청했습니다. Executor 확인을 기다립니다."
                        if signal["type"] == "CANCEL_REQUESTED"
                        else "입력을 전달했습니다. Worker 처리를 기다립니다."
                    ),
                )
            ],
        }


def _text_input(message: HumanMessage) -> str:
    if isinstance(message.content, str):
        text = message.content
    else:
        parts = []
        for block in message.content:
            if not isinstance(block, dict) or block.get("type") != "text":
                raise ValueError(
                    "현재 테스트 어댑터는 텍스트 입력만 지원합니다."
                )
            if not isinstance(block.get("text"), str):
                raise ValueError("text는 문자열이어야 합니다.")
            parts.append(block["text"])
        text = "\n".join(parts)
    if not text.strip():
        raise ValueError("텍스트 질문을 입력해주세요.")
    return text


def build_chat_graph(api: AgentApiClient, *, checkpointer: Any = None) -> Any:
    nodes = ChatNodes(api)
    graph = StateGraph(
        cast(Any, ChatState), input_schema=cast(Any, MessagesState)
    )
    graph.add_node("prepare", nodes.prepare)
    graph.add_node("submit", nodes.submit)
    graph.add_node("observe", nodes.observe)
    graph.add_node("review", nodes.review)
    graph.add_node("send", nodes.send)
    graph.add_edge(START, "prepare")
    graph.add_conditional_edges(
        "prepare",
        lambda state: "submit" if state["should_submit"] else "observe",
        ["submit", "observe"],
    )
    graph.add_edge("submit", "observe")
    graph.add_conditional_edges(
        "observe",
        lambda state: (
            END
            if state["review_kind"] == "TERMINAL"
            else "review"
            if state["review_kind"] in HUMAN_INTERRUPTS
            else "observe"
        ),
        [END, "review", "observe"],
    )
    graph.add_edge("review", "send")
    graph.add_edge("send", "observe")
    # Agent Server owns ONLY this UI adapter's checkpoint. Worker retains its
    # own business graph and PostgreSQL checkpointer without any shared saver.
    # Observation windows are not business/model iterations. A five-day job
    # already needs 14,400 idle windows at the default 30 seconds. Keep a
    # finite development guard, independent of the Worker's planning budget.
    return graph.compile(checkpointer=checkpointer).with_config(
        recursion_limit=1_000_000
    )
