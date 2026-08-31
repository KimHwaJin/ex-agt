"""Opt-in smoke: creates isolated QA tasks in the selected running service."""

import json
import os
from typing import cast
from uuid import uuid4

import pytest
from langchain_core.messages import HumanMessage
from langgraph.checkpoint.memory import InMemorySaver
from pydantic import HttpUrl

from ex_agent.dev_chat.client import AgentApiClient
from ex_agent.dev_chat.graph import build_chat_graph
from ex_agent.dev_chat.settings import ChatSettings

_API_URL = os.getenv("EX_AGENT_TEST_LIVE_API_URL")
pytestmark = pytest.mark.skipif(not _API_URL, reason="Live API smoke disabled")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "message",
    [
        "ㅎㅇㅎㅇ",
        "평균과 중앙값의 차이를 두 문장으로 설명해줘.",
        "너는 무슨 일을 도와줄 수 있어?",
    ],
)
async def test_live_chat_qa_without_execution(message: str) -> None:
    api = AgentApiClient(
        ChatSettings(
            _env_file=None,
            api_url=HttpUrl(cast(str, _API_URL)),
            user_id="chat-qa-smoke-user",
            project_id="chat-qa-smoke-project",
            watch_seconds=60,
        )
    )
    graph = build_chat_graph(api, checkpointer=InMemorySaver())
    result = await graph.ainvoke(
        {"messages": [HumanMessage(content=message, id=str(uuid4()))]},
        {"configurable": {"thread_id": str(uuid4())}},
    )
    snapshot = result["snapshot"]
    print(json.dumps({"input": message, "task": snapshot}, ensure_ascii=False))
    assert "__interrupt__" not in result, snapshot
    assert snapshot["status"] == "SUCCEEDED", snapshot
    assert snapshot["execution_id"] is None
    assert snapshot["current_interrupt"] is None
    assert len(result["messages"]) == 2
    content = result["messages"][-1].content
    assert content == snapshot["terminal_message"]
    assert content
    assert "Worker" not in content
    assert "Task ID" not in content
    assert "SINGLE" not in content
    assert "MULTI" not in content
