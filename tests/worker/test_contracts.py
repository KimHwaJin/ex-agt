import subprocess
import sys
from typing import Any, cast
from uuid import uuid4

import pytest

from worker import EventContext, ExecutorEvent
from worker.consumer import AckDecision, HandlerResult, StreamMessage


def test_thread_identity_is_session_not_task():
    event = ExecutorEvent(
        event_id=uuid4(),
        execution_id=uuid4(),
        event_type="execution.completed",
        schema_version="1.0",
        event_sequence=1,
        occurred_at="now",
        payload={},
    )
    context = EventContext(
        "n", "session", "task", event.execution_id, uuid4(), event
    )
    assert context.graph_config == {"configurable": {"thread_id": "session"}}


def test_core_imports_without_agent_or_langgraph():
    code = """
import importlib.abc
import sys
class Block(importlib.abc.MetaPathFinder):
    def find_spec(self, fullname, path=None, target=None):
        if fullname.split('.')[0] in {
            'agent', 'api', 'ex_agent', 'langgraph', 'langchain',
            'langchain_core', 'fastapi',
        }:
            raise ImportError(fullname)
sys.meta_path.insert(0, Block())
from worker import ExecutorWorker
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        check=False,
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr


@pytest.mark.asyncio
async def test_defer_does_not_ack_or_increment_retry_budget():
    # Exercise the copied runtime through its actual finalization boundary.
    from worker.consumer import (
        RedisStreamConsumer,
        RedisStreamConsumerConfig,
    )

    class NoRedis:
        def __getattr__(self, name):
            raise AssertionError(f"DEFER unexpectedly called Redis: {name}")

    class Handler:
        def lock_key(self, message):
            return None

        async def handle(self, message):
            return HandlerResult(AckDecision.DEFER)

    consumer = RedisStreamConsumer(
        cast(Any, NoRedis()),
        RedisStreamConsumerConfig("s", "g", "c"),
        lambda _: Handler(),
    )
    result = await consumer._handle_and_finalize(
        "c",
        Handler(),
        StreamMessage("1-0", {}),
    )
    assert result.decision == AckDecision.DEFER
