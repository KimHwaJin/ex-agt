from contextlib import asynccontextmanager
from unittest.mock import AsyncMock, MagicMock
from uuid import uuid4

from langgraph.checkpoint.memory import InMemorySaver

from agent.admission.contracts import ApiRequest
from agent.admission.service import AdmissionService
from agent.admission.store import RequestStore
from agent.graph import build_session_graph, checkpoint_serializer
from ex_agent.domain.enums import ExecutionMode, Intent
from tests.agent.effect_support import effect_harness
from tests.agent.support import LocalGuard, boundary, services


@asynccontextmanager
async def admission_harness(
    tmp_path,
    monkeypatch,
    *,
    saver=None,
    guard=None,
    bindings=None,
    mode=ExecutionMode.SINGLE,
    intent=Intent.DATA_ANALYSIS_EXECUTION,
):
    async with effect_harness(tmp_path, mode=mode, create_task=False) as h:
        fake = services(mode=mode, intent=intent)
        for method in (
            "classify_intent",
            "review_request_risk",
            "search_workflows",
            "review_compiled_code_risk",
            "answer_question",
        ):
            monkeypatch.setattr(h.service, method, getattr(fake, method))
        monkeypatch.setattr(
            h.service, "build_plan", AsyncMock(return_value=h.state["plan"])
        )
        h.bindings = bindings or MagicMock(register=AsyncMock())
        h.guard = guard or LocalGuard()
        h.graph = build_session_graph(
            h.service,
            h.bindings,
            checkpointer=saver or InMemorySaver(serde=checkpoint_serializer()),
        )
        h.store = RequestStore(h.sessions)
        h.host = AdmissionService(
            h.graph, h.guard, h.store, retry_seconds=0.01
        )
        h.command = ApiRequest(request_id=uuid4(), turn=h.task, kind="START")
        yield h


async def snapshot(h):
    return await h.graph.aget_state(
        {"configurable": {"thread_id": h.task.session_id}}
    )


async def decision_request(h, decision="APPROVE"):
    waiting = boundary(await snapshot(h))
    payload = {
        "type": "PLAN_REVIEW",
        "decision": decision,
        "feedback": "다시 계획" if decision == "REVISE" else None,
        **{
            key: waiting.value[key]
            for key in (
                "plan_revision_id",
                "plan_revision_number",
                "public_payload_hash",
            )
        },
    }
    return ApiRequest(
        request_id=uuid4(),
        turn=h.task,
        kind="RESUME",
        interrupt_id=waiting.id,
        payload=payload,
    )
