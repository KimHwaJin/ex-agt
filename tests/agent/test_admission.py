import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from agent.admission.contracts import ApiRequest, RequestRecord
from agent.admission.recovery import RequestRecovery
from agent.admission.service import (
    UnsafeRecoveryError,
    next_invocation,
    owns_pending,
)
from agent.graph import build_session_graph, checkpoint_serializer
from ex_agent.domain.enums import Intent
from tests.agent.support import services, turn


def record(**kwargs):
    command = ApiRequest(request_id=uuid4(), turn=turn(), kind="START")
    return RequestRecord(
        command=command,
        fingerprint=command.fingerprint,
        target_node="begin_task",
        base_checkpoint_id=None,
        state="RUNNING",
        attempts=1,
        last_error=None,
        next_attempt_at=datetime.now(UTC),
        created_at=datetime.now(UTC),
        updated_at=datetime.now(UTC),
        created_by="user",
        updated_by="AGENT",
        **kwargs,
    )


def accepted_state(request):
    return {
        "api_receipts": {
            str(request.command.request_id): {
                "fingerprint": request.fingerprint,
                "task_id": request.command.turn.active_task_id,
            }
        },
        "invocation_owner": {
            "source": "API",
            "id": str(request.command.request_id),
        },
    }


async def test_input_receipt_is_checkpointed_before_business_steps():
    request = record()
    service = services(intent=Intent.GENERAL_QA)
    service.classify_intent.side_effect = RuntimeError(
        "classification crashed"
    )
    graph = build_session_graph(
        service,
        AsyncMock(),
        checkpointer=InMemorySaver(serde=checkpoint_serializer()),
    )
    config = {
        "configurable": {
            "thread_id": request.command.turn.session_id,
            "api_action": request.action,
        }
    }
    with pytest.raises(RuntimeError, match="classification crashed"):
        await graph.ainvoke(
            {"turn": request.command.turn.model_dump(mode="json")}, config
        )
    snapshot = await graph.aget_state(config)
    assert owns_pending(snapshot, request)
    assert next_invocation(snapshot, request) == (True, None)
    service.classify_intent.side_effect = None
    await graph.ainvoke(None, config)
    after = await graph.aget_state(config)
    assert next_invocation(after, request) == (False, None)


def test_old_api_request_never_recovers_worker_owned_pending_nodes():
    request = record()
    state = accepted_state(request)
    state["invocation_owner"] = {"source": "EXECUTOR", "id": str(uuid4())}
    snapshot = SimpleNamespace(
        values=state, next=("generate_report",), tasks=[]
    )
    assert next_invocation(snapshot, request) == (False, None)
    state["api_receipts"][str(request.command.request_id)]["fingerprint"] = (
        "bad"
    )
    with pytest.raises(UnsafeRecoveryError, match="identity mismatch"):
        next_invocation(snapshot, request)


def test_accepted_input_with_unknown_pending_owner_fails_closed():
    request = record()
    state = accepted_state(request)
    state.pop("invocation_owner")
    snapshot = SimpleNamespace(
        values=state, next=("submit_execution",), tasks=[]
    )
    with pytest.raises(UnsafeRecoveryError, match="owner is unknown"):
        next_invocation(snapshot, request)


@pytest.mark.parametrize("kind", ["RESUME", "CANCEL"])
def test_resume_requires_interrupt_and_rejects_executor_payload(kind):
    with pytest.raises(ValueError, match="interrupt ID"):
        ApiRequest(request_id=uuid4(), turn=turn(), kind=kind)
    with pytest.raises(ValueError, match="belong to Worker"):
        ApiRequest(
            request_id=uuid4(),
            turn=turn(),
            kind=kind,
            interrupt_id="test",
            payload={
                "type": "EXECUTOR_BOUNDARY",
                "execution_id": str(uuid4()),
                "event_id": str(uuid4()),
                "event_sequence": 1,
                "event_type": "execution.completed",
            },
        )


async def test_recovery_has_bounded_concurrency_and_keeps_other_requests():
    service = AsyncMock()
    service.store.due.return_value = list(range(8))
    current = peak = 0

    async def execute(request_id):
        nonlocal current, peak
        current += 1
        peak = max(peak, current)
        try:
            await asyncio.sleep(0.001)
            if request_id == 0:
                raise RuntimeError("one unavailable dependency")
        finally:
            current -= 1

    service.execute.side_effect = execute
    loop = RequestRecovery(service, concurrency=2)
    assert await loop.once() == 8
    assert peak == 2 and service.execute.await_count == 8


async def test_recovery_stops_without_waiting_for_another_poll():
    service = AsyncMock()
    service.store.due.return_value = []
    stop = asyncio.Event()
    loop = RequestRecovery(service, poll_seconds=60)
    running = asyncio.create_task(loop.run(stop))
    await asyncio.sleep(0)
    stop.set()
    await asyncio.wait_for(running, timeout=1)
