import asyncio
from copy import deepcopy
from unittest.mock import AsyncMock
from uuid import uuid4

import httpx
import pytest

from agent.effects.files import capture_files, input_file, restore_files
from agent.effects.journal import EffectJournal
from agent.effects.runner import ExecutorEffectSender
from agent.effects.store import EffectRecord, digest
from agent.graph import checkpoint_serializer
from ex_agent.config import Settings
from ex_agent.domain.contracts import MultiDecision, PlanDraft, PlanStepDraft
from ex_agent.domain.enums import ExecutionMode, MultiAction
from ex_agent.executor.client import ExecutorClient
from ex_agent.graph.node_groups.execution import ExecutionNodes
from tests.agent.support import services
from tests.test_execution_mode_policy import plan


async def test_adaptive_plan_is_valid_after_checkpoint_roundtrip():
    service = services(mode=ExecutionMode.MULTI)
    draft = plan(ExecutionMode.MULTI, 2)
    service.adapt_multi_plan.return_value = MultiDecision(
        action=MultiAction.APPEND_STEP,
        rationale="next",
        next_step=draft.steps[1],
    )
    updates = await ExecutionNodes(service).adapt_multi_plan(
        {
            "plan": draft,
            "executor_reconciliation": {
                "outcome": "OPERATION_SUCCEEDED",
                "execution_id": str(uuid4()),
                "execution_version": 2,
            },
        }
    )
    serializer = checkpoint_serializer()
    restored = serializer.loads_typed(serializer.dumps_typed(updates))
    assert isinstance(restored["plan"], PlanDraft)
    assert isinstance(restored["plan"].steps[0], PlanStepDraft)
    assert restored["plan"].steps[0].sequence == 0


def test_request_digest_is_canonical_and_rejects_nan():
    assert digest({"a": 1, "b": 2}) == digest({"b": 2, "a": 1})
    assert digest({"a": 1}) != digest({"a": 2})
    with pytest.raises(ValueError):
        digest({"bad": float("nan")})


async def test_journal_returns_cached_response_without_reprepare_or_send():
    task_id = uuid4()
    record = EffectRecord(
        "test",
        task_id,
        "report",
        digest({"input": 1}),
        {"markdown": "first"},
        {"artifact_id": str(uuid4())},
    )
    store = AsyncMock()
    store.get.return_value = record
    prepare, send = AsyncMock(), AsyncMock()
    result = await EffectJournal(store).run(
        task_id=task_id,
        key="test",
        kind="report",
        inputs={"input": 1},
        prepare=prepare,
        send=send,
    )
    assert result == record
    prepare.assert_not_called()
    send.assert_not_called()
    with pytest.raises(ValueError, match="different input"):
        await EffectJournal(store).run(
            task_id=task_id,
            key="test",
            kind="report",
            inputs={"input": 2},
            prepare=prepare,
            send=send,
        )


@pytest.mark.parametrize("kind", ["submit", "append", "finalize", "cancel"])
async def test_sender_validates_receipt_before_journaling(kind, tmp_path):
    expected, other = uuid4(), uuid4()
    response = {
        "execution_id": str(other),
        "operation": None,
        "state": {"status": "RUNNING", "version": 2},
    }
    client = AsyncMock()
    client.post_prepared.return_value = response
    sender = ExecutorEffectSender(
        Settings(_env_file=None, executor_shared_storage_root=tmp_path), client
    )
    with pytest.raises(ValueError, match="another Execution"):
        await sender.send(
            {
                "kind": kind,
                "path": "/executions",
                "body": {},
                "execution_id": str(expected),
            }
        )
    if kind in {"submit", "append"}:
        with pytest.raises(ValueError, match="omitted Operation"):
            await sender.send(
                {
                    "kind": kind,
                    "path": "/executions",
                    "body": {},
                }
            )


def test_inputs_are_content_addressed_and_checksum_checked(tmp_path):
    first = input_file("task", "# one", "md")
    second = input_file("task", "# two", "md")
    assert first["path"] != second["path"]
    restore_files(tmp_path, [first, second])
    assert (tmp_path / "requests" / first["path"]).read_text() == "# one"
    tampered = {**first, "content": "changed"}
    with pytest.raises(ValueError, match="checksum"):
        restore_files(tmp_path, [tampered])
    with pytest.raises(ValueError, match="escaped"):
        capture_files(
            tmp_path,
            [
                {
                    "payload": {
                        "source": {
                            "path": "../private.py",
                            "sha256": "0" * 64,
                        }
                    }
                }
            ],
        )


async def test_http_retries_keep_identical_wire_body():
    seen = []

    async def handle(request):
        seen.append(request.content)
        if len(seen) < 3:
            raise httpx.ReadTimeout("lost", request=request)
        return httpx.Response(200, json={"ok": True})

    async with httpx.AsyncClient(
        base_url="http://executor.test",
        transport=httpx.MockTransport(handle),
    ) as http:
        client = ExecutorClient(
            "http://executor.test", timeout_seconds=1, client=http
        )
        assert await client.post_prepared(
            "/executions", {"idempotency_key": "fixed", "x": 1}
        ) == {"ok": True}
    assert len(seen) == 3 and len(set(seen)) == 1


async def test_prepare_failure_never_sends_request():
    store = AsyncMock()
    store.get.return_value = None
    store.prepare.side_effect = RuntimeError("DB unavailable")
    send = AsyncMock()
    with pytest.raises(RuntimeError, match="DB unavailable"):
        await EffectJournal(store).run(
            task_id=uuid4(),
            key="test",
            kind="submit",
            inputs={},
            prepare=AsyncMock(return_value={"body": {}}),
            send=send,
        )
    send.assert_not_called()


async def test_cancellation_after_prepare_leaves_request_for_retry():
    task_id = uuid4()
    request = {"body": {"idempotency_key": "test"}}
    record = EffectRecord("test", task_id, "submit", digest({}), request, None)
    store = AsyncMock()
    store.get.return_value = None
    store.prepare.return_value = record
    send = AsyncMock(side_effect=asyncio.CancelledError())
    with pytest.raises(asyncio.CancelledError):
        await EffectJournal(store).run(
            task_id=task_id,
            key="test",
            kind="submit",
            inputs={},
            prepare=AsyncMock(return_value=deepcopy(request)),
            send=send,
        )
    store.complete.assert_not_called()
    send.assert_awaited_once_with(request)
