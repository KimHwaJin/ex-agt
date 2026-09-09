"""Identity ignores transport metadata, never substantive event changes."""

from copy import deepcopy
from uuid import uuid4

import pytest

from worker import ExecutorEvent


@pytest.fixture
def envelope():
    return {
        "event_id": str(uuid4()),
        "execution_id": str(uuid4()),
        "event_type": "execution.completed",
        "event_sequence": 10,
        "schema_version": "1.0",
        "occurred_at": "2026-09-06T10:47:18.990828+00:00",
        "payload": {"result": {"ok": True, "count": 1}},
    }


def rest_item(envelope):
    return {
        **envelope,
        "occurred_at": "2026-09-06T10:47:18.990828Z",
        "created_at": "2026-09-06T10:47:18.990828Z",
        "created_by": "agent",
        "created_by_type": "AGENT",
        "updated_at": "2026-09-06T10:47:18.990830Z",
        "updated_by": "worker",
        "updated_by_type": "AGENT",
        "delivery": {"status": "PUBLISHED", "attempt_count": 2},
    }


@pytest.mark.parametrize(
    "timestamp",
    [
        "2026-09-06T10:47:18.990828Z",
        "2026-09-06T19:47:18.990828+09:00",
    ],
)
def test_transport_metadata_and_equivalent_time(envelope, timestamp):
    original = ExecutorEvent.model_validate(envelope)
    response = rest_item(envelope)
    response["occurred_at"] = timestamp
    other = ExecutorEvent.model_validate(response)
    before = deepcopy(other.model_dump(mode="json"))
    assert original.identity_document() == other.identity_document()
    assert other.model_dump(mode="json") == before


def test_payload_key_order_is_not_identity(envelope):
    other = deepcopy(envelope)
    other["payload"] = {"result": {"count": 1, "ok": True}}
    assert ExecutorEvent.model_validate(envelope).identity_document() == (
        ExecutorEvent.model_validate(other).identity_document()
    )


@pytest.mark.parametrize(
    "timestamp",
    [
        "not-a-date",
        "2026-09-06T10:47:18.990828",
    ],
)
def test_invalid_or_ambiguous_timestamp_is_rejected(envelope, timestamp):
    envelope["occurred_at"] = timestamp
    with pytest.raises(ValueError):
        ExecutorEvent.model_validate(envelope).identity_document()


@pytest.mark.parametrize("rest_first", [False, True])
async def test_legacy_inbox_matches_without_rewrite(
    worker,
    envelope,
    rest_first,
):
    first, second = envelope, rest_item(envelope)
    if rest_first:
        first, second = second, first
    original = ExecutorEvent.model_validate(first)
    await worker.bindings.register(
        execution_id=original.execution_id,
        session_id="identity-session",
        task_id="identity-task",
    )
    # Simulate the previous version's unnormalized persisted JSON. The
    # current write contract intentionally remains the same.
    await worker.store.ingest(original)
    await worker.store.ingest(
        ExecutorEvent.model_validate(second),
        catch_up=True,
    )
    async with worker.pool.connection() as conn:
        cur = await conn.execute(
            "SELECT event FROM ew_inbox WHERE namespace=%s",
            (worker.settings.namespace,),
        )
        assert await cur.fetchall() == [(first,)]
        cur = await conn.execute(
            "SELECT catch_up_version FROM ew_bindings WHERE namespace=%s",
            (worker.settings.namespace,),
        )
        assert await cur.fetchone() == (1,)


@pytest.mark.parametrize(
    ("field", "replacement"),
    [
        ("event_id", "d9d3d7cd-7aaa-443b-8e99-83c6238ae7d7"),
        ("execution_id", "d9d3d7cd-7aaa-443b-8e99-83c6238ae7d7"),
        ("event_sequence", 11),
        ("event_type", "execution.operation_completed"),
        ("occurred_at", "2026-09-06T10:47:18.990829Z"),
        ("payload", {"result": {"ok": 1, "count": 1}}),
        ("payload", {"result": {"ok": True, "count": 2}}),
    ],
)
async def test_conflicts_remain_rejected(worker, envelope, field, replacement):
    await worker.store.ingest(ExecutorEvent.model_validate(envelope))
    changed = {**rest_item(envelope), field: replacement}
    with pytest.raises(ValueError, match="Conflicting"):
        await worker.store.ingest(ExecutorEvent.model_validate(changed))
    assert await worker.store.counts() == {"inbox:RECEIVED": 1}


@pytest.mark.parametrize("value", [1e20, 1e24, 1.2e20, 1e-20])
async def test_jsonb_numeric_roundtrip_does_not_change_identity(
    worker,
    envelope,
    value,
):
    envelope["payload"] = {"values": [value, True, None]}
    original = ExecutorEvent.model_validate(envelope)
    await worker.store.ingest(original)
    await worker.store.ingest(original)
    assert await worker.store.counts() == {"inbox:RECEIVED": 1}


def test_json_arrays_cannot_impersonate_tagged_numbers(envelope):
    number = {**envelope, "payload": {"value": 1}}
    array = {**envelope, "payload": {"value": [1]}}
    assert ExecutorEvent.model_validate(number).identity_document() != (
        ExecutorEvent.model_validate(array).identity_document()
    )
