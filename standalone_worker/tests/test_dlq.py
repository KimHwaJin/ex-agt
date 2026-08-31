from __future__ import annotations

import json
from typing import Any, cast

import pytest

from executor_worker.dlq import (
    DeadLetterAction,
    DeadLetterEntry,
    DeadLetterFormatError,
    DeadLetterManager,
)


def _values(message_id: str) -> dict[str, str]:
    return {
        "schema_version": "1",
        "failure_id": f"failure-{message_id}",
        "source_stream": "jobs",
        "source_group": "workers",
        "source_message_id": message_id,
        "reason": "dependency unavailable",
        "retry_attempts": "3",
        "fields": json.dumps({"job_id": message_id}),
        "dead_lettered_at": "2026-08-30T00:00:00+00:00",
        "consumer": "worker-a-0",
        "error_type": "RuntimeError",
        "reclaimed": "true",
    }


class FakeRedis:
    def __init__(self) -> None:
        self.rows = [
            ("1-0", _values("source-1")),
            ("2-0", _values("source-2")),
        ]
        self.markers: dict[str, str] = {}
        self.eval_calls: list[tuple[Any, ...]] = []

    async def xrange(
        self,
        stream: str,
        *,
        min: str,
        max: str,
        count: int,
    ) -> list[tuple[str, dict[str, str]]]:
        del stream, max
        if min.startswith("("):
            rows = [row for row in self.rows if row[0] > min[1:]]
        elif min != "-":
            rows = [row for row in self.rows if row[0] == min]
        else:
            rows = self.rows
        return rows[:count]

    async def get(self, key: str) -> str | None:
        return self.markers.get(key)

    async def eval(self, *args: Any) -> list[Any]:
        self.eval_calls.append(args)
        return [
            1,
            json.dumps(
                {
                    "action": "REPLAYED",
                    "target_message_id": "9-0",
                }
            ),
        ]


def test_dead_letter_entry_accepts_legacy_envelope() -> None:
    entry = DeadLetterEntry.from_redis(
        "3-0",
        {
            "source_stream": "jobs",
            "source_group": "workers",
            "source_message_id": "1-0",
            "reason": "invalid",
            "fields": '{"job_id": "one"}',
        },
    )

    assert entry.schema_version == "0"
    assert entry.retry_attempts == 0
    assert entry.fields == {"job_id": "one"}
    assert len(entry.failure_id) == 64


def test_dead_letter_entry_rejects_invalid_fields() -> None:
    with pytest.raises(DeadLetterFormatError, match="Invalid DLQ entry"):
        DeadLetterEntry.from_redis(
            "3-0",
            {
                "source_stream": "jobs",
                "source_group": "workers",
                "source_message_id": "1-0",
                "reason": "invalid",
                "fields": "[]",
            },
        )


@pytest.mark.asyncio
async def test_list_uses_exclusive_cursor_and_reports_next_page() -> None:
    redis = FakeRedis()
    manager = DeadLetterManager(cast(Any, redis), "jobs.dlq")

    first = await manager.list_entries(limit=1)
    second = await manager.list_entries(limit=1, after=first.next_cursor)

    assert [entry.entry_id for entry in first.entries] == ["1-0"]
    assert first.next_cursor == "1-0"
    assert [entry.entry_id for entry in second.entries] == ["2-0"]
    assert second.next_cursor is None


@pytest.mark.asyncio
async def test_replay_returns_existing_marker_without_republishing() -> None:
    redis = FakeRedis()
    manager = DeadLetterManager(cast(Any, redis), "jobs.dlq")
    marker_key = manager._marker_key("1-0")
    redis.markers[marker_key] = json.dumps(
        {"action": "REPLAYED", "target_message_id": "8-0"}
    )

    result = await manager.replay(
        "1-0",
        actor="operator@example.com",
        reason="dependency recovered",
    )

    assert result.action is DeadLetterAction.REPLAYED
    assert result.applied is False
    assert result.target_message_id == "8-0"
    assert redis.eval_calls == []


@pytest.mark.asyncio
async def test_replay_passes_original_fields_to_atomic_script() -> None:
    redis = FakeRedis()
    manager = DeadLetterManager(cast(Any, redis), "jobs.dlq")

    result = await manager.replay(
        "1-0",
        actor="operator@example.com",
        reason="dependency recovered",
    )

    assert result.applied is True
    assert result.target_message_id == "9-0"
    call = redis.eval_calls[0]
    assert call[2:6] == (
        "jobs.dlq",
        "jobs",
        manager._marker_key("1-0"),
        "jobs.dlq.audit",
    )
    assert json.loads(call[7]) == {"job_id": "source-1"}


@pytest.mark.asyncio
async def test_action_requires_actor_and_reason() -> None:
    manager = DeadLetterManager(cast(Any, FakeRedis()), "jobs.dlq")

    with pytest.raises(ValueError, match="actor"):
        await manager.discard("1-0", actor="", reason="invalid payload")
    with pytest.raises(ValueError, match="reason"):
        await manager.replay("1-0", actor="operator", reason="")
