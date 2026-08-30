from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, cast

import pytest

from ex_agent.transport.stream_maintenance import SafeStreamTrimmer


class FakeRedis:
    def __init__(
        self,
        *,
        exists: bool = True,
        length: int = 6,
        groups: list[dict[str, Any]] | None = None,
        pending: dict[str, dict[str, Any]] | None = None,
    ) -> None:
        self.stream_exists = exists
        self.length = length
        self.groups = groups or []
        self.pending = pending or {}
        self.rows = [
            ("6000-0", {"value": "six"}),
            ("5000-0", {"value": "five"}),
            ("4000-0", {"value": "four"}),
        ]
        self.eval_calls: list[tuple[Any, ...]] = []

    async def exists(self, stream: str) -> int:
        del stream
        return int(self.stream_exists)

    async def xlen(self, stream: str) -> int:
        del stream
        return self.length

    async def xinfo_groups(self, stream: str) -> list[dict[str, Any]]:
        del stream
        return self.groups

    async def xpending(self, stream: str, group: str) -> dict[str, Any]:
        del stream
        return self.pending[group]

    async def xrevrange(
        self,
        stream: str,
        *,
        max: str,
        min: str,
        count: int,
    ) -> list[tuple[str, dict[str, str]]]:
        del stream, max, min
        return self.rows[:count]

    async def eval(self, *args: Any) -> list[Any]:
        self.eval_calls.append(args)
        return [3, b"4000-0", 2, 6]


def _now() -> datetime:
    return datetime.fromtimestamp(10, tz=UTC)


@pytest.mark.asyncio
async def test_plan_uses_oldest_constraint_across_groups_and_pending() -> None:
    redis = FakeRedis(
        groups=[
            {
                "name": "fast",
                "last-delivered-id": "6000-0",
                "pending": 0,
            },
            {
                "name": "slow",
                "last-delivered-id": "5000-0",
                "pending": 1,
            },
        ],
        pending={
            "slow": {
                "pending": 1,
                "min": "4000-0",
                "max": "4000-0",
                "consumers": [],
            }
        },
    )
    trimmer = SafeStreamTrimmer(
        cast(Any, redis),
        retention_seconds=3,
        minimum_retained_entries=2,
    )

    plan = await trimmer.plan("jobs", now=_now())

    assert plan.retention_boundary_id == "7000-0"
    assert plan.tail_boundary_id == "5000-0"
    assert plan.trim_before_id == "4000-0"
    assert plan.can_trim is True
    assert plan.groups[1].oldest_pending_id == "4000-0"


@pytest.mark.asyncio
async def test_plan_preserves_history_for_group_at_beginning() -> None:
    redis = FakeRedis(
        groups=[
            {
                "name": "new-reader",
                "last-delivered-id": "0-0",
                "pending": 0,
            }
        ]
    )
    trimmer = SafeStreamTrimmer(
        cast(Any, redis),
        retention_seconds=3,
        minimum_retained_entries=1,
    )

    plan = await trimmer.plan("jobs", now=_now())

    assert plan.trim_before_id == "0-0"
    assert plan.can_trim is False


@pytest.mark.asyncio
async def test_plan_does_not_trim_stream_below_minimum_tail() -> None:
    redis = FakeRedis(length=2)
    trimmer = SafeStreamTrimmer(
        cast(Any, redis),
        retention_seconds=3,
        minimum_retained_entries=2,
    )

    plan = await trimmer.plan("jobs", now=_now())

    assert plan.stream_exists is True
    assert plan.trim_before_id is None
    assert plan.can_trim is False


@pytest.mark.asyncio
async def test_plan_reports_missing_stream_without_error() -> None:
    redis = FakeRedis(exists=False, length=0)
    trimmer = SafeStreamTrimmer(cast(Any, redis))

    plan = await trimmer.plan("missing", now=_now())

    assert plan.stream_exists is False
    assert plan.stream_length == 0
    assert plan.trim_before_id is None


@pytest.mark.asyncio
async def test_trim_uses_atomic_script_and_decodes_response() -> None:
    redis = FakeRedis()
    trimmer = SafeStreamTrimmer(
        cast(Any, redis),
        retention_seconds=3,
        minimum_retained_entries=2,
    )

    result = await trimmer.trim("jobs", now=_now())

    assert result.removed_entries == 3
    assert result.trim_before_id == "4000-0"
    assert result.inspected_groups == 2
    assert redis.eval_calls[0][1:] == (1, "jobs", "7000-0", "2")


def test_trimmer_rejects_invalid_configuration() -> None:
    with pytest.raises(ValueError, match="retention_seconds"):
        SafeStreamTrimmer(cast(Any, FakeRedis()), retention_seconds=0)
    with pytest.raises(ValueError, match="minimum_retained_entries"):
        SafeStreamTrimmer(
            cast(Any, FakeRedis()),
            minimum_retained_entries=-1,
        )


@pytest.mark.asyncio
async def test_plan_rejects_naive_datetime_and_empty_stream() -> None:
    trimmer = SafeStreamTrimmer(cast(Any, FakeRedis()))

    with pytest.raises(ValueError, match="timezone-aware"):
        await trimmer.plan("jobs", now=datetime(2026, 1, 1))
    with pytest.raises(ValueError, match="stream cannot be empty"):
        await trimmer.plan(" ", now=_now())
