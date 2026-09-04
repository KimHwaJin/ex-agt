"""Bounded Streams operations shared by Redis 6.0.8 and newer servers."""

from __future__ import annotations

from collections.abc import Awaitable
from dataclasses import dataclass
from typing import Any, cast

from redis.asyncio import Redis

# One bounded, atomic page: do not use XPENDING IDLE or exclusive ranges.
# Lua never converts Stream IDs to numbers (IDs contain uint64 components).
_CLAIM_PAGE_SCRIPT = """
local pending = redis.call(
    'XPENDING', KEYS[1], ARGV[1], ARGV[4], '+', ARGV[5]
)
local messages = {}
local last = '0-0'
for _, entry in ipairs(pending) do
    last = entry[1]
    if entry[3] >= tonumber(ARGV[3]) then
        local claimed = redis.call(
            'XCLAIM', KEYS[1], ARGV[1], ARGV[2], ARGV[3], entry[1]
        )
        -- Redis 6.0 can return {false} for a deleted Stream body.
        if #claimed > 0 and claimed[1] then
            table.insert(messages, claimed[1])
        elseif #redis.call('XRANGE', KEYS[1], entry[1], entry[1]) == 0 then
            -- Redis 6.0 does not remove deleted entries from the PEL.
            -- Only ACK a missing body, never a live but unclaimable entry.
            redis.call('XACK', KEYS[1], ARGV[1], entry[1])
        end
    end
end
return {last, #pending, messages}
"""

_MAX_ID_PART = (1 << 64) - 1


def next_stream_id(value: str) -> str | None:
    """Inclusive successor; None means the Stream ID space is exhausted."""
    milliseconds, sequence = (int(part) for part in value.split("-"))
    if not all(0 <= part <= _MAX_ID_PART for part in (milliseconds, sequence)):
        raise ValueError("Stream ID components must be uint64")
    if sequence < _MAX_ID_PART:
        return f"{milliseconds}-{sequence + 1}"
    if milliseconds < _MAX_ID_PART:
        return f"{milliseconds + 1}-0"
    return None


async def claim_pending_page(
    redis: Redis,
    stream: str,
    group: str,
    consumer: str,
    *,
    min_idle_milliseconds: int,
    cursor: str,
    count: int,
) -> tuple[str, list[tuple[str, dict[str, str]]]]:
    if not 1 <= count <= 500:
        raise ValueError("Claim page size must be between 1 and 500")
    if min_idle_milliseconds < 1:
        raise ValueError("Claim idle time must be positive")
    operation = redis.eval(
        _CLAIM_PAGE_SCRIPT,
        1,
        stream,
        group,
        consumer,
        str(min_idle_milliseconds),
        cursor,
        str(count),
    )
    response: Any = await cast(Awaitable[Any], operation)
    last, scanned, entries = response
    next_cursor = (
        next_stream_id(_text(last)) if int(scanned) == count else None
    )
    messages = [
        (
            _text(message_id),
            {
                _text(key): _text(value)
                for key, value in zip(fields[::2], fields[1::2], strict=True)
            },
        )
        for message_id, fields in entries
    ]
    return next_cursor or "0-0", messages


@dataclass(frozen=True)
class GroupProgress:
    pending: int
    lag: int | None
    has_unread: bool


async def group_progress(
    redis: Redis, stream: str, group: str
) -> GroupProgress:
    groups: Any = await redis.xinfo_groups(stream)
    entry = next((row for row in groups if _text(row["name"]) == group), None)
    if entry is None:
        raise ValueError(f"Consumer group is missing: {stream}/{group}")
    lag = entry.get("lag")
    if lag is not None:
        return GroupProgress(int(entry["pending"]), int(lag), int(lag) > 0)
    # Unknown is not zero. Only test existence; do not scan/count a backlog.
    after = next_stream_id(_text(entry["last-delivered-id"]))
    unread = (
        await redis.xrange(stream, min=after, max="+", count=1)
        if after is not None
        else []
    )
    return GroupProgress(int(entry["pending"]), None, bool(unread))


def _text(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)
