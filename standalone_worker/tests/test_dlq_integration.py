from __future__ import annotations

import json
import os
from typing import Any
from uuid import uuid4

import pytest
from redis.asyncio import Redis

from executor_worker.dlq import (
    DeadLetterAction,
    DeadLetterManager,
)

pytestmark = pytest.mark.skipif(
    "TEST_REDIS_URL" not in os.environ,
    reason="Compose Redis is not configured",
)


def _envelope(source_stream: str, source_message_id: str) -> dict[str, str]:
    return {
        "schema_version": "1",
        "failure_id": str(uuid4()),
        "dead_lettered_at": "2026-08-30T00:00:00+00:00",
        "source_stream": source_stream,
        "source_group": "integration-workers",
        "source_message_id": source_message_id,
        "consumer": "integration-worker-0",
        "error_type": "RuntimeError",
        "reason": "dependency unavailable",
        "retry_attempts": "3",
        "reclaimed": "true",
        "fields": json.dumps(
            {"job_id": "job-one", "idempotency_key": "stable-one"}
        ),
    }


@pytest.mark.redis
@pytest.mark.asyncio
async def test_real_redis_replay_is_atomic_audited_and_idempotent() -> None:
    suffix = uuid4()
    source_stream = f"test-dlq-source-{suffix}"
    dlq_stream = f"test-dlq-{suffix}"
    audit_stream = f"test-dlq-audit-{suffix}"
    redis = Redis.from_url(
        os.environ["TEST_REDIS_URL"],
        decode_responses=True,
    )
    manager = DeadLetterManager(
        redis,
        dlq_stream,
        audit_stream=audit_stream,
    )
    try:
        dlq_id = await redis.xadd(
            dlq_stream,
            _envelope(source_stream, "1-0"),
        )

        first = await manager.replay(
            dlq_id,
            actor="integration-test",
            reason="dependency recovered",
        )
        second = await manager.replay(
            dlq_id,
            actor="integration-test",
            reason="duplicate request",
        )

        source_rows: Any = await redis.xrange(source_stream)
        dlq_rows: Any = await redis.xrange(dlq_stream)
        audit_rows: Any = await redis.xrange(audit_stream)
        assert first.action is DeadLetterAction.REPLAYED
        assert first.applied is True
        assert second.applied is False
        assert second.target_message_id == first.target_message_id
        assert len(source_rows) == 1
        assert source_rows[0][1] == {
            "job_id": "job-one",
            "idempotency_key": "stable-one",
        }
        assert dlq_rows == []
        assert len(audit_rows) == 1
        assert audit_rows[0][1]["action"] == "REPLAYED"
        assert audit_rows[0][1]["target_message_id"] == (
            first.target_message_id
        )
    finally:
        await redis.delete(source_stream, dlq_stream, audit_stream)
        await redis.delete(manager._marker_key(dlq_id))
        await redis.aclose()


@pytest.mark.redis
@pytest.mark.asyncio
async def test_real_redis_discard_is_atomic_audited_and_idempotent() -> None:
    suffix = uuid4()
    source_stream = f"test-discard-source-{suffix}"
    dlq_stream = f"test-discard-dlq-{suffix}"
    audit_stream = f"test-discard-audit-{suffix}"
    redis = Redis.from_url(
        os.environ["TEST_REDIS_URL"],
        decode_responses=True,
    )
    manager = DeadLetterManager(
        redis,
        dlq_stream,
        audit_stream=audit_stream,
    )
    try:
        dlq_id = await redis.xadd(
            dlq_stream,
            _envelope(source_stream, "2-0"),
        )

        first = await manager.discard(
            dlq_id,
            actor="integration-test",
            reason="invalid business request",
        )
        second = await manager.discard(
            dlq_id,
            actor="integration-test",
            reason="duplicate request",
        )

        assert first.action is DeadLetterAction.DISCARDED
        assert first.applied is True
        assert second.applied is False
        assert await redis.xlen(source_stream) == 0
        assert await redis.xlen(dlq_stream) == 0
        audit_rows: Any = await redis.xrange(audit_stream)
        assert len(audit_rows) == 1
        assert audit_rows[0][1]["action"] == "DISCARDED"
        assert audit_rows[0][1]["reason"] == "invalid business request"
    finally:
        await redis.delete(source_stream, dlq_stream, audit_stream)
        await redis.delete(manager._marker_key(dlq_id))
        await redis.aclose()
