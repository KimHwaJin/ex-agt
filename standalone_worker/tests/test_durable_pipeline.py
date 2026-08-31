from __future__ import annotations

import asyncio
from typing import Any, cast
from uuid import uuid4

import httpx
import pytest
from psycopg import AsyncConnection

from executor_worker import (
    DeferEvent,
    ExecutorEvent,
    ExecutorWorker,
    IgnoreEvent,
    RejectEvent,
)
from executor_worker.consumer import (
    AckDecision,
    PermanentMessageError,
    StreamMessage,
)
from executor_worker.dispatcher import Dispatcher
from executor_worker.ingress import EventRouter

pytestmark = [pytest.mark.postgres, pytest.mark.redis]


def event(execution_id=None, sequence=1, kind="execution.completed"):
    return ExecutorEvent(
        event_id=uuid4(),
        execution_id=execution_id or uuid4(),
        event_sequence=sequence,
        event_type=kind,
        schema_version="1.0",
        payload={},
        occurred_at="2026-08-31T00:00:00Z",
    )


async def register(worker, e, session="session", task="task"):
    await worker.bindings.register(
        execution_id=e.execution_id,
        session_id=session,
        task_id=task,
    )


async def route(worker, e):
    await worker.store.ingest(e)
    return await worker.store.advance(e.execution_id, {e.event_type}, 100)


async def command_message(worker):
    await worker.outbox.once()
    entries = await worker.redis.xrevrange(
        worker.settings.command_stream, count=1
    )
    entry_id, fields = entries[0]
    return StreamMessage(entry_id, fields)


async def test_early_event_is_durable_before_binding(worker):
    e = event()
    fields = {
        key: str(value)
        for key, value in e.model_dump(mode="json").items()
        if key != "payload"
    }
    fields["payload"] = "{}"
    assert (
        await worker.ingress.handle(StreamMessage("1-0", fields))
    ).decision == AckDecision.ACK
    assert await worker.store.scan_candidates(100) == []
    assert (await worker.store.counts())["inbox:RECEIVED"] == 1
    await register(worker, e)
    assert await worker.store.advance(e.execution_id, {e.event_type}, 100)
    assert (await worker.store.counts())["outbox:PENDING"] == 1


async def test_binding_is_immutable_and_task_can_have_multiple_executions(
    worker,
):
    first, second = event(), event()
    await register(worker, first)
    await register(worker, first)
    await register(worker, second)
    with pytest.raises(ValueError, match="immutable"):
        await register(worker, first, session="another")


async def test_duplicate_and_conflicting_event_identity(worker):
    e = event()
    await register(worker, e)
    await route(worker, e)
    await route(worker, e)
    assert (await worker.store.counts())["command:READY"] == 1
    with pytest.raises(ValueError, match="Conflicting"):
        await worker.store.ingest(e.model_copy(update={"payload": {"x": 2}}))
    with pytest.raises(ValueError, match="Conflicting"):
        await worker.store.ingest(event(e.execution_id))


async def test_inbox_promotion_and_outbox_are_one_transaction(
    worker,
    monkeypatch,
):
    e = event()
    await register(worker, e)
    await worker.store.ingest(e)
    original = AsyncConnection.execute

    async def fail_outbox(self, query, *args, **kwargs):
        if "INSERT INTO ew_outbox" in str(query):
            raise RuntimeError("injected before commit")
        return await original(self, query, *args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(AsyncConnection, "execute", fail_outbox)
        with pytest.raises(RuntimeError):
            await worker.store.advance(e.execution_id, {e.event_type}, 100)
    assert await worker.store.counts() == {"inbox:RECEIVED": 1}
    assert (await worker.store.scan_candidates(1))[0]["last_sequence"] == 0
    await worker.store.advance(e.execution_id, {e.event_type}, 100)
    assert (await worker.store.counts())["outbox:PENDING"] == 1


async def test_outbox_publish_then_crash_duplicates_only_envelope(
    worker,
    monkeypatch,
):
    e = event()
    await register(worker, e)
    await route(worker, e)

    async def fail_finish(*args, **kwargs):
        raise RuntimeError("crash after XADD")

    with monkeypatch.context() as patch:
        patch.setattr(worker.store, "finish_publications", fail_finish)
        with pytest.raises(RuntimeError):
            await worker.outbox.once()
    async with worker.pool.connection() as conn:
        await conn.execute(
            "UPDATE ew_outbox SET claim_until=now()-interval '1 second' "
            "WHERE namespace=%s",
            (worker.settings.namespace,),
        )
    assert await worker.outbox.once() == 1
    entries = await worker.redis.xrange(worker.settings.command_stream)
    assert len(entries) == 2
    assert entries[0][1] == entries[1][1]


async def test_outbox_multiple_relays_claim_once(worker):
    for number in range(5):
        e = event()
        await register(worker, e, session=str(number))
        await route(worker, e)
    total = await asyncio.gather(*(worker.outbox.once() for _ in range(4)))
    assert sum(total) == 5
    assert await worker.redis.xlen(worker.settings.command_stream) == 5


async def test_outbox_does_not_overtake_unfinished_predecessor(worker):
    first = event(kind="execution.operation_completed")
    second = event(first.execution_id, 2)
    await register(worker, first)
    await route(worker, first)
    await route(worker, second)
    first_message = await command_message(worker)
    assert await worker.outbox.once() == 0

    async def handler(context):
        pass

    dispatcher = Dispatcher(
        worker.store, worker.guard, {first.event_type: handler}
    )
    await dispatcher.handle(first_message)
    assert await worker.outbox.once() == 1


async def test_db_done_prevents_duplicate_handler(worker):
    e = event()
    await register(worker, e)
    await route(worker, e)
    message = await command_message(worker)
    calls = []

    async def handler(context):
        calls.append(context)

    dispatcher = Dispatcher(
        worker.store, worker.guard, {e.event_type: handler}
    )
    await dispatcher.handle(message)
    await dispatcher.handle(message)
    assert len(calls) == 1
    assert calls[0].graph_config["configurable"]["thread_id"] == "session"


async def test_retry_owner_is_pending_not_outbox_and_failure_is_durable(
    worker,
):
    e = event()
    await register(worker, e)
    await route(worker, e)
    message = await command_message(worker)

    async def fail(context):
        raise RuntimeError("handler failed")

    dispatcher = Dispatcher(
        worker.store, worker.guard, {e.event_type: fail}, max_attempts=2
    )
    assert (await dispatcher.handle(message)).decision == AckDecision.DEFER
    assert await worker.outbox.once() == 0
    with pytest.raises(PermanentMessageError):
        await dispatcher.handle(message)
    row = await worker.store.command(message.fields["command_id"])
    assert row["state"] == "FAILED"
    assert row["failure_attempts"] == 2
    assert await worker.outbox.once() == 0


async def test_defer_and_lock_contention_do_not_consume_failure_budget(worker):
    e = event()
    await register(worker, e)
    await route(worker, e)
    message = await command_message(worker)

    async def wait(context):
        raise DeferEvent("Checkpoint not ready")

    dispatcher = Dispatcher(worker.store, worker.guard, {e.event_type: wait})
    async with worker.guard.hold("session"):
        assert (await dispatcher.handle(message)).decision == AckDecision.DEFER
    for _ in range(8):
        assert (await dispatcher.handle(message)).decision == AckDecision.DEFER
    row = await worker.store.command(message.fields["command_id"])
    assert row["failure_attempts"] == 0


async def test_failure_recovery_is_audited_and_old_delivery_cannot_run(worker):
    e = event()
    await register(worker, e)
    await route(worker, e)
    old = await command_message(worker)

    async def fail(context):
        raise RejectEvent("bad input")

    dispatcher = Dispatcher(worker.store, worker.guard, {e.event_type: fail})
    with pytest.raises(PermanentMessageError):
        await dispatcher.handle(old)
    row = await worker.store.command(old.fields["command_id"])
    async with worker.guard.hold("session"):
        await worker.store.resolve_failed(
            row["command_id"], retry=True, actor="tester", reason="fixed"
        )
    assert (await dispatcher.handle(old)).outcome == "old_generation"
    new = await command_message(worker)
    assert new.fields["command_id"] == old.fields["command_id"]
    assert new.fields["generation"] == "1"
    async with worker.pool.connection() as conn:
        cur = await conn.execute(
            "SELECT action,created_by FROM ew_audit WHERE namespace=%s",
            (worker.settings.namespace,),
        )
        assert await cur.fetchone() == ("RETRY", "tester")


async def test_unregistered_events_are_recorded_ignored(worker):
    e = event(kind="execution.started")
    await register(worker, e)
    await worker.store.ingest(e)
    await worker.store.advance(e.execution_id, set(), 100)
    assert await worker.store.counts() == {"inbox:IGNORED": 1}


async def test_reclaimed_old_event_catches_up_missing_tail(worker):
    first = event()
    second = event(first.execution_id, 2)
    await register(worker, first)
    await route(worker, first)
    await worker.store.ingest(first, catch_up=True)

    def history(request):
        assert request.url.params["after_sequence"] == "1"
        return httpx.Response(
            200,
            json={
                "items": [second.model_dump(mode="json")],
                "has_more": False,
            },
        )

    async with httpx.AsyncClient(
        base_url="http://executor",
        transport=httpx.MockTransport(history),
    ) as http:
        router = EventRouter(worker.store, http, {first.event_type})
        assert await router.once() == 1
    assert await worker.store.scan_candidates(100) == []
    assert (await worker.store.counts())["command:READY"] == 2


async def test_namespaces_do_not_share_inbox_bindings_or_commands(worker):
    from executor_worker.store import Store

    e = event()
    await register(worker, e)
    await route(worker, e)
    other = Store(worker.pool, f"other-{uuid4()}")
    await other.ingest(e)
    assert await other.scan_candidates(10) == []
    assert await other.counts() == {"inbox:RECEIVED": 1}


async def test_failed_predecessor_blocks_until_explicit_operator_skip(worker):
    first, second = event(), None
    second = event(first.execution_id, 2)
    await register(worker, first)
    await route(worker, first)
    await route(worker, second)
    message = await command_message(worker)

    async def fail(context):
        raise RejectEvent("permanent")

    dispatcher = Dispatcher(
        worker.store, worker.guard, {first.event_type: fail}
    )
    with pytest.raises(PermanentMessageError):
        await dispatcher.handle(message)
    assert await worker.outbox.once() == 0
    row = await worker.store.command(message.fields["command_id"])
    async with worker.guard.hold("session"):
        await worker.store.resolve_failed(
            row["command_id"], retry=False, actor="operator", reason="obsolete"
        )
    assert await worker.outbox.once() == 1


async def test_outbox_partial_redis_failure_only_releases_failed_claim(worker):
    from redis.exceptions import ResponseError

    from executor_worker.outbox import Outbox

    for _ in range(2):
        e = event()
        await register(worker, e)
        await route(worker, e)

    class Pipeline:
        def xadd(self, *args, **kwargs):
            pass

        async def execute(self, **kwargs):
            return ["1-0", ResponseError("injected")]

    class PartialRedis:
        def pipeline(self, **kwargs):
            return Pipeline()

    relay = Outbox(worker.store, cast(Any, PartialRedis()), "unused")
    assert await relay.once() == 1
    counts = await worker.store.counts()
    assert counts["outbox:SENT"] == counts["outbox:PENDING"] == 1


async def test_handlers_run_concurrently_across_sessions(worker):
    entered = []
    both = asyncio.Event()
    messages = []
    for index in range(2):
        e = event()
        await register(worker, e, session=str(index))
        await route(worker, e)
    await worker.outbox.once()
    for entry in await worker.redis.xrange(worker.settings.command_stream):
        messages.append(StreamMessage(*entry))

    async def handler(context):
        entered.append(context.session_id)
        if len(entered) == 2:
            both.set()
        await asyncio.wait_for(both.wait(), 2)

    dispatcher = Dispatcher(
        worker.store, worker.guard, {"execution.completed": handler}
    )
    results = await asyncio.gather(*(dispatcher.handle(m) for m in messages))
    assert all(r.decision == AckDecision.ACK for r in results)
    assert set(entered) == {"0", "1"}


async def test_explicit_ignore_is_terminal_without_dlq(worker):
    e = event()
    await register(worker, e)
    await route(worker, e)

    async def ignore(context):
        raise IgnoreEvent("Old task")

    dispatcher = Dispatcher(worker.store, worker.guard, {e.event_type: ignore})
    assert (
        await dispatcher.handle(await command_message(worker))
    ).decision == AckDecision.ACK
    assert (await worker.store.counts())["command:IGNORED"] == 1


async def test_reverse_events_recover_history_in_bounded_pages(worker):
    execution_id = uuid4()
    events = [event(execution_id, number) for number in range(1, 7)]
    await register(worker, events[-1])
    await worker.store.ingest(events[-1])
    calls = []

    def history(request):
        assert request.url.path.endswith(f"/executions/{execution_id}/events")
        after = int(request.url.params["after_sequence"])
        calls.append(after)
        return httpx.Response(
            200,
            json={
                "items": [
                    e.model_dump(mode="json")
                    for e in events[after : after + 2]
                ],
                "has_more": after + 2 < 6,
            },
        )

    async with httpx.AsyncClient(
        base_url="http://executor/api/v1",
        transport=httpx.MockTransport(history),
    ) as http:
        router = EventRouter(
            worker.store, http, {events[0].event_type}, batch_size=2
        )
        for _ in range(6):
            await router.once()
    assert calls == [0, 2, 4]
    assert (await worker.store.counts())["command:READY"] == 6


async def test_incomplete_history_does_not_advance_or_drop_event(worker):
    e = event(sequence=3)
    await register(worker, e)
    await worker.store.ingest(e)
    async with httpx.AsyncClient(
        base_url="http://executor",
        transport=httpx.MockTransport(
            lambda _: httpx.Response(200, json={"items": []})
        ),
    ) as http:
        router = EventRouter(worker.store, http, {e.event_type})
        assert await router.once() == 0
    assert await worker.store.counts() == {"inbox:RECEIVED": 1}
    async with worker.pool.connection() as conn:
        cur = await conn.execute(
            "SELECT last_sequence,last_error "
            "FROM ew_bindings WHERE namespace=%s",
            (worker.settings.namespace,),
        )
        sequence, error = await cur.fetchone()
        assert sequence == 0
        assert "gap" in error


async def test_real_runtime_restores_pending_after_process_replacement(worker):
    e = event()
    await register(worker, e)
    started, cancelled, completed = (asyncio.Event() for _ in range(3))

    async def block(context):
        started.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancelled.set()
            raise

    settings = worker.settings
    async with ExecutorWorker(settings, {e.event_type: block}) as first:
        run = asyncio.create_task(first.run())
        fields = {
            key: str(value)
            for key, value in e.model_dump(mode="json").items()
            if key != "payload"
        }
        fields["payload"] = "{}"
        await worker.redis.xadd(settings.executor_event_stream, fields)
        await asyncio.wait_for(started.wait(), 8)
        first.request_stop()
        await asyncio.wait_for(run, 5)
        assert cancelled.is_set()
        assert await worker.redis.get(worker.guard.key("session")) is None

    async def finish(context):
        completed.set()

    replacement = settings.model_copy(update={"instance_id": str(uuid4())})
    async with ExecutorWorker(replacement, {e.event_type: finish}) as second:
        run = asyncio.create_task(second.run())
        try:
            await asyncio.wait_for(completed.wait(), 8)
            for _ in range(100):
                if (await worker.store.counts()).get("command:DONE") == 1:
                    break
                await asyncio.sleep(0.02)
            assert (await worker.store.counts())["command:DONE"] == 1
        finally:
            second.request_stop()
            await asyncio.wait_for(run, 5)
