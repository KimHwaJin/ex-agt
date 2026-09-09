from __future__ import annotations

import json
import logging
from uuid import uuid4

import pytest

from agent_worker import worker_main
from worker import ExecutorEvent, IgnoreEvent, RejectEvent
from worker.consumer import StreamMessage


@pytest.mark.parametrize("level", [None, "debug", "WARNING"])
def test_entrypoint_logging_preserves_host_handlers(monkeypatch, level):
    if level is None:
        monkeypatch.delenv("EW_LOG_LEVEL", raising=False)
    else:
        monkeypatch.setenv("EW_LOG_LEVEL", level)
    called = []

    async def main():
        called.append(True)

    monkeypatch.setattr(worker_main, "main", main)
    root_handlers = list(logging.getLogger().handlers)
    loggers = [logging.getLogger(n) for n in ("worker", "agent_worker")]
    old_levels = [logger.level for logger in loggers]
    try:
        worker_main.run_worker()
        assert called == [True]
        assert logging.getLogger().handlers == root_handlers
        for logger in loggers:
            assert logger.level == getattr(logging, (level or "INFO").upper())
    finally:
        for logger, old_level in zip(loggers, old_levels, strict=True):
            logger.setLevel(old_level)


def test_entrypoint_rejects_invalid_log_level(monkeypatch):
    monkeypatch.setenv("EW_LOG_LEVEL", "unknown")
    with pytest.raises(ValueError, match="EW_LOG_LEVEL"):
        worker_main.run_worker()


def test_entrypoint_failure_is_nonzero_without_secrets(monkeypatch, caplog):
    monkeypatch.setenv("EW_LOG_LEVEL", "INFO")

    async def main():
        raise ValueError("private-database-password")

    monkeypatch.setattr(worker_main, "main", main)
    loggers = [logging.getLogger(n) for n in ("worker", "agent_worker")]
    old_levels = [logger.level for logger in loggers]
    try:
        with pytest.raises(SystemExit) as error:
            worker_main.run_worker()
        assert error.value.code == 1
        assert "worker_failed error_type=ValueError" in caplog.text
        assert "private-database-password" not in caplog.text
        assert all(record.exc_info is None for record in caplog.records)
    finally:
        for logger, old_level in zip(loggers, old_levels, strict=True):
            logger.setLevel(old_level)


@pytest.mark.postgres
@pytest.mark.redis
@pytest.mark.parametrize("mode", ["success", "retry", "reject", "ignore"])
async def test_pipeline_logs_ids_without_payload_or_error_text(
    worker, caplog, mode
):
    caplog.set_level(logging.DEBUG, logger="worker")
    secret = "private-code-and-query-secret"
    event = ExecutorEvent(
        event_id=uuid4(),
        execution_id=uuid4(),
        event_type="execution.completed",
        event_sequence=1,
        schema_version="1.0",
        occurred_at="2026-09-07T00:00:00Z",
        payload={"code": secret, "query": secret},
    )
    await worker.bindings.register(
        execution_id=event.execution_id,
        session_id="logging-session",
        task_id="logging-task",
    )

    async def handler(context):
        assert context.event.payload["code"] == secret
        if mode == "retry":
            raise RuntimeError(secret)
        if mode == "reject":
            raise RejectEvent(secret)
        if mode == "ignore":
            raise IgnoreEvent(secret)

    worker.dispatcher.handlers[event.event_type] = handler
    ingress, dispatch = worker.consumers
    await ingress.ensure_group()
    await dispatch.ensure_group()
    fields = {
        key: str(value)
        for key, value in event.model_dump(mode="json").items()
        if key != "payload"
    }
    fields["payload"] = json.dumps(event.payload)
    source_id = await worker.redis.xadd(
        worker.settings.executor_event_stream, fields
    )
    await worker.redis.xreadgroup(
        worker.settings.event_group,
        "log-test",
        {worker.settings.executor_event_stream: ">"},
        count=1,
    )
    await ingress.process_message(
        "log-test", worker.ingress, StreamMessage(source_id, fields)
    )
    await worker.store.advance(event.execution_id, {event.event_type}, 100)
    assert await worker.outbox.once() == 1
    entries = await worker.redis.xreadgroup(
        worker.settings.command_group,
        "log-test",
        {worker.settings.command_stream: ">"},
        count=1,
    )
    message_id, envelope = entries[0][1][0]
    message = StreamMessage(message_id, envelope)
    await dispatch.process_message("log-test", worker.dispatcher, message)

    text = caplog.text
    assert "inbox_persisted" in text
    assert "outbox_published" in text
    assert "handler_started" in text
    assert "message_acked" in text
    assert str(event.event_id) in text
    assert str(event.execution_id) in text
    assert envelope["command_id"] in text
    assert secret not in text
    assert all(record.exc_info is None for record in caplog.records)
    row = await worker.store.command(envelope["command_id"])
    if mode == "success":
        assert "handler_completed" in text
        assert row["state"] == "DONE"
        await dispatch.process_message("log-test", worker.dispatcher, message)
        assert "command_duplicate" in caplog.text
        assert caplog.text.count("handler_started") == 1
    elif mode == "retry":
        assert "handler_failed" in text
        assert "terminal=False" in text
        assert "handler_completed" not in text
        assert row["state"] == "RUNNING"
    elif mode == "reject":
        assert "terminal=True" in text
        assert "message_dead_lettered" in text
        assert row["state"] == "FAILED"
    else:
        assert "handler_ignored" in text
        assert row["state"] == "IGNORED"


@pytest.mark.postgres
@pytest.mark.redis
async def test_readiness_logs_only_transitions(worker, caplog):
    caplog.set_level(logging.INFO, logger="worker")
    worker._log_readiness(False)
    worker._log_readiness(False)
    worker._log_readiness(True)
    worker._log_readiness(True)
    worker._log_readiness(False)
    assert caplog.messages == [
        "worker_readiness_changed ready=False",
        "worker_readiness_changed ready=True",
        "worker_readiness_changed ready=False",
    ]
