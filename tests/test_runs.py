"""Real DB tests of the public run API, durable demo and output projection."""

import asyncio
import json
from uuid import UUID, uuid4

import pytest
from pydantic import ValidationError

from agent_service.application.outputs import OutputWriter
from agent_service.domain.management import DomainError
from agent_service.domain.runs import RunRequest
from agent_service.integrations.template_protocol import decode_v1
from agent_service.integrations.template_workflow import WorkflowManager
from agent_service.runtime.demo_driver import DemoDriver


@pytest.fixture
def settings(settings):
    return settings.model_copy(
        update={
            "agent_backend": "demo",
            "embedded_run_worker": False,
            "demo_step_seconds": 0.05,
            "stream_poll_seconds": 0.05,
            "stream_max_seconds": 1,
        }
    )


@pytest.fixture
def runs(client):
    return client._transport.app.state.management_runtime.resources.runs


@pytest.fixture
async def session(client, home, identity):
    response = await client.post(
        "/api/v1/sessions",
        headers={
            **identity,
            "Idempotency-Key": str(uuid4()),
        },
        json={"project_id": home["default_project"]["project_id"]},
    )
    assert response.status_code == 201
    return response.json()


def payload(identity, session, **extra):
    return {
        "user_id": identity["X-User-UUID"],
        "session_id": session["session_id"],
        "input": {
            "type": "message",
            "content": [
                {"type": "text", "text": "테스트 입력"},
            ],
        },
        **extra,
    }


async def submit(client, identity, session, **extra):
    response = await client.post(
        "/api/v1/agent/runs",
        headers={
            **identity,
            "Idempotency-Key": str(uuid4()),
        },
        json=payload(identity, session, **extra),
    )
    assert response.status_code == 202, response.text
    return response.json()


async def advance(runs, db, receipt, identity, steps=1):
    # Deterministic clock advancement, no sleeps in transition tests.
    driver = DemoDriver(runs)
    for _ in range(steps):
        await db.execute(
            "UPDATE management.runs SET due_at = now() WHERE run_id = %s",
            (receipt["run_id"],),
        )
        await driver.tick(
            UUID(receipt["run_id"]), UUID(identity["X-User-UUID"])
        )
    return await runs.detail(
        UUID(identity["X-User-UUID"]), UUID(receipt["run_id"])
    )


def resume(identity, session, detail, decision="approve"):
    response = {"type": "plan_review", "decision": decision}
    if decision == "modify":
        response["instruction"] = "시각화를 빼 주세요"
    return payload(
        identity,
        session,
        input={
            "type": "resume",
            "run_id": str(detail.run_id),
            "interrupt_id": str(detail.pending_interrupts[0].interrupt_id),
            "response": response,
        },
    )


@pytest.mark.parametrize(
    "input_value",
    [
        {"type": "message", "content": []},
        {"type": "message", "content": [{"type": "text", "text": " "}]},
        {"type": "message", "content": [{"type": "image", "file_id": "x"}]},
        {
            "type": "message",
            "content": [{"type": "text", "text": "hi"}],
            "run_id": str(uuid4()),
        },
        {
            "type": "resume",
            "run_id": str(uuid4()),
            "interrupt_id": str(uuid4()),
            "response": {"type": "plan_review", "decision": "modify"},
        },
        {
            "type": "resume",
            "run_id": str(uuid4()),
            "interrupt_id": str(uuid4()),
            "response": {
                "type": "plan_review",
                "decision": "approve",
                "goto": "end",
            },
        },
    ],
)
def test_input_discriminator_and_bounds(input_value):
    with pytest.raises(ValidationError):
        RunRequest.model_validate(
            {
                "user_id": str(uuid4()),
                "session_id": str(uuid4()),
                "input": input_value,
            }
        )


@pytest.mark.postgres
async def test_admission_is_atomic_and_idempotent(
    client, identity, session, runs
):
    body = payload(identity, session)
    headers = {**identity, "Idempotency-Key": str(uuid4())}
    responses = await asyncio.gather(
        *[
            client.post("/api/v1/agent/runs", json=body, headers=headers)
            for _ in range(6)
        ]
    )
    assert all(item.status_code == 202 for item in responses)
    receipt = responses[0].json()
    assert all(item.json() == receipt for item in responses)
    messages = await client.get(
        f"/api/v1/sessions/{session['session_id']}/messages", headers=identity
    )
    assert len(messages.json()["items"]) == 1
    listed = await client.get(
        f"/api/v1/sessions/{session['session_id']}/runs", headers=identity
    )
    assert len(listed.json()["items"]) == 1
    detail = await runs.detail(
        UUID(identity["X-User-UUID"]), UUID(receipt["run_id"])
    )
    assert detail.project_id == UUID(session["project_id"])
    assert detail.backend == "demo"
    assert detail.pending_interrupts == detail.executions == []
    session_read = await client.get(
        f"/api/v1/sessions/{session['session_id']}", headers=identity
    )
    assert session_read.json()["active_run_id"] == receipt["run_id"]
    assert session_read.json()["is_locked"] is False
    assert (
        await client.post(
            "/api/v1/agent/runs",
            headers=headers,
            json={**body, "session_id": str(uuid4())},
        )
    ).status_code == 409
    competing = await client.post(
        "/api/v1/agent/runs",
        json=body,
        headers={
            **identity,
            "Idempotency-Key": str(uuid4()),
        },
    )
    assert competing.status_code == 409
    assert (
        await client.delete(
            f"/api/v1/sessions/{session['session_id']}", headers=identity
        )
    ).status_code == 409


@pytest.mark.postgres
async def test_hitl_revision_stale_response_and_cancel(
    client,
    identity,
    session,
    runs,
    db,
):
    receipt = await submit(client, identity, session)
    detail = await advance(runs, db, receipt, identity, 3)
    assert detail.status == "awaiting_input"
    body = resume(identity, session, detail, "modify")
    response = await client.post(
        "/api/v1/agent/runs",
        json=body,
        headers={
            **identity,
            "Idempotency-Key": str(uuid4()),
        },
    )
    assert response.status_code == 202
    revised = await advance(runs, db, receipt, identity)
    assert revised.pending_interrupts[0].payload["plan_version"] == 2
    assert revised.pending_interrupts[0].interrupt_id != (
        detail.pending_interrupts[0].interrupt_id
    )
    stale = await client.post(
        "/api/v1/agent/runs",
        json=body,
        headers={
            **identity,
            "Idempotency-Key": str(uuid4()),
        },
    )
    assert stale.status_code == 409
    assert stale.json()["code"] == "INTERRUPT_MISMATCH"
    response = await client.post(
        "/api/v1/agent/runs",
        json=resume(identity, session, revised),
        headers={
            **identity,
            "Idempotency-Key": str(uuid4()),
        },
    )
    assert response.status_code == 202
    detail = await advance(runs, db, receipt, identity)
    assert detail.executions[0].execution_id is None
    locked = await client.get(
        f"/api/v1/sessions/{session['session_id']}", headers=identity
    )
    assert locked.json()["is_locked"] is True
    blocked = await client.post(
        "/api/v1/agent/runs",
        json=payload(identity, session),
        headers={
            **identity,
            "Idempotency-Key": str(uuid4()),
        },
    )
    assert blocked.json()["code"] == "SESSION_LOCKED"
    path = f"/api/v1/agent/runs/{receipt['run_id']}/cancel"
    assert (await client.post(path, headers=identity)).status_code == 202
    assert (
        await client.get(
            f"/api/v1/sessions/{session['session_id']}", headers=identity
        )
    ).json()["is_locked"] is True
    detail = await advance(runs, db, receipt, identity)
    assert detail.status == "cancelled"
    assert detail.executions[0].status == "cancelled"
    assert (await client.post(path, headers=identity)).status_code == 200
    unlocked = (
        await client.get(
            f"/api/v1/sessions/{session['session_id']}", headers=identity
        )
    ).json()
    assert unlocked["active_run_id"] is None
    assert unlocked["is_locked"] is False
    assert (
        len(
            (
                await client.get(
                    f"/api/v1/sessions/{session['session_id']}/messages",
                    headers=identity,
                )
            ).json()["items"]
        )
        == 2
    )  # No report on cancellation.


@pytest.mark.postgres
async def test_success_recovery_and_replay(
    client, identity, session, runs, db
):
    receipt = await submit(client, identity, session)
    detail = await advance(runs, db, receipt, identity, 3)
    body = resume(identity, session, detail)
    headers = {**identity, "Idempotency-Key": str(uuid4())}
    assert (
        await client.post("/api/v1/agent/runs", headers=headers, json=body)
    ).status_code == 202
    # A fresh driver instance on every step simulates process-local state loss.
    for _ in range(3):
        detail = await advance(runs, db, receipt, identity)
    assert detail.status == "running"
    assert detail.executions[0].status == "completed"
    # Executor completion does not release the lock before report completion.
    assert (
        await client.get(
            f"/api/v1/sessions/{session['session_id']}", headers=identity
        )
    ).json()["is_locked"] is True
    detail = await advance(runs, db, receipt, identity)
    assert detail.status == "completed"
    assert detail.executions[0].simulated is True
    assert (
        await client.post("/api/v1/agent/runs", headers=headers, json=body)
    ).status_code == 202  # Same operation, no second resume.
    assert (
        await client.post(
            f"/api/v1/agent/runs/{receipt['run_id']}/cancel", headers=identity
        )
    ).status_code == 409
    path = f"/api/v1/agent/runs/{receipt['run_id']}/stream"
    stream = await client.get(path, headers=identity)
    assert stream.status_code == 200
    assert "run.completed" in stream.text
    ids = [
        line[4:]
        for line in stream.text.splitlines()
        if line.startswith("id: ")
    ]
    assert len(ids) == len(set(ids))
    later = await client.get(
        path, headers={**identity, "Last-Event-ID": ids[2]}
    )
    assert f"id: {ids[2]}\n" not in later.text
    assert f"id: {ids[3]}\n" in later.text
    assert (
        await client.get(
            path,
            headers={
                **identity,
                "Last-Event-ID": f"{uuid4()}:1",
            },
        )
    ).status_code == 422
    assert (
        await client.get(
            path,
            headers={
                **identity,
                "Last-Event-ID": f"{receipt['run_id']}:999999",
            },
        )
    ).status_code == 422


@pytest.mark.postgres
async def test_queued_cancellation_and_ownership(
    client, identity, session, home
):
    receipt = await submit(client, identity, session)
    other = await client.post(
        "/api/v1/me", headers={"X-User-Id": str(uuid4())}
    )
    stranger = {"X-User-UUID": other.json()["user"]["user_uuid"]}
    base = f"/api/v1/agent/runs/{receipt['run_id']}"
    for suffix in ["", "/stream"]:
        assert (
            await client.get(base + suffix, headers=stranger)
        ).status_code == 404
    assert (
        await client.post(base + "/cancel", headers=stranger)
    ).status_code == 404
    assert (
        await client.get(
            f"/api/v1/sessions/{session['session_id']}/messages",
            headers=stranger,
        )
    ).status_code == 404
    mismatch = await client.post(
        "/api/v1/agent/runs",
        headers={
            **identity,
            "Idempotency-Key": str(uuid4()),
        },
        json=payload(identity, session, user_id=str(uuid4())),
    )
    assert mismatch.status_code == 403
    assert (await client.post(base + "/cancel", headers=identity)).json()[
        "status"
    ] == "cancelled"
    new = await submit(client, identity, session)
    assert new["run_id"] != receipt["run_id"]


@pytest.mark.postgres
@pytest.mark.parametrize(
    "scenario,expected", [("reply", "completed"), ("failure", "failed")]
)
async def test_partial_and_normal_outputs(
    client,
    identity,
    session,
    runs,
    db,
    scenario,
    expected,
):
    runs.settings.demo_scenario = scenario
    receipt = await submit(client, identity, session)
    detail = await advance(runs, db, receipt, identity, 3)
    assert detail.status == expected
    messages = (
        await client.get(
            f"/api/v1/sessions/{session['session_id']}/messages",
            headers=identity,
        )
    ).json()["items"]
    assert len(messages) == 2
    assert messages[0]["role"] == "assistant"
    assert messages[0]["status"] == (
        "failed" if expected == "failed" else "completed"
    )
    assert messages[0]["content"][0]["text"].startswith("[DEMO]")


@pytest.mark.postgres
async def test_post_stream_does_not_drive_execution(
    client, identity, session, runs
):
    worker = asyncio.create_task(DemoDriver(runs).serve())
    try:
        response = await client.post(
            "/api/v1/agent/runs",
            headers={
                **identity,
                "Idempotency-Key": str(uuid4()),
            },
            json=payload(identity, session, stream=True),
        )
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/event-stream")
        assert "run.accepted" in response.text
        assert "message.delta" in response.text
        assert "run.interrupted" in response.text
    finally:
        worker.cancel()
        with pytest.raises(asyncio.CancelledError):
            await worker


@pytest.mark.postgres
async def test_output_transaction_and_producer_dedup(
    client, identity, session, runs, db
):
    receipt = await submit(client, identity, session)
    await advance(runs, db, receipt, identity)
    owner, run_id = UUID(identity["X-User-UUID"]), UUID(receipt["run_id"])
    async with runs.repository() as repo:
        run = await repo.run(owner, run_id, lock=True)
        message_id = UUID(run["checkpoint"]["message_id"])
        writer = OutputWriter(repo, run)
        await writer.append(message_id, "once", "test:chunk")
        await writer.append(message_id, "once", "test:chunk")
    with pytest.raises(RuntimeError):
        async with runs.repository() as repo:
            run = await repo.run(owner, run_id, lock=True)
            await OutputWriter(repo, run).append(
                message_id, "rollback", "test:bad"
            )
            raise RuntimeError("Simulated worker crash")
    messages = (
        await client.get(
            f"/api/v1/sessions/{session['session_id']}/messages",
            headers=identity,
        )
    ).json()["items"]
    assert messages[0]["content"] == [{"type": "text", "text": "once"}]
    async with runs.repository() as repo:
        events = await repo.events(run_id, 0)
    assert sum(e.type == "message.delta" for e in events) == 1
    delta = next(e for e in events if e.type == "message.delta")
    assert messages[0]["event_sequence"] == delta.sequence


@pytest.mark.postgres
async def test_competing_resumes_are_serialized(
    client, identity, session, runs, db
):
    receipt = await submit(client, identity, session)
    detail = await advance(runs, db, receipt, identity, 3)
    results = await asyncio.gather(
        *[
            client.post(
                "/api/v1/agent/runs",
                headers={**identity, "Idempotency-Key": str(uuid4())},
                json=resume(identity, session, detail, decision),
            )
            for decision in ["approve", "reject"]
        ]
    )
    assert sorted(result.status_code for result in results) == [202, 409]
    accepted = next(i for i, result in enumerate(results) if result.is_success)
    advanced = await advance(runs, db, receipt, identity)
    if accepted == 0:
        assert len(advanced.executions) == 1
    else:
        assert advanced.status == "rejected"
        assert advanced.executions == []


@pytest.mark.postgres
async def test_disabled_backend_leaves_no_admission(
    client, identity, session, runs
):
    runs.settings.agent_backend = "disabled"
    response = await client.post(
        "/api/v1/agent/runs",
        headers={**identity, "Idempotency-Key": str(uuid4())},
        json=payload(identity, session),
    )
    assert response.status_code == 503
    base = f"/api/v1/sessions/{session['session_id']}"
    saved = (await client.get(base, headers=identity)).json()
    assert saved["active_run_id"] is None
    assert (await client.get(base + "/runs", headers=identity)).json()[
        "items"
    ] == []
    assert (await client.get(base + "/messages", headers=identity)).json()[
        "items"
    ] == []


@pytest.mark.postgres
async def test_payload_bounds_before_json_decode(client, identity):
    headers = {
        **identity,
        "Idempotency-Key": str(uuid4()),
        "Content-Type": "application/json",
    }
    response = await client.post(
        "/api/v1/agent/runs", headers=headers, content=b"x" * 131073
    )
    assert response.status_code == 413
    response = await client.post(
        "/api/v1/agent/runs",
        headers=headers,
        content="[" * 33 + "0" + "]" * 33,
    )
    assert response.status_code == 422
    assert response.json()["code"] == "JSON_TOO_DEEP"


@pytest.mark.postgres
async def test_cursor_scopes_and_expired_stream(
    client, identity, session, runs, db
):
    for _ in range(3):
        receipt = await submit(client, identity, session)
        await client.post(
            f"/api/v1/agent/runs/{receipt['run_id']}/cancel", headers=identity
        )
    path = f"/api/v1/sessions/{session['session_id']}"
    first = (await client.get(path + "/runs?limit=2", headers=identity)).json()
    assert first["has_more"] is True
    second = (
        await client.get(
            path + "/runs",
            headers=identity,
            params={"limit": 2, "cursor": first["next_cursor"]},
        )
    ).json()
    assert len(second["items"]) == 1
    assert (
        await client.get(
            path + "/messages",
            headers=identity,
            params={"cursor": first["next_cursor"]},
        )
    ).status_code == 422
    await db.execute(
        "UPDATE management.runs SET first_event_seq = 3 WHERE run_id = %s",
        (receipt["run_id"],),
    )
    expired = await client.get(
        f"/api/v1/agent/runs/{receipt['run_id']}/stream", headers=identity
    )
    assert expired.status_code == 410
    detail = await runs.detail(
        UUID(identity["X-User-UUID"]), UUID(receipt["run_id"])
    )
    assert detail.last_event_id.endswith(":4")


def test_message_request_does_not_allow_raw_graph_commands():
    with pytest.raises(ValidationError):
        RunRequest.model_validate(
            json.loads(
                json.dumps(
                    {
                        "user_id": str(uuid4()),
                        "session_id": str(uuid4()),
                        "input": {
                            "type": "message",
                            "content": [{"type": "text", "text": "hi"}],
                        },
                        "command": {"goto": "execute"},
                    }
                )
            )
        )


@pytest.mark.postgres
async def test_backend_switch_does_not_accept_unhandled_resume(
    client, identity, session, runs, db
):
    receipt = await submit(client, identity, session)
    detail = await advance(runs, db, receipt, identity, 3)
    runs.settings.agent_backend = "langgraph"
    response = await client.post(
        "/api/v1/agent/runs",
        headers={**identity, "Idempotency-Key": str(uuid4())},
        json=resume(identity, session, detail),
    )
    assert response.status_code == 409
    assert response.json()["code"] == "RUN_BACKEND_UNAVAILABLE"
    cancelled = await client.post(
        f"/api/v1/agent/runs/{receipt['run_id']}/cancel", headers=identity
    )
    assert cancelled.json()["status"] == "cancelled"


@pytest.mark.postgres
async def test_template_manager_roundtrip_and_authorization(
    client, identity, session, runs
):
    owner = UUID(identity["X-User-UUID"])

    async def resolver(data, config):
        # Fake verified principal, independent of body/config metadata.
        return decode_v1(data, owner=owner)

    manager = WorkflowManager()
    driver_task = asyncio.create_task(DemoDriver(runs).serve())
    envelope = {
        "agent_request": {
            "request_id": str(uuid4()),
            "request": payload(identity, session),
        }
    }
    try:
        async with manager.bind(runs, resolver):
            steps = [
                item
                async for item in manager.astream(
                    envelope,
                    config={"metadata": {"thread_id": "untrusted-thread"}},
                    stream_mode=["custom", "updates"],
                )
            ]
            assert all(
                mode == "custom" and isinstance(s, dict) for mode, s in steps
            )
            result = steps[-1][1]
            assert result["status"] == "awaiting_input"
            assert result["answer"]
            assert result["inputRequest"]["pattern"] == "review"
            replay = await manager.ainvoke(envelope)
            assert replay == result
            interrupted = result["inputRequest"]
            followup = {
                "agent_request": {
                    "request_id": str(uuid4()),
                    "request": payload(
                        identity,
                        session,
                        input={
                            "type": "resume",
                            "run_id": result["run_id"],
                            "interrupt_id": interrupted["interrupt_id"],
                            "response": {
                                "type": "plan_review",
                                "decision": "reject",
                            },
                        },
                    ),
                }
            }
            rejected = await manager.ainvoke(followup)
            assert rejected["status"] == "rejected"
            assert rejected["inputRequest"] is None
            assert rejected["input_requests"] == []
            # A new key cannot consume a stale interrupt a second time.
            followup["agent_request"]["request_id"] = str(uuid4())
            with pytest.raises(DomainError):
                await manager.ainvoke(followup)
            envelope["agent_request"]["request"]["user_id"] = str(uuid4())
            with pytest.raises(DomainError) as caught:
                await manager.ainvoke(envelope)
            assert caught.value.code == "IDENTITY_MISMATCH"
    finally:
        driver_task.cancel()
        await asyncio.gather(driver_task, return_exceptions=True)


@pytest.mark.postgres
async def test_template_observer_timeout_keeps_durable_work(
    client, identity, session, runs
):
    owner = UUID(identity["X-User-UUID"])

    async def resolver(data, config):
        return decode_v1(data, owner=owner)

    manager = WorkflowManager()
    async with manager.bind(runs, resolver):
        result = await manager.ainvoke(
            {
                "agent_request": {
                    "request_id": str(uuid4()),
                    "request": payload(identity, session),
                }
            }
        )
    assert result["status"] == "queued"  # No worker was started.
    assert result["answer"] == ""
    assert result["error"] is None
    assert (
        await runs.detail(owner, UUID(result["run_id"]))
    ).status == "queued"
    await runs.cancel(owner, UUID(result["run_id"]))
