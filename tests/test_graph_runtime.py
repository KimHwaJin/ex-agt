"""Real PostgreSQL checkpointer with deterministic, async model boundaries."""

import asyncio
import json
from uuid import UUID, uuid4

import pytest
from langchain_core.language_models.chat_models import (
    BaseChatModel,
    agenerate_from_stream,
)
from langchain_core.messages import AIMessageChunk, HumanMessage
from langchain_core.outputs import ChatGenerationChunk
from psycopg import OperationalError
from psycopg_pool import PoolClosed
from pydantic import Field, ValidationError

from d_test.agent_service.agents.assistant import build_assistant
from d_test.agent_service.agents.code_planner import build_code_planners
from d_test.agent_service.agents.intake import build_intake_agents
from d_test.agent_service.application.outputs import OutputWriter
from d_test.agent_service.graphs.assistant.builder import build_graph
from d_test.agent_service.infrastructure.database.checkpoints import (
    Checkpoints,
)
from d_test.agent_service.runtime.graph_driver import GraphDriver
from d_test.agent_service.runtime.management import ManagementRuntime
from d_test.agent_service.settings import Settings


class TestModel(BaseChatModel):
    __test__ = False
    seen: list[list[str]] = Field(default_factory=list)
    hold: bool = False
    fail: bool = False
    entered: asyncio.Event = Field(default_factory=asyncio.Event)
    release: asyncio.Event = Field(default_factory=asyncio.Event)
    intent: str = "general_question"
    structured_seen: list[str] = Field(default_factory=list)
    plan_implementation: str = "catalog"
    structured_invalid: bool = False
    planning_inputs: dict[str, str] = Field(default_factory=dict)
    bad_tool: bool = False
    risk_warnings: list[str] = Field(default_factory=list)

    def bind_tools(self, tools, **kwargs):
        raise AssertionError("Toolless intake must not bind empty tools")

    @property
    def _llm_type(self):
        return "test-async-model"

    def _generate(self, *args, **kwargs):
        raise AssertionError("Sync model calls are forbidden")

    async def _agenerate(
        self, messages, stop=None, run_manager=None, **kwargs
    ):
        return await agenerate_from_stream(self._astream(messages, **kwargs))

    async def _astream(self, messages, stop=None, run_manager=None, **kwargs):
        schema = kwargs.get("response_format", {}).get("json_schema", {})
        if schema:
            name = schema["name"]
            self.structured_seen.append(name)
            self.planning_inputs[name] = "\n".join(
                str(m.content) for m in messages
            )
            value = (
                {
                    "intent": self.intent,
                    "reason": "사용자 요청 의미에 따른 분류",
                }
                if name == "RequestRoute"
                else {
                    "title": "샘플 분석 계획",
                    "summary": "합성 데이터를 준비하고 분석할 예정입니다.",
                    "implementation": self.plan_implementation,
                    "steps": [
                        {
                            "description": "샘플 데이터 준비",
                            "reason": "재현 가능한 분석 입력 확보",
                            "expected_result": "샘플 데이터셋",
                        }
                    ],
                }
            )
            if name == "RiskAssessment":
                value = {
                    "warnings": self.risk_warnings,
                    "rationale": "테스트 검토",
                }
            elif name == "CatalogProposal":
                value = {
                    "steps": [
                        {
                            "description": "합성 데이터 준비",
                            "reason": "입력 확보",
                            "expected_result": "행 목록",
                            "input_step": None,
                            "parameters": {"query": "sample", "rows": 50},
                            "skill_id": "sample-data",
                            "skill_version": "0.1.0",
                            "tool_id": "unknown"
                            if self.bad_tool
                            else "fetch_sample_data",
                            "tool_version": "0.1.0",
                        }
                    ]
                }
            elif name == "GeneratedProposal":
                value = {
                    "steps": [
                        {
                            "description": "직접 계산",
                            "reason": "사용자 요청",
                            "expected_result": "합계",
                            "input_step": None,
                            "parameters": {},
                            "function_lines": [
                                "def total():",
                                "    return sum(range(11))",
                            ],
                        }
                    ]
                }
            text = "not-json" if self.structured_invalid else json.dumps(value)
            yield ChatGenerationChunk(message=AIMessageChunk(content=text))
            return
        texts = [m.content for m in messages if isinstance(m, HumanMessage)]
        self.seen.append(texts)
        self.entered.set()
        yield ChatGenerationChunk(message=AIMessageChunk(content="reply: "))
        if self.hold:
            await self.release.wait()
        if self.fail:
            raise ValueError("Intentional model failure")
        for text in texts:
            yield ChatGenerationChunk(message=AIMessageChunk(content=text))
            await asyncio.sleep(0)


@pytest.fixture
def model(monkeypatch):
    model = TestModel()
    monkeypatch.setattr(
        "d_test.agent_service.runtime.management.build_model", lambda _: model
    )
    return model


@pytest.fixture
def settings(settings, model):
    return settings.model_copy(
        update={
            "agent_backend": "langgraph",
            "model_name": "test-model",
            "embedded_run_worker": False,
            "worker_poll_seconds": 0.05,
            "output_flush_chars": 1,
        }
    )


@pytest.fixture
def runtime(client):
    return client._transport.app.state.management_runtime.resources


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


async def submit(client, identity, session, text="alpha"):
    response = await client.post(
        "/api/v1/agent/runs",
        headers={
            **identity,
            "Idempotency-Key": str(uuid4()),
        },
        json={
            "user_id": identity["X-User-UUID"],
            "session_id": session["session_id"],
            "input": {
                "type": "message",
                "content": [{"type": "text", "text": text}],
            },
        },
    )
    assert response.status_code == 202, response.text
    return UUID(response.json()["run_id"])


async def drive(runtime, identity, run_id, *, model=None):
    router, planner = build_intake_agents(model) if model else (None, None)
    driver = (
        runtime.driver
        if model is None
        else GraphDriver(
            runtime.runs,
            runtime.checkpoints,
            build_assistant(model),
            router=router,
            planner=planner,
            code_planners=build_code_planners(model),
        )
    )
    await driver.run_one(run_id, UUID(identity["X-User-UUID"]))
    return await runtime.runs.detail(UUID(identity["X-User-UUID"]), run_id)


@pytest.mark.postgres
async def test_history_survives_new_pool_and_session_isolation(
    client, identity, session, runtime, model, home
):
    first = await submit(client, identity, session, "remember-indigo")
    assert (await drive(runtime, identity, first)).status == "completed"
    # A different runtime connection pool must load existing checkpoints.
    await runtime.checkpoints.close()
    runtime.checkpoints = Checkpoints(runtime.settings)
    await runtime.checkpoints.open()
    fresh = TestModel()
    second = await submit(client, identity, session, "what-did-I-say")
    assert (await drive(runtime, identity, second, model=fresh)).status == (
        "completed"
    )
    assert fresh.seen == [["remember-indigo", "what-did-I-say"]]
    async with runtime.checkpoints.session(session["session_id"]) as saver:
        router, planner = build_intake_agents(fresh)
        graph = build_graph(
            build_assistant(fresh),
            saver,
            router=router,
            planner=planner,
            code_planners=build_code_planners(fresh),
        )
        snapshot = await graph.aget_state(
            {
                "configurable": {
                    "thread_id": session["session_id"],
                }
            }
        )
        assert len(snapshot.values["messages"]) == 4
        assert not snapshot.next
    other = (
        await client.post(
            "/api/v1/sessions",
            headers={
                **identity,
                "Idempotency-Key": str(uuid4()),
            },
            json={"project_id": home["default_project"]["project_id"]},
        )
    ).json()
    third = await submit(client, identity, other, "isolated")
    await drive(runtime, identity, third, model=fresh)
    assert fresh.seen[-1] == ["isolated"]


@pytest.mark.postgres
async def test_checkpoint_complete_projection_retry_without_model_call(
    client, identity, session, runtime, monkeypatch, model
):
    original = OutputWriter.replace
    fail_once = True

    async def replace(self, message_id, text, *, complete=False):
        nonlocal fail_once
        if complete and fail_once:
            fail_once = False
            raise OperationalError("Projection connection lost")
        await original(self, message_id, text, complete=complete)

    monkeypatch.setattr(OutputWriter, "replace", replace)
    run_id = await submit(client, identity, session)
    assert (await drive(runtime, identity, run_id)).status == "running"
    assert len(model.seen) == 1
    replacement = TestModel(fail=True)
    result = await drive(runtime, identity, run_id, model=replacement)
    assert result.status == "completed"
    assert replacement.seen == []
    messages = (
        await client.get(
            f"/api/v1/sessions/{session['session_id']}/messages",
            headers=identity,
        )
    ).json()["items"]
    assert len(messages) == 2
    assert messages[0]["content"][0]["text"] == "reply: alpha"


@pytest.mark.postgres
async def test_cancel_stops_graph_before_releasing_session(
    client, identity, session, runtime, model
):
    model.hold = True
    run_id = await submit(client, identity, session)
    task = asyncio.create_task(drive(runtime, identity, run_id))
    await asyncio.wait_for(model.entered.wait(), 5)
    response = await client.post(
        f"/api/v1/agent/runs/{run_id}/cancel", headers=identity
    )
    assert response.status_code == 202
    assert (await asyncio.wait_for(task, 5)).status == "cancelled"
    saved = (
        await client.get(
            f"/api/v1/sessions/{session['session_id']}", headers=identity
        )
    ).json()
    assert saved["active_run_id"] is None
    assert len(model.seen) == 1
    # A new input must not execute the cancelled pending graph work first.
    new_model = TestModel()
    second = await submit(client, identity, session, "new-turn")
    assert (
        await drive(runtime, identity, second, model=new_model)
    ).status == ("completed")
    assert len(new_model.seen) == 1


@pytest.mark.postgres
async def test_shutdown_recovery_and_concurrent_workers(
    client, identity, session, runtime, model
):
    model.hold = True
    run_id = await submit(client, identity, session)
    task = asyncio.create_task(drive(runtime, identity, run_id))
    await asyncio.wait_for(model.entered.wait(), 5)
    competing = TestModel()
    await drive(runtime, identity, run_id, model=competing)
    assert competing.seen == []
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert (
        await runtime.runs.detail(UUID(identity["X-User-UUID"]), run_id)
    ).status == "running"
    recovered = TestModel()
    assert (
        await drive(runtime, identity, run_id, model=recovered)
    ).status == ("completed")
    assert recovered.seen == [["alpha"]]
    events = (
        await client.get(
            f"/api/v1/agent/runs/{run_id}/stream", headers=identity
        )
    ).text
    assert "message.updated" in events
    assert "message.delta" in events


@pytest.mark.postgres
async def test_model_error_has_partial_message_and_no_report(
    client, identity, session, runtime, model
):
    model.fail = True
    run_id = await submit(client, identity, session)
    detail = await drive(runtime, identity, run_id)
    assert detail.status == "failed"
    assert detail.executions == []
    messages = (
        await client.get(
            f"/api/v1/sessions/{session['session_id']}/messages",
            headers=identity,
        )
    ).json()["items"]
    assert len(messages) == 2
    assert messages[0]["status"] == "failed"
    assert messages[0]["content"][0]["text"] == "reply: "


@pytest.mark.postgres
async def test_checkpoint_setup_is_idempotent_and_missing_schema_fails(
    runtime,
):
    await runtime.checkpoints.setup()
    await runtime.checkpoints.setup()
    missing = Checkpoints(
        runtime.settings.model_copy(
            update={
                "checkpoint_schema": "missing_" + uuid4().hex,
            }
        )
    )
    try:
        with pytest.raises(Exception, match="does not exist"):
            await missing.open()
    finally:
        await missing.close()


def test_graph_settings_require_model_and_pool_headroom(settings):
    for changes in [
        {"model_name": None},
        {"checkpoint_pool_max_size": 2},
        {"checkpoint_schema": "public;drop"},
    ]:
        with pytest.raises(ValidationError):
            Settings.model_validate({**settings.model_dump(), **changes})


@pytest.mark.postgres
async def test_closed_idle_connection_is_replaced_without_losing_memory(
    client, identity, session, runtime, db
):
    first = await submit(client, identity, session, "saved-fact")
    assert (await drive(runtime, identity, first)).status == "completed"
    async with runtime.checkpoints.connection() as connection:
        row = await (
            await connection.execute("SELECT pg_backend_pid() AS pid")
        ).fetchone()
        pid = row["pid"]
    # This PID belongs only to this test's pool, never an external service.
    await db.execute("SELECT pg_terminate_backend(%s)", (pid,))
    second = await submit(client, identity, session, "recall")
    fresh = TestModel()
    assert (await drive(runtime, identity, second, model=fresh)).status == (
        "completed"
    )
    assert fresh.seen == [["saved-fact", "recall"]]


@pytest.mark.postgres
async def test_readiness_checks_checkpoint_connection(runtime):
    await runtime.ready()
    await runtime.checkpoints.close()
    with pytest.raises(PoolClosed):
        await runtime.ready()


async def resume_review(
    client,
    identity,
    session,
    detail,
    decision,
    *,
    instruction=None,
    key=None,
    main_model_name=None,
):
    response = {"type": "plan_review", "decision": decision}
    if instruction is not None:
        response["instruction"] = instruction
    return await client.post(
        "/api/v1/agent/runs",
        headers={**identity, "Idempotency-Key": key or str(uuid4())},
        json={
            "user_id": identity["X-User-UUID"],
            "session_id": session["session_id"],
            "main_model_name": main_model_name,
            "input": {
                "type": "resume",
                "run_id": str(detail.run_id),
                "interrupt_id": str(detail.pending_interrupts[0].interrupt_id),
                "response": response,
            },
        },
    )


@pytest.mark.postgres
@pytest.mark.parametrize("intent", ["general_question", "analysis_question"])
async def test_question_routes_without_review_or_json_leak(
    client, identity, session, runtime, model, intent
):
    model.intent = intent
    run_id = await submit(client, identity, session)
    detail = await drive(runtime, identity, run_id)
    assert detail.status == "completed"
    assert detail.pending_interrupts == []
    assert model.structured_seen == ["RequestRoute"]
    events = (
        await client.get(
            f"/api/v1/agent/runs/{run_id}/stream", headers=identity
        )
    ).text
    assert "event: run.classified" in events
    async with runtime.runs.repository() as repo:
        deltas = await repo.all(
            "SELECT data FROM management.run_events "
            "WHERE run_id = %s AND type = 'message.delta'",
            (run_id,),
        )
    assert "".join(row["data"]["delta"] for row in deltas) == "reply: alpha"


@pytest.mark.postgres
@pytest.mark.parametrize("intent", ["analysis_task", "code_task"])
async def test_task_review_releases_graph_lock_but_blocks_new_messages(
    client, identity, session, runtime, model, intent
):
    model.intent = intent
    model.plan_implementation = "generated_code"
    run_id = await submit(client, identity, session)
    detail = await drive(runtime, identity, run_id)
    assert detail.status == "awaiting_input"
    assert detail.executions == []
    assert detail.pending_interrupts[0].payload["executable"] is False
    assert detail.pending_interrupts[0].payload["intent"] == intent
    assert model.seen == []
    async with runtime.checkpoints.session(session["session_id"]) as saver:
        assert saver is not None
    # Duplicate workers must not consume recovery attempts while waiting.
    await drive(runtime, identity, run_id)
    assert model.structured_seen == [
        "RequestRoute",
        "PlanDraft",
        "RiskAssessment",
        "GeneratedProposal",
    ]
    response = await client.post(
        "/api/v1/agent/runs",
        headers={**identity, "Idempotency-Key": str(uuid4())},
        json={
            "user_id": identity["X-User-UUID"],
            "session_id": session["session_id"],
            "input": {
                "type": "message",
                "content": [{"type": "text", "text": "new work"}],
            },
        },
    )
    assert response.status_code == 409


@pytest.mark.postgres
async def test_modify_versions_stale_reviews_and_idempotent_resume(
    client, identity, session, runtime, model, db
):
    model.intent = "analysis_task"
    run_id = await submit(client, identity, session)
    detail = await drive(runtime, identity, run_id)
    original = detail
    plan_id = detail.pending_interrupts[0].payload["plan_id"]
    for version in range(2, 6):
        model.plan_implementation = "generated_code"
        key = str(uuid4())
        instruction = "내부 함수 없이 직접 코드를 작성해줘"
        first = await resume_review(
            client,
            identity,
            session,
            detail,
            "modify",
            instruction=instruction,
            key=key,
        )
        assert first.status_code == 202
        replay = await resume_review(
            client,
            identity,
            session,
            detail,
            "modify",
            instruction=instruction,
            key=key,
        )
        assert replay.json() == first.json()
        previous_id = detail.pending_interrupts[0].interrupt_id
        detail = await drive(runtime, identity, run_id)
        assert detail.status == "awaiting_input"
        review = detail.pending_interrupts[0]
        assert review.interrupt_id != previous_id
        assert review.payload["plan_version"] == version
        assert review.payload["plan_id"] == plan_id
        assert review.payload["implementation"] == "generated_code"
    stale = await resume_review(client, identity, session, original, "approve")
    assert stale.status_code == 409
    count = await (
        await db.execute(
            "SELECT count(*) FROM management.run_interrupts WHERE run_id = %s",
            (run_id,),
        )
    ).fetchone()
    assert count[0] == 5
    assert model.structured_seen.count("RequestRoute") == 1
    assert model.structured_seen.count("PlanDraft") == 5


@pytest.mark.postgres
@pytest.mark.parametrize(
    "decision,status", [("approve", "failed"), ("reject", "rejected")]
)
async def test_restart_resume_no_extra_model_or_execution(
    client, identity, session, runtime, model, decision, status
):
    model.intent = "analysis_task"
    run_id = await submit(client, identity, session)
    detail = await drive(runtime, identity, run_id)
    response = await resume_review(client, identity, session, detail, decision)
    assert response.status_code == 202
    await runtime.checkpoints.close()
    runtime.checkpoints = Checkpoints(runtime.settings)
    await runtime.checkpoints.open()
    fresh = TestModel(fail=True, structured_invalid=True)
    result = await drive(runtime, identity, run_id, model=fresh)
    assert result.status == status
    assert result.executions == []
    assert result.pending_interrupts == []
    assert fresh.seen == fresh.structured_seen == []
    if decision == "approve":
        assert result.error.code == "EXECUTOR_NOT_CONFIGURED"
    saved = (
        await client.get(
            f"/api/v1/sessions/{session['session_id']}", headers=identity
        )
    ).json()
    assert saved["active_run_id"] is None


@pytest.mark.postgres
async def test_interrupt_projection_failure_recovers_without_replanning(
    client, identity, session, runtime, model, monkeypatch
):
    from d_test.agent_service.runtime import graph_driver

    model.intent = "analysis_task"
    original = graph_driver.publish_review
    fail_once = True

    async def publish(output, interruption):
        nonlocal fail_once
        await original(output, interruption)
        if fail_once:
            fail_once = False
            raise OperationalError("Crash before review transaction commit")

    monkeypatch.setattr(graph_driver, "publish_review", publish)
    run_id = await submit(client, identity, session)
    assert (await drive(runtime, identity, run_id)).status == "running"
    fresh = TestModel(structured_invalid=True)
    detail = await drive(runtime, identity, run_id, model=fresh)
    assert detail.status == "awaiting_input"
    assert len(detail.pending_interrupts) == 1
    assert fresh.structured_seen == []


@pytest.mark.postgres
async def test_cancel_review_then_new_run_does_not_resume_old_plan(
    client, identity, session, runtime, model
):
    model.intent = "analysis_task"
    run_id = await submit(client, identity, session)
    detail = await drive(runtime, identity, run_id)
    result = await client.post(
        f"/api/v1/agent/runs/{run_id}/cancel", headers=identity
    )
    assert result.json()["status"] == "cancelled"
    stale = await resume_review(client, identity, session, detail, "approve")
    assert stale.status_code == 409
    fresh = TestModel()
    new_run = await submit(client, identity, session, "새 질문")
    assert (
        await drive(runtime, identity, new_run, model=fresh)
    ).status == "completed"
    assert fresh.structured_seen == ["RequestRoute"]


@pytest.mark.postgres
async def test_invalid_route_output_fails_without_fallback_execution(
    client, identity, session, runtime, model
):
    model.structured_invalid = True
    run_id = await submit(client, identity, session)
    detail = await drive(runtime, identity, run_id)
    assert detail.status == "failed"
    assert detail.pending_interrupts == detail.executions == []
    assert model.seen == []


@pytest.mark.postgres
async def test_concurrent_review_responses_accept_only_one(
    client, identity, session, runtime, model
):
    model.intent = "analysis_task"
    run_id = await submit(client, identity, session)
    detail = await drive(runtime, identity, run_id)
    results = await asyncio.gather(
        resume_review(client, identity, session, detail, "approve"),
        resume_review(client, identity, session, detail, "reject"),
    )
    assert sorted(r.status_code for r in results) == [202, 409]
    assert (await drive(runtime, identity, run_id)).status in {
        "failed",
        "rejected",
    }


@pytest.mark.postgres
async def test_legacy_v1_run_remains_compatible(
    client, identity, session, runtime, model
):
    run_id = await submit(client, identity, session)
    async with runtime.runs.repository() as repo:
        run = await repo.run(UUID(identity["X-User-UUID"]), run_id, lock=True)
        cp = {**run["checkpoint"], "graph_version": "assistant-v1"}
        await repo.checkpoint(run, cp, 0)
    assert (await drive(runtime, identity, run_id)).status == "completed"
    assert model.structured_seen == []


@pytest.mark.postgres
async def test_resume_completion_projection_retry_does_not_repeat_decision(
    client, identity, session, runtime, model, monkeypatch
):
    model.intent = "analysis_task"
    run_id = await submit(client, identity, session)
    detail = await drive(runtime, identity, run_id)
    assert (
        await resume_review(client, identity, session, detail, "reject")
    ).status_code == 202
    original = OutputWriter.replace
    fail_once = True

    async def replace(self, message_id, text, *, complete=False):
        nonlocal fail_once
        if complete and fail_once:
            fail_once = False
            raise OperationalError("Crash after checkpointed review result")
        await original(self, message_id, text, complete=complete)

    monkeypatch.setattr(OutputWriter, "replace", replace)
    assert (await drive(runtime, identity, run_id)).status == "running"
    fresh = TestModel(structured_invalid=True, fail=True)
    assert (
        await drive(runtime, identity, run_id, model=fresh)
    ).status == "rejected"
    assert fresh.seen == fresh.structured_seen == []


@pytest.mark.postgres
async def test_modified_checkpoint_projection_retry_does_not_reapply_modify(
    client, identity, session, runtime, model, monkeypatch
):
    from d_test.agent_service.runtime import graph_driver

    model.intent = "analysis_task"
    run_id = await submit(client, identity, session)
    detail = await drive(runtime, identity, run_id)
    assert (
        await resume_review(
            client, identity, session, detail, "modify", instruction="수정"
        )
    ).status_code == 202
    original = graph_driver.publish_review
    fail_once = True

    async def publish(output, interruption):
        nonlocal fail_once
        await original(output, interruption)
        if fail_once:
            fail_once = False
            raise OperationalError("Crash projecting second review")

    monkeypatch.setattr(graph_driver, "publish_review", publish)
    assert (await drive(runtime, identity, run_id)).status == "running"
    fresh = TestModel(structured_invalid=True)
    restored = await drive(runtime, identity, run_id, model=fresh)
    assert restored.status == "awaiting_input"
    assert restored.pending_interrupts[0].payload["plan_version"] == 2
    assert fresh.structured_seen == []


@pytest.mark.postgres
async def test_plan_snapshots_audit_code_privacy_and_direct_revision(
    client, identity, session, runtime, model
):
    model.intent = "analysis_task"
    model.risk_warnings = ["리소스 사용량을 확인하세요."]
    run_id = await submit(client, identity, session)
    first = await drive(runtime, identity, run_id)
    payload = first.pending_interrupts[0].payload
    assert payload["code_prepared"] is True
    assert payload["generation_risk"]["warnings"] == model.risk_warnings
    assert payload["steps"][0]["tool_id"] == "fetch_sample_data"
    async with runtime.runs.repository() as repo:
        saved = await repo.one(
            "SELECT * FROM management.execution_plans WHERE run_id = %s",
            (run_id,),
        )
        assert saved["plan_sha256"] == payload["plan_sha256"]
        assert saved["snapshot"]["plan"] == payload
        assert "def fetch_sample_data" in saved["snapshot"]["cells"][0]["code"]
        assert saved["created_by"] == UUID(identity["X-User-UUID"])
        assert saved["updated_by"] == saved["created_by"]
        assert saved["updated_at"] == saved["created_at"]
        events = await repo.all(
            "SELECT data FROM management.run_events WHERE run_id = %s",
            (run_id,),
        )
    public = json.dumps(events, default=str)
    assert "def fetch_sample_data" not in public
    assert "function_source" not in public
    response = await resume_review(
        client,
        identity,
        session,
        first,
        "modify",
        instruction="내부 함수 없이 직접 코드를 만들어줘",
    )
    assert response.status_code == 202
    model.plan_implementation = "generated_code"
    revised = await drive(runtime, identity, run_id)
    assert revised.pending_interrupts[0].payload["plan_version"] == 2
    direct_prompt = model.planning_inputs["GeneratedProposal"]
    assert "parameters_schema" not in direct_prompt
    assert "fetch_sample_data" not in direct_prompt
    async with runtime.runs.repository() as repo:
        versions = await repo.all(
            "SELECT * FROM management.execution_plans WHERE run_id = %s "
            "ORDER BY plan_version",
            (run_id,),
        )
    assert len(versions) == 2
    assert versions[0] == saved
    assert versions[0]["plan_id"] == versions[1]["plan_id"]
    assert versions[0]["plan_sha256"] != versions[1]["plan_sha256"]
    assert "tool_id" not in versions[1]["snapshot"]["cells"][0]


@pytest.mark.postgres
async def test_unknown_tool_never_publishes_approval(
    client, identity, session, runtime, model
):
    model.intent, model.bad_tool = "analysis_task", True
    run_id = await submit(client, identity, session)
    detail = await drive(runtime, identity, run_id)
    assert detail.status == "failed"
    assert not detail.pending_interrupts and not detail.executions
    async with runtime.runs.repository() as repo:
        plans = await repo.all(
            "SELECT * FROM management.execution_plans WHERE run_id = %s",
            (run_id,),
        )
    assert plans == []


@pytest.mark.postgres
async def test_v2_pending_review_remains_resumable(
    client, identity, session, runtime, model
):
    model.intent = "analysis_task"
    run_id = await submit(client, identity, session)
    async with runtime.runs.repository() as repo:
        run = await repo.run(UUID(identity["X-User-UUID"]), run_id, lock=True)
        await repo.checkpoint(
            run, {**run["checkpoint"], "graph_version": "assistant-v2"}, 0
        )
    detail = await drive(runtime, identity, run_id)
    assert detail.pending_interrupts[0].payload["stage"] == "draft"
    assert model.structured_seen == ["RequestRoute", "PlanDraft"]
    response = await resume_review(client, identity, session, detail, "reject")
    assert response.status_code == 202
    fresh = TestModel()
    result = await drive(runtime, identity, run_id, model=fresh)
    assert result.status == "rejected"
    assert fresh.structured_seen == []


@pytest.mark.postgres
async def test_model_selection_new_message_and_hitl_resume(
    client, identity, session, runtime, model
):
    alternate = TestModel(intent="analysis_task")
    router, planner = build_intake_agents(alternate)
    runtime.driver.model_agents["alternate-model"] = (
        build_assistant(alternate),
        router,
        planner,
        build_code_planners(alternate),
    )
    runtime.settings.allowed_model_names = ["alternate-model"]
    catalog = await client.get("/api/v1/agent/models", headers=identity)
    assert catalog.status_code == 200
    assert catalog.json()["items"] == [
        {"name": "test-model"},
        {"name": "alternate-model"},
    ]
    model.intent = "analysis_task"
    run_id = await submit(client, identity, session)
    detail = await drive(runtime, identity, run_id)
    key = str(uuid4())
    changed = await resume_review(
        client,
        identity,
        session,
        detail,
        "modify",
        instruction="분석 계획을 변경해줘",
        key=key,
        main_model_name="alternate-model",
    )
    assert changed.status_code == 202, changed.text
    replay = await resume_review(
        client,
        identity,
        session,
        detail,
        "modify",
        instruction="분석 계획을 변경해줘",
        key=key,
        main_model_name="alternate-model",
    )
    assert replay.json() == changed.json()
    detail = await drive(runtime, identity, run_id)
    assert detail.status == "awaiting_input"
    assert alternate.structured_seen == [
        "PlanDraft",
        "RiskAssessment",
        "CatalogProposal",
    ]
    assert model.structured_seen.count("PlanDraft") == 1
    # Invalid model rolls back interrupt consumption and can be retried.
    invalid = await resume_review(
        client,
        identity,
        session,
        detail,
        "reject",
        main_model_name="unknown",
    )
    assert invalid.status_code == 422
    # Omitted selection on resume keeps the most recently selected model.
    accepted = await resume_review(client, identity, session, detail, "reject")
    assert accepted.status_code == 202
    assert (await drive(runtime, identity, run_id)).status == "rejected"
    async with runtime.runs.repository() as repo:
        history = await repo.all(
            "SELECT data FROM management.run_events WHERE run_id = %s "
            "AND type = 'model.selected' ORDER BY sequence",
            (run_id,),
        )
    assert [row["data"]["model_name"] for row in history] == [
        "test-model",
        "alternate-model",
        "alternate-model",
    ]
    alternate.intent = "general_question"
    response = await client.post(
        "/api/v1/agent/runs",
        headers={**identity, "Idempotency-Key": str(uuid4())},
        json={
            "user_id": identity["X-User-UUID"],
            "session_id": session["session_id"],
            "main_model_name": "alternate-model",
            "input": {
                "type": "message",
                "content": [{"type": "text", "text": "new-question"}],
            },
        },
    )
    assert response.status_code == 202
    result = await drive(runtime, identity, UUID(response.json()["run_id"]))
    assert result.status == "completed"
    assert alternate.seen[-1][-1] == "new-question"


@pytest.mark.postgres
@pytest.mark.parametrize("fail_second", [False, True])
async def test_runtime_owns_all_selected_model_clients(
    runtime, monkeypatch, fail_second
):
    built, closed = [], []

    def build(settings):
        if fail_second and settings.model_name == "alternate-model":
            raise RuntimeError("second model init failed")
        model = TestModel()
        built.append(model)
        return model

    async def close(model):
        closed.append(model)

    monkeypatch.setattr(
        "d_test.agent_service.runtime.management.build_model", build
    )
    monkeypatch.setattr(
        "d_test.agent_service.runtime.management.close_model", close
    )
    fresh = ManagementRuntime.build(
        runtime.settings.model_copy(
            update={
                "allowed_model_names": ["alternate-model"],
            }
        )
    )
    try:
        if fail_second:
            with pytest.raises(RuntimeError, match="second model init"):
                await fresh.start()
        else:
            await fresh.start()
            assert isinstance(fresh.driver, GraphDriver)
            assert fresh.driver.model_agents is not None
            assert set(fresh.driver.model_agents) == {
                "test-model",
                "alternate-model",
            }
            assert len(built) == 2
    finally:
        await fresh.close()
    assert [id(model) for model in closed] == [id(model) for model in built]
    assert fresh.pool.closed
