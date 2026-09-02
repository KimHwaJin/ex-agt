from datetime import UTC, datetime
from pathlib import Path
from unittest.mock import AsyncMock

import httpx
import pytest
import yaml

from agent.cutover import (
    ADMISSION_SCOPE,
    ADMISSION_STATE,
    AdmissionFreezeState,
    CutoverProbe,
    CutoverSnapshot,
    HttpAdmissionFreezeProbe,
    StreamGroupState,
    UnsafeStaticAdmissionFreezeProbe,
)

ROOT = Path(__file__).resolve().parents[2]


def group(stream: str, name: str) -> StreamGroupState:
    return StreamGroupState(
        stream=stream,
        group=name,
        exists=True,
        pending=0,
        lag=0,
        last_delivered_id="10-0",
    )


def snapshot(**changes) -> CutoverSnapshot:
    values = {
        "admission": AdmissionFreezeState(
            source="https://bff.internal/cutover",
            verified=True,
            schema_version=1,
            state=ADMISSION_STATE,
            scope=ADMISSION_SCOPE,
            freeze_id="release-42",
            revision="bff-revision-7",
            frozen_at="2026-09-02T00:00:00+00:00",
        ),
        "active_tasks": 0,
        "unfinished_commands": 0,
        "unpublished_product_events": 0,
        "locked_sessions": 0,
        "command_group": group("agent.commands", "commands-v1"),
        "executor_event_group": group("executor.events", "events-v1"),
    }
    values.update(changes)
    return CutoverSnapshot(**values)


def test_ready_snapshot_has_no_blockers() -> None:
    assert snapshot().blockers() == ()


def test_every_unsafe_boundary_is_reported() -> None:
    missing = StreamGroupState("executor.events", "events-v1", False)
    blockers = snapshot(
        admission=AdmissionFreezeState(
            source="https://bff.internal/cutover",
            verified=False,
            error="HTTP 503",
        ),
        active_tasks=2,
        unfinished_commands=3,
        unpublished_product_events=4,
        locked_sessions=1,
        command_group=StreamGroupState(
            "agent.commands",
            "commands-v1",
            True,
            pending=5,
            lag=6,
            last_delivered_id="7-0",
        ),
        executor_event_group=missing,
    ).blockers()

    assert len(blockers) == 10
    assert "not verified" in blockers[0]
    assert "pending agent.commands/commands-v1: 5" in blockers[7]
    assert "lag agent.commands/commands-v1: 6" in blockers[8]
    assert blockers[9].endswith("executor.events/events-v1")


@pytest.mark.asyncio
async def test_stable_report_requires_two_identical_ready_samples() -> None:
    probe = object.__new__(CutoverProbe)
    probe.snapshot = AsyncMock(side_effect=[snapshot(), snapshot()])

    report = await probe.stable_report(
        stable_seconds=0,
    )

    assert report.ready is True
    assert report.stable is True
    assert probe.snapshot.await_count == 2


@pytest.mark.asyncio
async def test_changed_progress_blocks_cutover() -> None:
    probe = object.__new__(CutoverProbe)
    changed = snapshot(command_group=group("agent.commands", "commands-v2"))
    probe.snapshot = AsyncMock(side_effect=[snapshot(), changed])

    report = await probe.stable_report(
        stable_seconds=0,
    )

    assert report.ready is False
    assert report.blockers == (
        "drain evidence changed between stable samples",
    )


@pytest.mark.asyncio
async def test_blocked_first_sample_returns_without_waiting() -> None:
    probe = object.__new__(CutoverProbe)
    probe.snapshot = AsyncMock(return_value=snapshot(active_tasks=1))

    report = await probe.stable_report(
        stable_seconds=30,
    )

    assert report.ready is False
    assert report.second is None
    probe.snapshot.assert_awaited_once()


def test_kubernetes_preflight_job_runs_read_only_cutover_check() -> None:
    manifest = yaml.safe_load(
        (
            ROOT / "deploy" / "worker-cutover" / "preflight-job.yaml.example"
        ).read_text(encoding="utf-8")
    )
    container = manifest["spec"]["template"]["spec"]["containers"][0]

    assert manifest["kind"] == "Job"
    assert manifest["spec"]["backoffLimit"] == 0
    assert container["args"] == [
        "ex-agent-cutover-check",
        "--admission-evidence-url",
        "$(BFF_CUTOVER_EVIDENCE_URL)",
        "--expected-freeze-id",
        "$(CUTOVER_FREEZE_ID)",
        "--stable-seconds",
        "10",
    ]
    secrets = {
        source["secretRef"]["name"]
        for source in container["envFrom"]
        if "secretRef" in source
    }
    assert "ex-agent-cutover" in secrets


def freeze_payload(**changes) -> dict[str, object]:
    payload: dict[str, object] = {
        "schema_version": 1,
        "state": ADMISSION_STATE,
        "scope": ADMISSION_SCOPE,
        "freeze_id": "release-42",
        "revision": "bff-revision-7",
        "frozen_at": "2026-09-02T00:00:00Z",
        "expires_at": "2026-09-03T00:00:00Z",
    }
    payload.update(changes)
    return payload


@pytest.mark.asyncio
async def test_http_admission_probe_verifies_correlated_receipt() -> None:
    observed_authorization = ""

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal observed_authorization
        observed_authorization = request.headers["Authorization"]
        return httpx.Response(200, json=freeze_payload())

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    probe = HttpAdmissionFreezeProbe(
        url="https://bff.internal/operations/admission-freeze",
        expected_freeze_id="release-42",
        bearer_token="secret-token",
        client=client,
        now=lambda: datetime(2026, 9, 2, 1, tzinfo=UTC),
    )
    try:
        state = await probe.snapshot()
    finally:
        await client.aclose()

    assert state.verified is True
    assert state.freeze_id == "release-42"
    assert state.revision == "bff-revision-7"
    assert state.blockers() == ()
    assert observed_authorization == "Bearer secret-token"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("changes", "message"),
    [
        ({"freeze_id": "another-release"}, "does not match"),
        ({"state": "OPEN"}, "state must be"),
        ({"scope": "ALL_REQUESTS"}, "scope must be"),
        ({"schema_version": 2}, "schema_version"),
        (
            {"expires_at": "2026-09-01T00:00:00Z"},
            "expired",
        ),
    ],
)
async def test_http_admission_probe_rejects_unsafe_receipt(
    changes: dict[str, object],
    message: str,
) -> None:
    transport = httpx.MockTransport(
        lambda _: httpx.Response(200, json=freeze_payload(**changes))
    )
    async with httpx.AsyncClient(transport=transport) as client:
        probe = HttpAdmissionFreezeProbe(
            url="https://bff.internal/operations/admission-freeze",
            expected_freeze_id="release-42",
            bearer_token="secret-token",
            client=client,
            now=lambda: datetime(2026, 9, 2, 1, tzinfo=UTC),
        )
        state = await probe.snapshot()

    assert state.verified is False
    assert message in (state.error or "")
    assert state.blockers()


@pytest.mark.asyncio
async def test_unsafe_probe_is_visibly_marked_for_local_rehearsal() -> None:
    state = await UnsafeStaticAdmissionFreezeProbe().snapshot()

    assert state.verified is True
    assert state.source == "unsafe-operator-assertion"
    assert state.freeze_id == "unsafe-local-rehearsal"
