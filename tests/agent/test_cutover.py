from pathlib import Path
from unittest.mock import AsyncMock

import pytest
import yaml

from agent.cutover import (
    CutoverProbe,
    CutoverSnapshot,
    StreamGroupState,
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
        "admissions_frozen": True,
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
        admissions_frozen=False,
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

    assert len(blockers) == 8
    assert "new task admission" in blockers[0]
    assert "pending agent.commands/commands-v1: 5" in blockers[5]
    assert "lag agent.commands/commands-v1: 6" in blockers[6]
    assert blockers[7].endswith("executor.events/events-v1")


@pytest.mark.asyncio
async def test_stable_report_requires_two_identical_ready_samples() -> None:
    probe = object.__new__(CutoverProbe)
    probe.snapshot = AsyncMock(side_effect=[snapshot(), snapshot()])

    report = await probe.stable_report(
        admissions_frozen=True,
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
        admissions_frozen=True,
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
        admissions_frozen=True,
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
        "--admissions-frozen",
        "--stable-seconds",
        "10",
    ]
