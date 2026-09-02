import json

import pytest

from agent.cutover_policy import (
    CutoverPhase,
    RollbackDisposition,
    rollback_decision,
)
from agent.cutover_policy_main import main


@pytest.mark.parametrize(
    "phase",
    [
        CutoverPhase.ADMISSION_OPEN,
        CutoverPhase.FREEZE_VERIFIED,
        CutoverPhase.LEGACY_DRAINED,
        CutoverPhase.LEGACY_STOPPED,
    ],
)
def test_legacy_can_continue_before_migration(
    phase: CutoverPhase,
) -> None:
    assert rollback_decision(phase).legacy_restart_allowed is True


def test_post_migration_restart_requires_compatibility_evidence() -> None:
    decision = rollback_decision(CutoverPhase.MIGRATION_APPLIED)

    assert decision.disposition is (
        RollbackDisposition.LEGACY_RESTART_REQUIRES_SCHEMA_CHECK
    )
    assert decision.legacy_restart_allowed is False


@pytest.mark.parametrize(
    "phase",
    [
        CutoverPhase.TARGET_STARTED,
        CutoverPhase.TARGET_VERIFIED,
        CutoverPhase.ADMISSION_REOPENED,
    ],
)
def test_target_start_is_the_forward_only_boundary(
    phase: CutoverPhase,
) -> None:
    decision = rollback_decision(phase)

    assert decision.disposition is RollbackDisposition.FORWARD_RECOVERY_ONLY
    assert decision.legacy_restart_allowed is False


def test_rollback_cli_emits_machine_readable_decision(
    capsys: pytest.CaptureFixture[str],
) -> None:
    assert main(["--phase", "TARGET_STARTED"]) == 0

    payload = json.loads(capsys.readouterr().out)
    assert payload["phase"] == "TARGET_STARTED"
    assert payload["disposition"] == "FORWARD_RECOVERY_ONLY"
