"""Machine-readable rollback policy for the one-time Worker cutover."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from enum import StrEnum
from typing import Any


class CutoverPhase(StrEnum):
    ADMISSION_OPEN = "ADMISSION_OPEN"
    FREEZE_VERIFIED = "FREEZE_VERIFIED"
    LEGACY_DRAINED = "LEGACY_DRAINED"
    LEGACY_STOPPED = "LEGACY_STOPPED"
    MIGRATION_APPLIED = "MIGRATION_APPLIED"
    TARGET_STARTED = "TARGET_STARTED"
    TARGET_VERIFIED = "TARGET_VERIFIED"
    ADMISSION_REOPENED = "ADMISSION_REOPENED"


class RollbackDisposition(StrEnum):
    NO_CUTOVER = "NO_CUTOVER"
    LEGACY_CONTINUES = "LEGACY_CONTINUES"
    LEGACY_RESTART_ALLOWED = "LEGACY_RESTART_ALLOWED"
    LEGACY_RESTART_REQUIRES_SCHEMA_CHECK = (
        "LEGACY_RESTART_REQUIRES_SCHEMA_CHECK"
    )
    FORWARD_RECOVERY_ONLY = "FORWARD_RECOVERY_ONLY"


@dataclass(frozen=True)
class RollbackDecision:
    phase: CutoverPhase
    disposition: RollbackDisposition
    legacy_restart_allowed: bool
    admission_may_reopen: bool
    guidance: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def rollback_decision(phase: CutoverPhase) -> RollbackDecision:
    """Return the safest action after a failed cutover step."""

    if phase is CutoverPhase.ADMISSION_OPEN:
        return RollbackDecision(
            phase=phase,
            disposition=RollbackDisposition.NO_CUTOVER,
            legacy_restart_allowed=True,
            admission_may_reopen=True,
            guidance="No cutover boundary has been crossed.",
        )
    if phase in {
        CutoverPhase.FREEZE_VERIFIED,
        CutoverPhase.LEGACY_DRAINED,
    }:
        return RollbackDecision(
            phase=phase,
            disposition=RollbackDisposition.LEGACY_CONTINUES,
            legacy_restart_allowed=True,
            admission_may_reopen=True,
            guidance=(
                "Keep the legacy release running, verify its readiness, "
                "then reopen START admission with the same freeze ID."
            ),
        )
    if phase is CutoverPhase.LEGACY_STOPPED:
        return RollbackDecision(
            phase=phase,
            disposition=RollbackDisposition.LEGACY_RESTART_ALLOWED,
            legacy_restart_allowed=True,
            admission_may_reopen=False,
            guidance=(
                "Restart the pinned legacy image, verify readiness and "
                "drain state, then explicitly reopen admission."
            ),
        )
    if phase is CutoverPhase.MIGRATION_APPLIED:
        return RollbackDecision(
            phase=phase,
            disposition=(
                RollbackDisposition.LEGACY_RESTART_REQUIRES_SCHEMA_CHECK
            ),
            legacy_restart_allowed=False,
            admission_may_reopen=False,
            guidance=(
                "Do not restart legacy until the pinned release passes "
                "the post-migration schema compatibility check."
            ),
        )
    return RollbackDecision(
        phase=phase,
        disposition=RollbackDisposition.FORWARD_RECOVERY_ONLY,
        legacy_restart_allowed=False,
        admission_may_reopen=(phase is CutoverPhase.ADMISSION_REOPENED),
        guidance=(
            "The target Worker may have consumed events or written "
            "session checkpoints. Keep legacy at zero and repair or roll "
            "forward with a compatible target image."
        ),
    )


__all__ = [
    "CutoverPhase",
    "RollbackDecision",
    "RollbackDisposition",
    "rollback_decision",
]
