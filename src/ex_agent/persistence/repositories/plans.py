from __future__ import annotations

import hashlib
import json
from collections.abc import Sequence
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ex_agent.domain.contracts import CompiledStep, PersistedPlan, PlanDraft
from ex_agent.persistence.database import transaction
from ex_agent.persistence.models import Plan, PlanRevision, PlanStep, Task


class PlanRepository:
    """Versioned public plans and immutable compiled step metadata."""

    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
    ) -> None:
        self._sessions = sessions

    async def persist(
        self,
        task_id: UUID,
        plan: PlanDraft,
        compiled: list[tuple[CompiledStep, str]],
        registry_snapshot_hash: str,
        feedback: str | None,
        *,
        expected_revision_number: int | None = None,
    ) -> PersistedPlan:
        payload = plan.model_dump(mode="json")
        payload_hash = _payload_hash(payload)
        compiled_bundle_id = uuid4()
        async with transaction(self._sessions) as session:
            if expected_revision_number is not None:
                task = await session.get(Task, task_id, with_for_update=True)
                if task is None:
                    raise LookupError("Unknown plan Task")
            plan_row = await session.scalar(
                select(Plan).where(Plan.task_id == task_id).with_for_update()
            )
            if expected_revision_number is not None:
                existing = await _existing_revision(
                    session,
                    plan_row,
                    expected_revision_number,
                    payload_hash,
                    registry_snapshot_hash,
                    feedback,
                    compiled,
                )
                if existing is not None:
                    return existing
            if plan_row is None:
                plan_row = Plan(task_id=task_id, current_revision=1)
                session.add(plan_row)
                await session.flush()
                revision_number = 1
            else:
                revision_number = plan_row.current_revision + 1
                plan_row.current_revision = revision_number
            revision = PlanRevision(
                plan_id=plan_row.id,
                revision_number=revision_number,
                public_payload=payload,
                public_payload_hash=payload_hash,
                compiled_bundle_id=compiled_bundle_id,
                registry_snapshot_hash=registry_snapshot_hash,
                feedback=feedback,
            )
            session.add(revision)
            await session.flush()
            for step, path in compiled:
                draft = plan.steps[step.sequence]
                session.add(
                    PlanStep(
                        plan_revision_id=revision.id,
                        sequence=step.sequence,
                        title=draft.title,
                        purpose=draft.purpose,
                        selection_rationale=draft.selection_rationale,
                        skill_ref=(
                            draft.skill.model_dump(mode="json")
                            if draft.skill
                            else None
                        ),
                        tool_ref=(
                            draft.tool.model_dump(mode="json")
                            if draft.tool
                            else None
                        ),
                        parameters=draft.parameters,
                        compiled_source_sha256=step.source_sha256,
                        compiled_source_path=path,
                        timeout_seconds=draft.timeout_seconds,
                    )
                )
        return PersistedPlan(
            plan_id=plan_row.id,
            plan_revision_id=revision.id,
            plan_revision_number=revision_number,
            public_payload_hash=payload_hash,
            compiled_bundle_id=compiled_bundle_id,
        )

    async def approved_steps(
        self,
        revision_id: UUID,
    ) -> Sequence[PlanStep]:
        async with self._sessions() as session:
            result = await session.scalars(
                select(PlanStep)
                .where(PlanStep.plan_revision_id == revision_id)
                .order_by(PlanStep.sequence)
            )
            return result.all()


def _payload_hash(payload: dict[str, Any]) -> str:
    value = json.dumps(
        payload,
        sort_keys=True,
        ensure_ascii=False,
        separators=(",", ":"),
    )
    return hashlib.sha256(value.encode()).hexdigest()


async def _existing_revision(
    session: AsyncSession,
    plan: Plan | None,
    number: int,
    payload_hash: str,
    registry_hash: str,
    feedback: str | None,
    compiled: list[tuple[CompiledStep, str]],
) -> PersistedPlan | None:
    revision = (
        None
        if plan is None
        else await session.scalar(
            select(PlanRevision).where(
                PlanRevision.plan_id == plan.id,
                PlanRevision.revision_number == number,
            )
        )
    )
    if revision is None:
        expected = 1 if plan is None else plan.current_revision + 1
        if number != expected:
            raise ValueError("Plan revision must advance exactly once")
        return None
    rows = await session.scalars(
        select(PlanStep).where(PlanStep.plan_revision_id == revision.id)
    )
    actual = sorted(
        (row.sequence, row.compiled_source_sha256, row.compiled_source_path)
        for row in rows
    )
    requested = sorted(
        (step.sequence, step.source_sha256, path) for step, path in compiled
    )
    if (
        revision.public_payload_hash != payload_hash
        or revision.registry_snapshot_hash != registry_hash
        or revision.feedback != feedback
        or actual != requested
    ):
        raise ValueError("Plan revision was reused with different input")
    return PersistedPlan(
        plan_id=revision.plan_id,
        plan_revision_id=revision.id,
        plan_revision_number=revision.revision_number,
        public_payload_hash=revision.public_payload_hash,
        compiled_bundle_id=revision.compiled_bundle_id,
    )


__all__ = ["PlanRepository"]
