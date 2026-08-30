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
from ex_agent.persistence.models import Plan, PlanRevision, PlanStep


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
    ) -> PersistedPlan:
        payload = plan.model_dump(mode="json")
        payload_hash = _payload_hash(payload)
        compiled_bundle_id = uuid4()
        async with transaction(self._sessions) as session:
            plan_row = await session.scalar(
                select(Plan).where(Plan.task_id == task_id).with_for_update()
            )
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


__all__ = ["PlanRepository"]
