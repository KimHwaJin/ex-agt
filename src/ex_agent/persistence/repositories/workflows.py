from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ex_agent.domain.contracts import PlanDraft, WorkflowCandidate
from ex_agent.persistence.models import Workflow, WorkflowVersion


class WorkflowCatalogRepository:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
    ) -> None:
        self._sessions = sessions

    async def candidates(
        self,
        embedding: list[float],
        limit: int = 3,
    ) -> list[WorkflowCandidate]:
        distance = WorkflowVersion.embedding.cosine_distance(embedding)
        async with self._sessions() as session:
            rows = (
                await session.execute(
                    select(
                        WorkflowVersion, Workflow, distance.label("distance")
                    )
                    .join(Workflow, Workflow.id == WorkflowVersion.workflow_id)
                    .where(
                        WorkflowVersion.active.is_(True),
                        WorkflowVersion.review_status == "APPROVED",
                        WorkflowVersion.embedding.is_not(None),
                        Workflow.status == "ACTIVE",
                        Workflow.visibility == "SERVICE",
                    )
                    .order_by(distance)
                    .limit(limit)
                )
            ).all()
        return [
            WorkflowCandidate(
                workflow_version_id=version.id,
                name=workflow.name,
                description=workflow.description,
                score=max(0.0, 1.0 - float(distance_value)),
                plan=PlanDraft.model_validate(version.plan_payload),
                input_contract=version.input_contract,
                public_payload_hash=version.public_payload_hash,
            )
            for version, workflow, distance_value in rows
        ]

    async def version(self, version_id: UUID) -> WorkflowVersion:
        async with self._sessions() as session:
            version = await session.scalar(
                select(WorkflowVersion)
                .join(Workflow, Workflow.id == WorkflowVersion.workflow_id)
                .where(
                    WorkflowVersion.id == version_id,
                    WorkflowVersion.active.is_(True),
                    WorkflowVersion.review_status == "APPROVED",
                    Workflow.status == "ACTIVE",
                    Workflow.visibility == "SERVICE",
                )
            )
            if version is None:
                raise LookupError(f"Unknown Workflow version: {version_id}")
            return version


__all__ = ["WorkflowCatalogRepository"]
