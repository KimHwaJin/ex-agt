from typing import Any
from uuid import UUID

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from ex_agent.persistence.database import transaction
from ex_agent.persistence.models import ModelCallAudit


class AuditRepository:
    def __init__(
        self,
        sessions: async_sessionmaker[AsyncSession],
    ) -> None:
        self._sessions = sessions

    async def record_model_call(
        self,
        *,
        task_id: str,
        component: str,
        duration_ms: int,
        succeeded: bool,
        metadata: dict[str, Any],
    ) -> None:
        async with transaction(self._sessions) as session:
            session.add(
                ModelCallAudit(
                    task_id=UUID(task_id),
                    component=component,
                    duration_ms=duration_ms,
                    succeeded=succeeded,
                    metadata_json=metadata,
                )
            )


__all__ = ["AuditRepository"]
