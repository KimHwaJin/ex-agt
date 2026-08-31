from typing import Any
from uuid import UUID

from sqlalchemy import ForeignKey, String
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from ex_agent.persistence.models import Task, TimestampMixin


class EffectBase(DeclarativeBase):
    """Keep new schema out of the legacy baseline's create_all metadata."""


class ExecutorEffect(EffectBase, TimestampMixin):
    __tablename__ = "agent_executor_effects"

    key: Mapped[str] = mapped_column(String(255), primary_key=True)
    task_id: Mapped[UUID] = mapped_column(
        ForeignKey(Task.__table__.c.id, ondelete="CASCADE"), index=True
    )
    kind: Mapped[str] = mapped_column(String(32))
    input_sha256: Mapped[str] = mapped_column(String(64))
    request_sha256: Mapped[str] = mapped_column(String(64))
    request: Mapped[dict[str, Any]] = mapped_column(JSONB)
    response: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB, nullable=True
    )
    created_by: Mapped[str] = mapped_column(String(255), default="AGENT")
    updated_by: Mapped[str] = mapped_column(String(255), default="AGENT")
