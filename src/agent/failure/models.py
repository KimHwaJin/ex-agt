from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import Boolean, DateTime, ForeignKey, Index, String, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from ex_agent.persistence.models import Task, TimestampMixin


class FailureBase(DeclarativeBase):
    """Keep future tables out of the legacy baseline create_all."""


class FailureCleanup(FailureBase, TimestampMixin):
    __tablename__ = "agent_failure_cleanups"
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012
    __table_args__ = (
        Index(
            "ix_agent_failure_due",
            "next_attempt_at",
            "task_id",
            postgresql_where=text("state = 'PENDING'"),
        ),
    )
    task_id: Mapped[UUID] = mapped_column(
        ForeignKey(Task.__table__.c.id, ondelete="CASCADE"),
        primary_key=True,
    )
    session_id: Mapped[str] = mapped_column(String(255))
    turn: Mapped[dict[str, Any]] = mapped_column(JSONB)
    source: Mapped[dict[str, Any]] = mapped_column(JSONB)
    reason: Mapped[str] = mapped_column(Text)
    state: Mapped[str] = mapped_column(String(32), default="PENDING")
    attempts: Mapped[int] = mapped_column(default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    execution_id: Mapped[UUID | None]
    executor_status: Mapped[str | None] = mapped_column(String(32))
    preserve_terminal: Mapped[bool] = mapped_column(Boolean)
    final_status: Mapped[str] = mapped_column(String(64))
    message: Mapped[str | None] = mapped_column(Text)
    created_by: Mapped[str] = mapped_column(String(255), default="AGENT")
    updated_by: Mapped[str] = mapped_column(String(255), default="AGENT")
