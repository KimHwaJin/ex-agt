from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import DateTime, ForeignKey, Index, Integer, String, Text, text
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column

from ex_agent.persistence.models import Task, TimestampMixin


class AdmissionBase(DeclarativeBase):
    """Do not let the legacy baseline create this future schema."""


class ApiRequestRow(AdmissionBase, TimestampMixin):
    __tablename__ = "agent_api_requests"
    # SQLAlchemy declares this configuration mapping as an instance attribute.
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012
    __table_args__ = (
        Index(
            "uq_agent_api_active_session",
            "session_id",
            unique=True,
            postgresql_where=text("state IN ('PENDING','RUNNING','BLOCKED')"),
        ),
        Index(
            "ix_agent_api_due",
            "next_attempt_at",
            "request_id",
            postgresql_where=text("state IN ('PENDING','RUNNING')"),
        ),
    )
    request_id: Mapped[UUID] = mapped_column(primary_key=True)
    task_id: Mapped[UUID] = mapped_column(
        ForeignKey(Task.__table__.c.id, ondelete="CASCADE"),
        index=True,
    )
    session_id: Mapped[str] = mapped_column(String(255))
    command: Mapped[dict[str, Any]] = mapped_column(JSONB)
    fingerprint: Mapped[str] = mapped_column(String(64))
    target_node: Mapped[str] = mapped_column(String(64))
    base_checkpoint_id: Mapped[str | None] = mapped_column(String(255))
    state: Mapped[str] = mapped_column(String(32), default="PENDING")
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    last_error: Mapped[str | None] = mapped_column(Text)
    created_by: Mapped[str] = mapped_column(String(255))
    updated_by: Mapped[str] = mapped_column(String(255))
