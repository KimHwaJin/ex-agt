from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import (
    CheckConstraint,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.sql import func


class StreamMaintenanceBase(DeclarativeBase):
    """Keep this table out of legacy baseline metadata creation."""


class StreamMaintenanceJob(StreamMaintenanceBase):
    __tablename__ = "agent_stream_maintenance_jobs"
    __mapper_args__ = {"eager_defaults": True}  # noqa: RUF012
    __table_args__ = (
        UniqueConstraint(
            "created_by",
            "idempotency_key",
            name="uq_agent_stream_maintenance_idempotency",
        ),
        CheckConstraint(
            "action IN ('PLAN','TRIM')",
            name="ck_agent_stream_maintenance_action",
        ),
        CheckConstraint(
            "state IN ('PENDING','RUNNING','SUCCEEDED','FAILED')",
            name="ck_agent_stream_maintenance_state",
        ),
        CheckConstraint(
            "retention_seconds > 0",
            name="ck_agent_stream_maintenance_retention",
        ),
        CheckConstraint(
            "minimum_retained_entries >= 0",
            name="ck_agent_stream_maintenance_minimum_entries",
        ),
        Index(
            "ix_agent_stream_maintenance_page",
            "created_at",
            "id",
        ),
        Index(
            "uq_agent_stream_maintenance_active_trim",
            "stream_key",
            unique=True,
            postgresql_where=text(
                "action = 'TRIM' AND state IN ('PENDING','RUNNING')"
            ),
        ),
        Index(
            "ix_agent_stream_maintenance_due",
            "next_attempt_at",
            "id",
            postgresql_where=text("state IN ('PENDING','RUNNING')"),
        ),
    )

    id: Mapped[UUID] = mapped_column(primary_key=True, default=uuid4)
    stream_alias: Mapped[str] = mapped_column(String(64))
    stream_key: Mapped[str] = mapped_column(String(255))
    action: Mapped[str] = mapped_column(String(16))
    state: Mapped[str] = mapped_column(String(16), default="PENDING")
    idempotency_key: Mapped[str] = mapped_column(String(255))
    request_hash: Mapped[str] = mapped_column(String(64))
    reason: Mapped[str] = mapped_column(Text)
    retention_seconds: Mapped[int] = mapped_column(Integer)
    minimum_retained_entries: Mapped[int] = mapped_column(Integer)
    attempts: Mapped[int] = mapped_column(Integer, default=0)
    next_attempt_at: Mapped[datetime] = mapped_column(DateTime(timezone=True))
    claimed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True)
    )
    result: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    last_error: Mapped[str | None] = mapped_column(Text)
    created_by: Mapped[str] = mapped_column(String(255))
    updated_by: Mapped[str] = mapped_column(String(255))
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


__all__ = ["StreamMaintenanceBase", "StreamMaintenanceJob"]
