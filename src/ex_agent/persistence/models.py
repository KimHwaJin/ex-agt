from __future__ import annotations

from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from pgvector.sqlalchemy import Vector
from sqlalchemy import (
    BigInteger,
    Boolean,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.dialects.postgresql import UUID as PGUUID
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class TimestampMixin:
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
    )


class Task(Base, TimestampMixin):
    __tablename__ = "agent_tasks"
    __table_args__ = (
        Index("ix_agent_tasks_session_created", "session_id", "created_at"),
    )

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    user_id: Mapped[str] = mapped_column(String(255), index=True)
    project_id: Mapped[str] = mapped_column(String(255), index=True)
    session_id: Mapped[str] = mapped_column(String(255), index=True)
    input_message_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True))
    user_message: Mapped[str] = mapped_column(Text)
    status: Mapped[str] = mapped_column(String(64), index=True)
    execution_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        nullable=True,
        index=True,
    )
    current_interrupt: Mapped[dict[str, Any] | None] = mapped_column(
        JSONB,
        nullable=True,
    )
    terminal_message: Mapped[str | None] = mapped_column(Text, nullable=True)
    version: Mapped[int] = mapped_column(Integer, default=1)


class Message(Base, TimestampMixin):
    __tablename__ = "agent_messages"

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    task_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_tasks.id", ondelete="CASCADE"),
        index=True,
    )
    role: Mapped[str] = mapped_column(String(32))
    content: Mapped[str] = mapped_column(Text)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        default=dict,
    )


class TaskEvent(Base):
    __tablename__ = "agent_task_events"
    __table_args__ = (
        Index("ix_agent_task_events_task_id_id", "task_id", "id"),
        Index(
            "ix_agent_task_events_delivery_state_id",
            "delivery_state",
            "id",
        ),
    )

    id: Mapped[int] = mapped_column(
        BigInteger,
        primary_key=True,
        autoincrement=True,
    )
    task_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_tasks.id", ondelete="CASCADE"),
    )
    event_type: Mapped[str] = mapped_column(String(128), index=True)
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    delivery_state: Mapped[str] = mapped_column(
        String(32),
        default="PENDING",
    )
    delivery_attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    delivery_last_error: Mapped[str | None] = mapped_column(
        Text,
        nullable=True,
    )
    delivery_claimed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        index=True,
    )


class SessionLock(Base, TimestampMixin):
    __tablename__ = "agent_session_locks"

    session_id: Mapped[str] = mapped_column(String(255), primary_key=True)
    active_task_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_tasks.id", ondelete="CASCADE"),
        unique=True,
    )
    execution_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        nullable=True,
    )
    locked: Mapped[bool] = mapped_column(Boolean, default=True)


class WorkflowCommand(Base, TimestampMixin):
    __tablename__ = "agent_workflow_commands"
    __table_args__ = (
        UniqueConstraint("idempotency_key", name="uq_agent_command_idem"),
        Index("ix_agent_commands_state_created", "state", "created_at"),
    )

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    task_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_tasks.id", ondelete="CASCADE"),
        index=True,
    )
    command_type: Mapped[str] = mapped_column(String(64))
    idempotency_key: Mapped[str] = mapped_column(String(255))
    payload: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    state: Mapped[str] = mapped_column(String(32), default="PENDING")
    attempt_count: Mapped[int] = mapped_column(Integer, default=0)
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    publish_claimed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True),
        nullable=True,
    )


class StreamInbox(Base):
    __tablename__ = "agent_stream_inbox"
    __table_args__ = (
        UniqueConstraint(
            "stream_name",
            "message_id",
            name="uq_agent_stream_message",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    stream_name: Mapped[str] = mapped_column(String(255))
    message_id: Mapped[str] = mapped_column(String(128))
    received_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )


class Plan(Base, TimestampMixin):
    __tablename__ = "agent_plans"

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    task_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_tasks.id", ondelete="CASCADE"),
        unique=True,
    )
    current_revision: Mapped[int] = mapped_column(Integer, default=1)


class PlanRevision(Base, TimestampMixin):
    __tablename__ = "agent_plan_revisions"
    __table_args__ = (
        UniqueConstraint(
            "plan_id",
            "revision_number",
            name="uq_agent_plan_revision_number",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    plan_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_plans.id", ondelete="CASCADE"),
        index=True,
    )
    revision_number: Mapped[int] = mapped_column(Integer)
    public_payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    public_payload_hash: Mapped[str] = mapped_column(String(64))
    compiled_bundle_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True))
    registry_snapshot_hash: Mapped[str] = mapped_column(String(64))
    feedback: Mapped[str | None] = mapped_column(Text, nullable=True)


class PlanStep(Base):
    __tablename__ = "agent_plan_steps"
    __table_args__ = (
        UniqueConstraint(
            "plan_revision_id",
            "sequence",
            name="uq_agent_plan_step_sequence",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    plan_revision_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_plan_revisions.id", ondelete="CASCADE"),
        index=True,
    )
    sequence: Mapped[int] = mapped_column(Integer)
    title: Mapped[str] = mapped_column(String(255))
    purpose: Mapped[str] = mapped_column(Text)
    selection_rationale: Mapped[str] = mapped_column(Text)
    skill_ref: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    tool_ref: Mapped[dict[str, Any] | None] = mapped_column(JSONB)
    parameters: Mapped[dict[str, Any]] = mapped_column(JSONB, default=dict)
    compiled_source_sha256: Mapped[str] = mapped_column(String(64))
    compiled_source_path: Mapped[str] = mapped_column(Text)
    timeout_seconds: Mapped[int] = mapped_column(Integer)


class ExecutorBinding(Base, TimestampMixin):
    __tablename__ = "agent_executor_bindings"

    task_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_tasks.id", ondelete="CASCADE"),
        primary_key=True,
    )
    execution_id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        unique=True,
        index=True,
    )
    operation_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True))
    execution_version: Mapped[int] = mapped_column(Integer, default=0)
    next_step_sequence: Mapped[int] = mapped_column(Integer, default=0)
    last_event_sequence: Mapped[int] = mapped_column(Integer, default=0)


class Workflow(Base, TimestampMixin):
    __tablename__ = "agent_workflows"

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    name: Mapped[str] = mapped_column(String(255), unique=True)
    description: Mapped[str] = mapped_column(Text)
    owner_user_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        index=True,
    )
    owner_project_id: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
        index=True,
    )
    visibility: Mapped[str] = mapped_column(String(32), default="SERVICE")
    status: Mapped[str] = mapped_column(String(32), default="ACTIVE")
    latest_version: Mapped[int] = mapped_column(Integer, default=1)
    access_policy: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        default=dict,
    )
    required_permission: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )


class WorkflowVersion(Base, TimestampMixin):
    __tablename__ = "agent_workflow_versions"
    __table_args__ = (
        UniqueConstraint(
            "workflow_id",
            "version",
            name="uq_agent_workflow_version",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    workflow_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_workflows.id", ondelete="CASCADE"),
        index=True,
    )
    version: Mapped[int] = mapped_column(Integer)
    source_task_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        nullable=True,
        index=True,
    )
    source_plan_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        nullable=True,
    )
    source_plan_revision_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        nullable=True,
    )
    source_execution_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        nullable=True,
    )
    objective: Mapped[str | None] = mapped_column(Text, nullable=True)
    strategy_summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    runtime_profile: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
    )
    input_contract: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        default=dict,
    )
    output_contract: Mapped[dict[str, Any]] = mapped_column(
        JSONB,
        default=dict,
    )
    tool_registry_snapshot_hash: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    searchable_text: Mapped[str | None] = mapped_column(Text, nullable=True)
    searchable_text_hash: Mapped[str | None] = mapped_column(
        String(64),
        nullable=True,
    )
    embedding_model: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )
    embedding_dimension: Mapped[int | None] = mapped_column(
        Integer,
        nullable=True,
    )
    request_examples: Mapped[list[str]] = mapped_column(JSONB, default=list)
    tags: Mapped[list[str]] = mapped_column(JSONB, default=list)
    promotion_policy_version: Mapped[str | None] = mapped_column(
        String(128),
        nullable=True,
    )
    plan_payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    public_payload_hash: Mapped[str] = mapped_column(String(64))
    embedding: Mapped[list[float] | None] = mapped_column(
        Vector(1024),
        nullable=True,
    )
    promoted_by: Mapped[str] = mapped_column(String(255))
    active: Mapped[bool] = mapped_column(Boolean, default=True)


class WorkflowStep(Base):
    __tablename__ = "agent_workflow_steps"
    __table_args__ = (
        UniqueConstraint(
            "workflow_version_id",
            "sequence",
            name="uq_agent_workflow_step_sequence",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    workflow_version_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_workflow_versions.id", ondelete="CASCADE"),
        index=True,
    )
    source_plan_revision_id: Mapped[UUID | None] = mapped_column(
        PGUUID(as_uuid=True),
        nullable=True,
    )
    sequence: Mapped[int] = mapped_column(Integer)
    skill_ref: Mapped[dict[str, Any]] = mapped_column(JSONB)
    tool_ref: Mapped[dict[str, Any]] = mapped_column(JSONB)
    purpose: Mapped[str] = mapped_column(Text)
    selection_rationale: Mapped[str] = mapped_column(Text)
    parameter_template: Mapped[dict[str, Any]] = mapped_column(JSONB)
    expected_outputs: Mapped[list[str]] = mapped_column(JSONB, default=list)
    validation_criteria: Mapped[list[str]] = mapped_column(
        JSONB,
        default=list,
    )
    timeout_seconds: Mapped[int] = mapped_column(Integer)


class SuccessfulExecutionStep(Base, TimestampMixin):
    __tablename__ = "agent_successful_execution_steps"
    __table_args__ = (
        UniqueConstraint(
            "task_id",
            "execution_sequence",
            name="uq_agent_successful_execution_step",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    task_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_tasks.id", ondelete="CASCADE"),
        index=True,
    )
    execution_sequence: Mapped[int] = mapped_column(Integer)
    operation_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True))
    source_plan_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True))
    source_plan_revision_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True))
    registry_snapshot_hash: Mapped[str] = mapped_column(String(64))
    step_payload: Mapped[dict[str, Any]] = mapped_column(JSONB)
    step_payload_hash: Mapped[str] = mapped_column(String(64))


class WorkflowPromotion(Base, TimestampMixin):
    __tablename__ = "agent_workflow_promotions"
    __table_args__ = (
        UniqueConstraint(
            "idempotency_key",
            name="uq_agent_workflow_promotion_idem",
        ),
    )

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    task_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_tasks.id", ondelete="CASCADE"),
        index=True,
    )
    actor_user_id: Mapped[str] = mapped_column(String(255))
    idempotency_key: Mapped[str] = mapped_column(String(255))
    request_hash: Mapped[str] = mapped_column(String(64))
    workflow_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_workflows.id", ondelete="RESTRICT"),
    )
    workflow_version_id: Mapped[UUID] = mapped_column(
        ForeignKey("agent_workflow_versions.id", ondelete="RESTRICT"),
    )
    policy_version: Mapped[str] = mapped_column(String(128))


class ModelCallAudit(Base):
    __tablename__ = "agent_model_call_audits"

    id: Mapped[UUID] = mapped_column(
        PGUUID(as_uuid=True),
        primary_key=True,
        default=uuid4,
    )
    task_id: Mapped[UUID] = mapped_column(PGUUID(as_uuid=True), index=True)
    component: Mapped[str] = mapped_column(String(128))
    duration_ms: Mapped[int] = mapped_column(Integer)
    succeeded: Mapped[bool] = mapped_column(Boolean)
    metadata_json: Mapped[dict[str, Any]] = mapped_column(
        "metadata",
        JSONB,
        default=dict,
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
    )
