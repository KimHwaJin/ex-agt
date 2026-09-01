import os
from uuid import UUID

import pytest
from sqlalchemy import func, select

from agent.projections import TaskStateProjector
from ex_agent.domain.enums import TaskStatus
from ex_agent.persistence.models import Task, TaskEvent
from tests.agent.admission_support import admission_harness, snapshot

pytestmark = [
    pytest.mark.postgres,
    pytest.mark.skipif(
        "TEST_DATABASE_URL" not in os.environ,
        reason="Requires isolated PostgreSQL",
    ),
]


async def test_checkpoint_projection_records_interrupt_id_once(
    tmp_path,
    monkeypatch,
):
    async with admission_harness(tmp_path, monkeypatch) as h:
        projector = TaskStateProjector(h.sessions)
        h.host.snapshot_projector = projector

        assert (await h.host.handle(h.command)).state == "APPLIED"
        current = await snapshot(h)
        boundary = next(
            item for task in current.tasks for item in task.interrupts
        )
        task_id = UUID(h.task.active_task_id)
        async with h.sessions() as session:
            task = await session.get(Task, task_id)
            before = await session.scalar(
                select(func.count())
                .select_from(TaskEvent)
                .where(TaskEvent.task_id == task_id)
            )
            assert task.status == TaskStatus.WAITING_FOR_APPROVAL.value
            assert task.current_interrupt["interrupt_id"] == boundary.id
            assert task.current_interrupt["kind"] == "PLAN_REVIEW"

        await projector(current)

        async with h.sessions() as session:
            after = await session.scalar(
                select(func.count())
                .select_from(TaskEvent)
                .where(TaskEvent.task_id == task_id)
            )
        assert after == before
