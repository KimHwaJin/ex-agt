import os
from uuid import uuid4

import pytest
from psycopg import AsyncConnection

from agent.cutover import CutoverProbe, UnsafeStaticAdmissionFreezeProbe

pytestmark = [pytest.mark.postgres, pytest.mark.redis]


@pytest.mark.skipif(
    not {"TEST_DATABASE_URL", "TEST_REDIS_URL"}.issubset(os.environ),
    reason="Requires isolated PostgreSQL and Redis",
)
@pytest.mark.asyncio
async def test_probe_reads_real_legacy_tables_and_stream_groups() -> None:
    database_url = os.environ["TEST_DATABASE_URL"]
    redis_url = os.environ["TEST_REDIS_URL"]
    suffix = str(uuid4())
    task_id = uuid4()
    message_id = uuid4()
    command_id = uuid4()
    session_id = f"cutover-{suffix}"
    command_stream = f"cutover-commands-{suffix}"
    command_group = f"cutover-command-group-{suffix}"
    event_stream = f"cutover-events-{suffix}"
    event_group = f"cutover-event-group-{suffix}"
    probe = CutoverProbe(
        database_url=database_url,
        redis_url=redis_url,
        command_stream=command_stream,
        command_group=command_group,
        executor_event_stream=event_stream,
        executor_event_group=event_group,
        admission_probe=UnsafeStaticAdmissionFreezeProbe(),
    )
    try:
        baseline = await probe._database_counts()
        for stream, group in (
            (command_stream, command_group),
            (event_stream, event_group),
        ):
            await probe.redis.xadd(stream, {"test": "value"})
            await probe.redis.xgroup_create(
                stream, group, id="$", mkstream=True
            )
        async with await AsyncConnection.connect(
            database_url.replace(
                "postgresql+psycopg://",
                "postgresql://",
                1,
            )
        ) as conn:
            await conn.execute(
                """INSERT INTO agent_tasks
                (id,user_id,project_id,session_id,input_message_id,
                 user_message,status,version,created_by,updated_by)
                VALUES (%s,'user','project',%s,%s,'test','EXECUTING',
                        1,'test','test')""",
                (task_id, session_id, message_id),
            )
            await conn.execute(
                """INSERT INTO agent_workflow_commands
                (id,task_id,command_type,idempotency_key,payload,state,
                 attempt_count)
                VALUES (%s,%s,'START',%s,'{}','PENDING',0)""",
                (command_id, task_id, f"cutover-{suffix}"),
            )
            await conn.execute(
                """INSERT INTO agent_task_events
                (task_id,event_type,payload,delivery_state,
                 delivery_attempt_count)
                VALUES (%s,'test','{}','PENDING',0)""",
                (task_id,),
            )
            await conn.execute(
                """INSERT INTO agent_session_locks
                (session_id,active_task_id,locked)
                VALUES (%s,%s,true)""",
                (session_id, task_id),
            )
        observed = await probe.snapshot()

        assert (
            observed.active_tasks,
            observed.unfinished_commands,
            observed.unpublished_product_events,
            observed.locked_sessions,
        ) == tuple(value + 1 for value in baseline)
        assert observed.command_group.pending == 0
        assert observed.command_group.lag == 0
        assert observed.executor_event_group.pending == 0
        assert observed.executor_event_group.lag == 0
    finally:
        async with await AsyncConnection.connect(
            database_url.replace(
                "postgresql+psycopg://",
                "postgresql://",
                1,
            )
        ) as conn:
            await conn.execute(
                "DELETE FROM agent_tasks WHERE id=%s",
                (task_id,),
            )
        await probe.redis.delete(command_stream, event_stream)
        await probe.close()
