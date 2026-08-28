from uuid import UUID

import pytest

from ex_agent.worker import _autoclaim_entries, _task_graph_config


def test_task_graph_config_isolates_tasks_within_session() -> None:
    first_task_id = UUID("11111111-1111-4111-8111-111111111111")
    second_task_id = UUID("22222222-2222-4222-8222-222222222222")

    first = _task_graph_config(first_task_id)
    second = _task_graph_config(second_task_id)

    assert first["configurable"]["thread_id"] == str(first_task_id)
    assert second["configurable"]["thread_id"] == str(second_task_id)


def test_autoclaim_entries_reads_redis_response() -> None:
    entries = [("1-0", {"command_id": "command-1"})]

    assert _autoclaim_entries(["0-0", entries, []]) == entries


def test_autoclaim_entries_rejects_invalid_response() -> None:
    with pytest.raises(TypeError, match="invalid response"):
        _autoclaim_entries([])
