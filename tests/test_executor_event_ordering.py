from uuid import UUID, uuid4

import pytest

from ex_agent.executor.contracts import ExecutorEvent
from ex_agent.worker import _merge_contiguous_events


def _event(
    execution_id: UUID,
    sequence: int,
    event_type: str = "execution.step_started",
) -> ExecutorEvent:
    return ExecutorEvent.model_validate(
        {
            "event_id": uuid4(),
            "event_type": event_type,
            "schema_version": "1.0",
            "execution_id": execution_id,
            "event_sequence": sequence,
            "payload": {},
            "occurred_at": "2026-08-27T00:00:00Z",
        }
    )


def test_gap_history_is_merged_in_execution_sequence() -> None:
    execution_id = uuid4()
    first = _event(execution_id, 1, "execution.started")
    second = _event(execution_id, 2, "execution.operation_started")
    current = _event(execution_id, 3)

    result = _merge_contiguous_events(
        current,
        [second, first],
        after_sequence=0,
    )

    assert [event.event_sequence for event in result] == [1, 2, 3]


def test_unclosed_gap_is_rejected() -> None:
    execution_id = uuid4()
    current = _event(execution_id, 3)

    with pytest.raises(ValueError, match="did not close"):
        _merge_contiguous_events(
            current,
            [_event(execution_id, 1)],
            after_sequence=0,
        )


def test_already_checkpointed_event_is_ignored() -> None:
    event = _event(uuid4(), 3)

    assert (
        _merge_contiguous_events(
            event,
            [],
            after_sequence=3,
        )
        == []
    )
