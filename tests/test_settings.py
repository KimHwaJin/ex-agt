import pytest
from pydantic import ValidationError

from ex_agent.config import Settings


def test_lock_renewal_is_faster_than_stream_reclaim() -> None:
    with pytest.raises(ValidationError, match="command claim idle"):
        Settings(
            task_lock_renew_interval_seconds=30,
            command_claim_idle_milliseconds=30000,
        )


def test_outbox_active_poll_does_not_exceed_idle_poll() -> None:
    with pytest.raises(ValidationError, match="outbox_poll_milliseconds"):
        Settings(
            outbox_poll_milliseconds=1000,
            outbox_idle_max_milliseconds=500,
        )
