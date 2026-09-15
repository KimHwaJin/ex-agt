"""Checkpoint setup: python -m d_test.agent_service.checkpoint_main."""

from pathlib import Path

from d_test.agent_service.bootstrap.configuration import load_settings
from d_test.agent_service.bootstrap.event_loop import run_async
from d_test.agent_service.infrastructure.database.checkpoints import (
    Checkpoints,
)
from d_test.agent_service.settings import Settings


async def main(settings: Settings | None = None):
    checkpoints = Checkpoints(settings or load_settings(Path.cwd()))
    try:
        await checkpoints.open(validate=False)
        await checkpoints.setup()
    finally:
        await checkpoints.close()


if __name__ == "__main__":
    run_async(main())
