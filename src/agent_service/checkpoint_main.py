"""Explicit checkpoint migration: python -m agent_service.checkpoint_main."""

import asyncio
from pathlib import Path

from agent_service.bootstrap.configuration import load_settings
from agent_service.infrastructure.database.checkpoints import Checkpoints
from agent_service.settings import Settings


async def main(settings: Settings | None = None):
    checkpoints = Checkpoints(settings or load_settings(Path.cwd()))
    try:
        await checkpoints.open(validate=False)
        await checkpoints.setup()
    finally:
        await checkpoints.close()


if __name__ == "__main__":
    asyncio.run(main())
