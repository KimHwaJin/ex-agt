"""Explicit checkpoint migration: python -m agent_service.checkpoint_main."""

import asyncio
from pathlib import Path

from agent_service.bootstrap.configuration import load_settings
from agent_service.infrastructure.database.checkpoints import Checkpoints


async def main():
    checkpoints = Checkpoints(load_settings(Path.cwd()))
    try:
        await checkpoints.open(validate=False)
        await checkpoints.setup()
    finally:
        await checkpoints.close()


if __name__ == "__main__":
    asyncio.run(main())
