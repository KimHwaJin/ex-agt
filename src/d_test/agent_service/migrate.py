"""Deployment entry point for app tables and checkpoint library migrations."""

import asyncio

from alembic import command
from alembic.config import Config

from d_test.agent_service.checkpoint_main import main as setup_checkpoints
from d_test.agent_service.settings import Settings


def main(settings: Settings | None = None):
    config = Config("alembic.ini")
    if settings is not None:
        config.attributes["agent_settings"] = settings
    command.upgrade(config, "head")
    asyncio.run(setup_checkpoints(settings))


if __name__ == "__main__":
    main()
