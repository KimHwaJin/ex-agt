"""Deployment entry point for app tables and checkpoint library migrations."""

import asyncio

from alembic import command
from alembic.config import Config

from agent_service.checkpoint_main import main as setup_checkpoints


def main():
    command.upgrade(Config("alembic.ini"), "head")
    asyncio.run(setup_checkpoints())


if __name__ == "__main__":
    main()
