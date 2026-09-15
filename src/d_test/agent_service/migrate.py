"""Deployment entry point for app tables and checkpoint library migrations."""

from alembic import command
from alembic.config import Config

from d_test.agent_service.bootstrap.event_loop import run_async
from d_test.agent_service.checkpoint_main import main as setup_checkpoints
from d_test.agent_service.settings import Settings


def main(settings: Settings | None = None):
    config = Config(
        settings.database_migration_config if settings else "alembic.ini"
    )
    if settings is not None:
        config.attributes["agent_settings"] = settings
    command.upgrade(config, "head")
    run_async(setup_checkpoints(settings))


if __name__ == "__main__":
    main()
