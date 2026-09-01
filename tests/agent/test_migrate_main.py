import os
from contextlib import asynccontextmanager
from types import SimpleNamespace

from agent import migrate_main


async def test_deployment_migration_runs_all_three_schema_owners(
    monkeypatch,
):
    upgrades = []
    setup = []
    worker_database_urls = []

    def upgrade(config, revision):
        upgrades.append((config.config_file_name, revision))
        if config.config_file_name == "worker_migrations/alembic.ini":
            worker_database_urls.append(os.environ["EW_DATABASE_URL"])

    @asynccontextmanager
    async def saver(url):
        assert url == "postgresql://db/agent"
        yield SimpleNamespace(setup=lambda: _record_setup(setup))

    monkeypatch.setenv("EW_DATABASE_URL", "postgresql://stale/worker")
    monkeypatch.setattr(migrate_main.command, "upgrade", upgrade)
    monkeypatch.setattr(
        migrate_main,
        "Settings",
        lambda: SimpleNamespace(
            agent_checkpoint_database_url="postgresql://db/agent"
        ),
    )
    monkeypatch.setattr(
        migrate_main.AsyncPostgresSaver,
        "from_conn_string",
        saver,
    )

    await migrate_main.migrate()

    assert upgrades == [
        ("alembic.ini", "head"),
        ("worker_migrations/alembic.ini", "head"),
    ]
    assert setup == [True]
    assert worker_database_urls == ["postgresql://db/agent"]


async def _record_setup(calls):
    calls.append(True)
