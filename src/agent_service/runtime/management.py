"""Process-local pool and service; caller owns startup and shutdown."""

import asyncio
from dataclasses import dataclass

from agent_service.application.cursors import CursorCodec
from agent_service.application.management import ManagementService
from agent_service.application.runs import RunService
from agent_service.infrastructure.database.management import (
    Pool,
    Store,
    create_pool,
)
from agent_service.runtime.demo_driver import DemoDriver
from agent_service.settings import Settings


@dataclass
class ManagementRuntime:
    pool: Pool
    service: ManagementService
    runs: RunService
    settings: Settings
    worker: asyncio.Task | None = None

    @classmethod
    def build(cls, settings: Settings) -> "ManagementRuntime":
        pool = create_pool(settings)
        service = ManagementService(
            Store(pool), CursorCodec(settings.cursor_secret.get_secret_value())
        )
        runs = RunService(
            Store(pool),
            CursorCodec(settings.cursor_secret.get_secret_value()),
            settings,
        )
        return cls(pool=pool, service=service, runs=runs, settings=settings)

    async def start(self) -> None:
        await self.pool.open(wait=True)
        async with self.pool.connection() as connection:
            await connection.execute("SELECT 1 FROM management.users LIMIT 0")
            await connection.execute(
                "SELECT 1 FROM management.projects LIMIT 0"
            )
            await connection.execute(
                "SELECT 1 FROM management.sessions LIMIT 0"
            )
            await connection.execute(
                "SELECT 1 FROM management.requests LIMIT 0"
            )
            await connection.execute(
                "SELECT 1 FROM management.run_events LIMIT 0"
            )
            await connection.execute(
                "SELECT event_sequence FROM management.messages LIMIT 0"
            )
        if self.settings.embedded_run_worker:
            self.worker = asyncio.create_task(
                DemoDriver(self.runs).serve(), name="demo-run-worker"
            )

    async def close(self) -> None:
        if self.worker:
            self.worker.cancel()
            try:
                await self.worker
            except asyncio.CancelledError:
                pass
        await self.pool.close()
