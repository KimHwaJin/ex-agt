"""Process-local pool and service; caller owns startup and shutdown."""

from dataclasses import dataclass

from agent_service.application.cursors import CursorCodec
from agent_service.application.management import ManagementService
from agent_service.infrastructure.database.management import (
    Pool,
    Store,
    create_pool,
)
from agent_service.settings import Settings


@dataclass
class ManagementRuntime:
    pool: Pool
    service: ManagementService

    @classmethod
    def build(cls, settings: Settings) -> "ManagementRuntime":
        pool = create_pool(settings)
        service = ManagementService(
            Store(pool), CursorCodec(settings.cursor_secret.get_secret_value())
        )
        return cls(pool=pool, service=service)

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

    async def close(self) -> None:
        await self.pool.close()
