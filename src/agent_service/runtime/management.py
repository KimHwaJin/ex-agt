"""Process-local pool and service; caller owns startup and shutdown."""

import asyncio
from dataclasses import dataclass

from langchain_core.language_models import BaseChatModel

from agent_service.agents.assistant import (
    build_assistant,
    build_model,
    close_model,
)
from agent_service.agents.intake import build_intake_agents
from agent_service.application.cursors import CursorCodec
from agent_service.application.management import ManagementService
from agent_service.application.runs import RunService
from agent_service.infrastructure.database.checkpoints import Checkpoints
from agent_service.infrastructure.database.management import (
    Pool,
    Store,
    create_pool,
)
from agent_service.runtime.demo_driver import DemoDriver
from agent_service.runtime.graph_driver import GraphDriver
from agent_service.settings import Settings


@dataclass
class ManagementRuntime:
    pool: Pool
    service: ManagementService
    runs: RunService
    settings: Settings
    worker: asyncio.Task | None = None
    checkpoints: Checkpoints | None = None
    driver: DemoDriver | GraphDriver | None = None
    model: BaseChatModel | None = None

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
        if self.settings.agent_backend == "langgraph":
            async with self.pool.connection() as connection:
                await connection.execute(
                    "SELECT graph_interrupt_id "
                    "FROM management.run_interrupts LIMIT 0"
                )
            self.checkpoints = Checkpoints(self.settings)
            await self.checkpoints.open()
            self.model = build_model(self.settings)
            router, planner = build_intake_agents(
                self.model, self.settings.context_message_limit
            )
            self.driver = GraphDriver(
                self.runs,
                self.checkpoints,
                build_assistant(self.model),
                router=router,
                planner=planner,
            )
        elif self.settings.agent_backend == "demo":
            self.driver = DemoDriver(self.runs)
        if self.settings.embedded_run_worker and self.driver:
            self.worker = asyncio.create_task(
                self.driver.serve(), name="run-worker"
            )

    async def close(self) -> None:
        try:
            if self.worker:
                self.worker.cancel()
                try:
                    await self.worker
                except asyncio.CancelledError:
                    pass
        finally:
            try:
                if self.model is not None:
                    await close_model(self.model)
            finally:
                try:
                    if self.checkpoints:
                        await self.checkpoints.close()
                finally:
                    await self.pool.close()

    async def ready(self):
        async with self.pool.connection() as connection:
            await connection.execute("SELECT 1")
        if self.checkpoints:
            async with self.checkpoints.connection() as connection:
                await connection.execute("SELECT 1 FROM checkpoints LIMIT 0")
        if self.settings.embedded_run_worker and (
            self.worker is None or self.worker.done()
        ):
            raise RuntimeError("Embedded worker is not running")
