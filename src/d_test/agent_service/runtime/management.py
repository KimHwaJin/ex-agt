"""Process-local pool and service; caller owns startup and shutdown."""

import asyncio
from dataclasses import dataclass, field

from langchain_core.language_models import BaseChatModel

from d_test.agent_service.agents.assistant import (
    build_assistant,
    build_model,
    close_model,
)
from d_test.agent_service.agents.code_planner import build_code_planners
from d_test.agent_service.agents.intake import build_intake_agents
from d_test.agent_service.application.cursors import CursorCodec
from d_test.agent_service.application.management import ManagementService
from d_test.agent_service.application.runs import RunService
from d_test.agent_service.infrastructure.database.checkpoints import (
    Checkpoints,
)
from d_test.agent_service.infrastructure.database.management import (
    Pool,
    Store,
    create_pool,
)
from d_test.agent_service.runtime.demo_driver import DemoDriver
from d_test.agent_service.runtime.graph_driver import GraphDriver
from d_test.agent_service.settings import Settings


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
    models: list[BaseChatModel] = field(default_factory=list)

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
                await connection.execute(
                    "SELECT 1 FROM management.execution_plans LIMIT 0"
                )
            self.checkpoints = Checkpoints(self.settings)
            await self.checkpoints.open()
            bundles = {}
            for name in self.settings.selectable_models:
                configured = self.settings.model_copy(
                    update={"model_name": name}
                )
                model = build_model(configured)
                self.models.append(model)
                router, planner = build_intake_agents(
                    model, self.settings.context_message_limit
                )
                bundles[name] = (
                    build_assistant(model),
                    router,
                    planner,
                    await asyncio.to_thread(
                        build_code_planners,
                        model,
                        self.settings.context_message_limit,
                        self.settings.code_plan_max_tokens,
                    ),
                )
            self.model = self.models[0]
            agent, router, planner, code_planners = bundles[
                self.settings.selectable_models[0]
            ]
            self.driver = GraphDriver(
                self.runs,
                self.checkpoints,
                agent,
                router=router,
                planner=planner,
                code_planners=code_planners,
                model_agents=bundles,
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
                results = await asyncio.gather(
                    *(close_model(model) for model in self.models),
                    return_exceptions=True,
                )
                self.models.clear()
                for result in results:
                    if isinstance(result, BaseException):
                        raise result
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
