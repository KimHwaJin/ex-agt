"""Standalone demo worker: python -m agent_service.worker_main."""

import asyncio
from pathlib import Path

from agent_service.bootstrap.configuration import load_settings
from agent_service.bootstrap.logging import initialize_logging
from agent_service.runtime.demo_driver import DemoDriver
from agent_service.runtime.management import ManagementRuntime


async def main() -> None:
    settings = load_settings(Path.cwd())
    if settings.agent_backend != "demo":
        raise RuntimeError("Only the explicitly enabled demo driver exists")
    settings = settings.model_copy(update={"embedded_run_worker": False})
    initialize_logging(settings)
    runtime = ManagementRuntime.build(settings)
    try:
        await runtime.start()
        await DemoDriver(runtime.runs).serve()
    finally:
        await runtime.close()


if __name__ == "__main__":
    asyncio.run(main())
