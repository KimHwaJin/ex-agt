"""Standalone worker: python -m agent_service.worker_main."""

import asyncio
import signal
from pathlib import Path

from agent_service.bootstrap.configuration import load_settings
from agent_service.bootstrap.logging import initialize_logging
from agent_service.runtime.management import ManagementRuntime


async def main() -> None:
    settings = load_settings(Path.cwd())
    if settings.agent_backend == "disabled":
        raise RuntimeError("No agent backend configured")
    settings = settings.model_copy(update={"embedded_run_worker": False})
    initialize_logging(settings)
    runtime = ManagementRuntime.build(settings)
    try:
        await runtime.start()
        assert runtime.driver is not None
        task = asyncio.create_task(runtime.driver.serve())
        loop = asyncio.get_running_loop()
        for signum in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(signum, task.cancel)
        try:
            await task
        except asyncio.CancelledError:
            pass
    finally:
        await runtime.close()


if __name__ == "__main__":
    asyncio.run(main())
