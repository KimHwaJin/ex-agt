"""Standalone worker: python -m d_test.agent_service.worker_main."""

import asyncio
import signal
import sys
from pathlib import Path

from d_test.agent_service.bootstrap.configuration import load_settings
from d_test.agent_service.bootstrap.event_loop import run_async
from d_test.agent_service.bootstrap.logging import initialize_logging
from d_test.agent_service.runtime.management import ManagementRuntime
from d_test.agent_service.settings import Settings


async def main(settings: Settings | None = None) -> None:
    settings = settings or load_settings(Path.cwd())
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
        # Windows has no add_signal_handler; Runner handles console Ctrl+C.
        if sys.platform != "win32":
            for signum in (signal.SIGTERM, signal.SIGINT):
                loop.add_signal_handler(signum, task.cancel)
        try:
            await task
        except asyncio.CancelledError:
            pass
    finally:
        await runtime.close()


if __name__ == "__main__":
    run_async(main())
