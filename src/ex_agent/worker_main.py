import asyncio
import logging
import signal

from ex_agent.config import Settings, get_settings
from ex_agent.worker import WorkflowWorker

logger = logging.getLogger(__name__)


async def _run_until_stopped(
    worker: WorkflowWorker,
    stop_requested: asyncio.Event,
) -> None:
    run_task = asyncio.create_task(worker.run())
    stop_task = asyncio.create_task(stop_requested.wait())
    try:
        done, _ = await asyncio.wait(
            {run_task, stop_task},
            return_when=asyncio.FIRST_COMPLETED,
        )
        if run_task in done:
            await run_task
            return
        await worker.shutdown()
        if not run_task.cancelled():
            await run_task
    finally:
        if not stop_task.done():
            stop_task.cancel()
        await asyncio.gather(stop_task, return_exceptions=True)


async def _run_worker(settings: Settings) -> None:
    worker = WorkflowWorker(settings)
    stop_requested = asyncio.Event()
    loop = asyncio.get_running_loop()
    registered_signals: list[signal.Signals] = []

    def request_stop() -> None:
        if not stop_requested.is_set():
            logger.info("Worker shutdown signal received")
            stop_requested.set()

    for selected_signal in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(selected_signal, request_stop)
        except NotImplementedError:
            logger.warning(
                "Async signal handlers are unavailable",
                extra={"signal": selected_signal.name},
            )
            break
        registered_signals.append(selected_signal)
    try:
        await _run_until_stopped(worker, stop_requested)
    finally:
        for selected_signal in registered_signals:
            loop.remove_signal_handler(selected_signal)


def run_worker() -> None:
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)
    asyncio.run(_run_worker(settings))
