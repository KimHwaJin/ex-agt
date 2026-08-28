import asyncio
import logging

from ex_agent.config import get_settings
from ex_agent.worker import WorkflowWorker


def run_worker() -> None:
    settings = get_settings()
    logging.basicConfig(level=settings.log_level)
    asyncio.run(WorkflowWorker(settings).run())
