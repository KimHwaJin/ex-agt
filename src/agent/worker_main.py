"""Worker process entrypoint: python -m agent.worker_main.

Owns startup, shared Agent runtime lifetime and graceful shutdown.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from agent.graph import checkpoint_serializer
from agent.integrations import worker_hooks
from agent.runtime import build_worker_settings, open_agent_runtime
from ex_agent.config import Settings as AgentSettings
from worker import EventContext, ExecutorWorker, Settings


async def main() -> None:
    agent_settings = AgentSettings()
    settings: Settings = build_worker_settings(agent_settings)
    event_handler = None

    async def resume_graph(context: EventContext) -> None:
        # Dispatcher already holds SessionGuard. Do not acquire it again.
        if event_handler is None:
            raise RuntimeError("Agent runtime has not been initialized")
        await event_handler(context)

    handlers = worker_hooks.build_handlers(resume_graph)
    if not handlers or any(
        not isinstance(kind, str) or not kind.strip() or not callable(handler)
        for kind, handler in handlers.items()
    ):
        raise ValueError(
            "worker_hooks.build_handlers() must register handlers"
        )

    async with ExecutorWorker(settings, handlers) as worker:
        # Both stores stay open until worker.run() has drained its handlers.
        # Schema setup belongs in a deployment Job, never Worker startup.
        async with AsyncPostgresSaver.from_conn_string(
            agent_settings.agent_checkpoint_database_url,
            serde=checkpoint_serializer(),
        ) as saver:
            async with open_agent_runtime(
                agent_settings,
                worker,
                saver,
            ) as runtime:
                graph = runtime.graph
                if not all(
                    callable(getattr(graph, method, None))
                    for method in ("aget_state", "ainvoke")
                ) or not getattr(graph, "checkpointer", None):
                    raise ValueError(
                        "Agent runtime must return a compiled graph with "
                        "a checkpointer"
                    )
                event_handler = runtime.event_handler
                worker.add_readiness_check(
                    "agent-runtime", runtime.lifecycle.ready
                )
                loop = asyncio.get_running_loop()
                installed = []
                try:
                    for signum in (signal.SIGTERM, signal.SIGINT):
                        loop.add_signal_handler(
                            signum, runtime.lifecycle.request_stop
                        )
                        installed.append(signum)
                    # No consumer groups, routing or ACKs before here.
                    await runtime.lifecycle.run_worker(worker)
                finally:
                    for signum in installed:
                        loop.remove_signal_handler(signum)


def run_worker() -> None:
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())


if __name__ == "__main__":
    run_worker()
