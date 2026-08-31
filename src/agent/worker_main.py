"""Worker process entrypoint: python -m agent.worker_main.

Edit agent.integrations.worker_hooks to connect the Agent. Owns startup,
resource lifetime and shutdown, not business event processing.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

from agent.integrations import worker_hooks
from agent.integrations.langgraph_adapter import SessionGraphAdapter
from worker import EventContext, ExecutorWorker, Settings


async def main() -> None:
    settings = Settings()
    adapter: SessionGraphAdapter | None = None

    async def resume_graph(context: EventContext) -> None:
        # Dispatcher already holds SessionGuard. Do not acquire it again.
        if adapter is None:
            raise RuntimeError("Agent graph has not been initialized")
        await adapter(context)

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
            settings.database_url,
        ) as saver:
            graph = await worker_hooks.create_graph(saver, worker.bindings)
            if not all(
                callable(getattr(graph, method, None))
                for method in ("aget_state", "ainvoke")
            ) or not getattr(graph, "checkpointer", None):
                raise ValueError(
                    "worker_hooks.create_graph() must return a compiled "
                    "graph with a checkpointer; "
                    "see src/worker/docs/agent-integration.md"
                )
            adapter = SessionGraphAdapter(graph)
            loop = asyncio.get_running_loop()
            installed = []
            try:
                for signum in (signal.SIGTERM, signal.SIGINT):
                    loop.add_signal_handler(signum, worker.request_stop)
                    installed.append(signum)
                # No consumer groups, reads, routing or ACKs before here.
                await worker.run()
            finally:
                for signum in installed:
                    loop.remove_signal_handler(signum)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())
