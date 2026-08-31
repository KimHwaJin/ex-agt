"""Worker process entrypoint: python main.py (or python -m main).

Edit agent_app.py to connect the recipient's Agent. This file owns startup,
resource lifetime and shutdown, not business event processing.
"""

from __future__ import annotations

import asyncio
import logging
import signal

from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver

import agent_app
from executor_worker import EventContext, ExecutorWorker, Settings
from executor_worker.langgraph_adapter import SessionGraphAdapter


async def main() -> None:
    settings = Settings()
    adapter: SessionGraphAdapter | None = None

    async def resume_graph(context: EventContext) -> None:
        # Dispatcher already holds SessionGuard. Do not acquire it again.
        if adapter is None:
            raise RuntimeError("Agent graph has not been initialized")
        await adapter(context)

    handlers = agent_app.build_handlers(resume_graph)
    if not handlers or any(
        not isinstance(kind, str) or not kind.strip() or not callable(handler)
        for kind, handler in handlers.items()
    ):
        raise ValueError("agent_app.build_handlers() must register handlers")

    async with ExecutorWorker(settings, handlers) as worker:
        # Both stores stay open until worker.run() has drained its handlers.
        # Schema setup belongs in a deployment Job, never Worker startup.
        async with AsyncPostgresSaver.from_conn_string(
            settings.database_url,
        ) as saver:
            graph = await agent_app.create_graph(saver, worker.bindings)
            if not all(
                callable(getattr(graph, method, None))
                for method in ("aget_state", "ainvoke")
            ) or not getattr(graph, "checkpointer", None):
                raise ValueError(
                    "agent_app.create_graph() must return a compiled graph "
                    "with a checkpointer; see docs/agent-integration.md"
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
