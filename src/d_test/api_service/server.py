"""Run the API with an explicit Psycopg-compatible Windows event loop."""

import sys
from typing import Any

import uvicorn
from fastapi import FastAPI

from d_test.agent_service.bootstrap.event_loop import run_async


def run_server(app: FastAPI, **options: Any) -> None:
    """Single-process entrypoint; deployment process management is external."""
    if sys.platform == "win32":
        if options.get("reload") or options.get("workers", 1) not in (None, 1):
            raise ValueError(
                "Windows run_server supports one worker without reload"
            )
        server = uvicorn.Server(uvicorn.Config(app, **options))
        run_async(server.serve())
    else:
        uvicorn.run(app, **options)
