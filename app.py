"""Template-compatible root entrypoint for the new management API.

An existing template can instead call install_management_api().
"""

from pathlib import Path

import uvicorn

from agent_service.bootstrap.configuration import load_settings
from api_service.factory import create_app

settings = load_settings(Path(__file__).resolve().parent)
app = create_app(settings)

if __name__ == "__main__":
    uvicorn.run(
        app,
        host=settings.bind_host,
        port=settings.bind_port,
        proxy_headers=False,
    )
