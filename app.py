"""Template-compatible root entrypoint for the new management API.

An existing template can instead call install_management_api().
"""

from pathlib import Path

from d_test.agent_service.bootstrap.configuration import load_settings
from d_test.api_service import create_app, run_server

settings = load_settings(Path(__file__).resolve().parent)
app = create_app(settings)

if __name__ == "__main__":
    run_server(
        app,
        host=settings.bind_host,
        port=settings.bind_port,
        proxy_headers=False,
    )
