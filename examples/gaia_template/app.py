"""Internal-template entrypoint example: keep gaia/core.py unchanged."""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent / "src"))


def main():
    # These modules belong to the internal template, not this repository.
    from common.config import config
    from gaia.core import GaiaService
    from prometheus_fastapi_instrumentator import Instrumentator
    from template_bindings import resolve_request
    from workflows.analysis import manager

    from d_test.agent_service.bootstrap.template import settings_from_template
    from d_test.api_service.server import run_server
    from d_test.api_service.template import install_template_runtime

    # Constructor initializes the host logger and existing routes/middleware.
    service = GaiaService()
    settings = settings_from_template(config.AGENT_SERVICE)
    app = service.app
    app.openapi = service.custom_openapi
    # These two lines reproduce the provided main() instrumentation.
    # Verify the public member name against the real template on migration.
    instrument = Instrumentator().instrument(app)
    instrument.expose(app)
    install_template_runtime(
        app, settings, manager=manager, resolve=resolve_request
    )
    # Do not call service.main(): it overwrites our composed lifespan.
    run_server(
        app,
        host="0.0.0.0",
        port=int(config.PORT),
        log_config=None,
        proxy_headers=False,
    )


if __name__ == "__main__":
    main()
