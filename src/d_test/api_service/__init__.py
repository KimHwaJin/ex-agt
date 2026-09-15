"""Public installation entrypoints, stable across internal refactors."""

from .bootstrap.application import (
    create_app as create_app,
)
from .bootstrap.application import (
    install_management_api as install_management_api,
)
from .bootstrap.server import run_server as run_server
from .integrations.gaia import (
    install_template_runtime as install_template_runtime,
)
from .routers import get_management_routers as get_management_routers
