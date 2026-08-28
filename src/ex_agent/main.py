import uvicorn

from ex_agent.api.app import create_app
from ex_agent.config import get_settings

app = create_app()


def run_api() -> None:
    settings = get_settings()
    uvicorn.run(
        "ex_agent.main:app",
        host=settings.host,
        port=settings.port,
        log_level=settings.log_level.lower(),
    )
