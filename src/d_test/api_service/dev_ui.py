"""Same-origin development console; no frontend server or template engine."""

from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, RedirectResponse

from d_test.agent_service.settings import Settings

ASSETS = Path(__file__).parent / "static" / "dev"
HEADERS = {
    "Cache-Control": "no-store",
    "X-Content-Type-Options": "nosniff",
    "X-Frame-Options": "DENY",
    "Referrer-Policy": "no-referrer",
    "Content-Security-Policy": (
        "default-src 'none'; script-src 'self'; style-src 'self'; "
        "connect-src 'self'; img-src 'self'; base-uri 'none'; "
        "frame-ancestors 'none'; form-action 'none'"
    ),
}


def install_dev_ui(app: FastAPI, settings: Settings) -> None:
    if settings.environment != "development":
        return

    @app.get("/dev/runtime", include_in_schema=False)
    async def runtime() -> dict:
        return {"agent_backend": settings.agent_backend}

    @app.get("/dev", name="management_dev_redirect", include_in_schema=False)
    async def redirect(request: Request) -> RedirectResponse:
        root = request.scope.get("root_path", "").rstrip("/")
        return RedirectResponse(f"{root}/dev/", headers=HEADERS)

    @app.get("/dev/", name="management_dev_page", include_in_schema=False)
    async def page() -> FileResponse:
        return FileResponse(
            ASSETS / "index.html", media_type="text/html", headers=HEADERS
        )

    @app.get(
        "/dev/assets/app.js",
        name="management_dev_script",
        include_in_schema=False,
    )
    async def script() -> FileResponse:
        return FileResponse(
            ASSETS / "app.js", media_type="text/javascript", headers=HEADERS
        )

    @app.get(
        "/dev/assets/chat.js",
        name="management_dev_chat",
        include_in_schema=False,
    )
    async def chat() -> FileResponse:
        return FileResponse(
            ASSETS / "chat.js", media_type="text/javascript", headers=HEADERS
        )

    @app.get(
        "/dev/assets/styles.css",
        name="management_dev_styles",
        include_in_schema=False,
    )
    async def styles() -> FileResponse:
        return FileResponse(
            ASSETS / "styles.css", media_type="text/css", headers=HEADERS
        )
