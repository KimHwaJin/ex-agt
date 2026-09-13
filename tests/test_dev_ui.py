"""Development assets are same-origin and never exposed in production."""

import httpx
import pytest
from fastapi import FastAPI

from agent_service.settings import Settings
from api_service.factory import create_app


async def test_dev_console_assets_and_redirect(settings):
    app = create_app(settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        redirect = await client.get("/dev")
        assert redirect.status_code == 307
        assert redirect.headers["location"] == "/dev/"
        page = await client.get("/dev/")
        assert page.status_code == 200
        assert 'lang="ko"' in page.text
        assert 'src="assets/app.js"' in page.text
        assert 'href="assets/styles.css"' in page.text
        for path, media_type in [
            ("/dev/", "text/html"),
            ("/dev/assets/app.js", "text/javascript"),
            ("/dev/assets/styles.css", "text/css"),
        ]:
            response = await client.get(path)
            assert response.status_code == 200
            assert media_type in response.headers["content-type"]
            assert response.headers["cache-control"] == "no-store"
            assert response.headers["x-content-type-options"] == "nosniff"
            policy = response.headers["content-security-policy"]
            assert "script-src 'self'" in policy
            assert "frame-ancestors 'none'" in policy
            assert "unsafe-inline" not in policy
        # Development UI does not expand the public management API contract.
        paths = app.openapi()["paths"]
        assert not any(path.startswith("/dev") for path in paths)
        assert sum(len(methods) for methods in paths.values()) == 12


@pytest.mark.parametrize(
    "path",
    [
        "/dev",
        "/dev/",
        "/dev/assets/app.js",
        "/dev/assets/styles.css",
    ],
)
async def test_production_has_no_dev_routes(settings, path):
    production = Settings.model_validate(
        {
            **settings.model_dump(),
            "environment": "production",
            "auth_mode": "trusted_header",
            "trusted_proxy_cidrs": ["127.0.0.1/32"],
            "logging_mode": "host",
            "logging_initializer": "platform_logging:initialize",
            "logging_yaml": "platform_logging.yaml",
        }
    )
    app = create_app(production)
    # No lifespan: production resources are not required to test route absence.
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert (await client.get(path)).status_code == 404


async def test_dev_assets_cannot_read_other_files(settings):
    app = create_app(settings)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        for path in [
            "/dev/assets/config.yaml",
            "/dev/assets/%2e%2e/config.yaml",
            "/dev/assets/index.html",
        ]:
            assert (await client.get(path)).status_code == 404


async def test_console_supports_template_mount_prefix(settings):
    host = FastAPI()
    host.mount("/template", create_app(settings))
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=host), base_url="http://test"
    ) as client:
        redirect = await client.get("/template/dev")
        assert redirect.headers["location"] == "/template/dev/"
        assert (await client.get("/template/dev/")).status_code == 200
        script = await client.get("/template/dev/assets/app.js")
        assert script.status_code == 200
        assert 'new URL("../api/v1/", window.location.href)' in script.text
