"""Private template behavior is simulated, not claimed as wire integration."""

import importlib.util
import logging
import sys
import traceback
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import pytest
import yaml
from fastapi import FastAPI
from pydantic import ValidationError

from d_test.agent_service.bootstrap.logging import initialize_logging
from d_test.agent_service.bootstrap.template import settings_from_template
from d_test.agent_service.integrations.template_protocol import decode_v1
from d_test.agent_service.integrations.template_workflow import WorkflowManager
from d_test.api_service import (
    get_management_routers,
    install_management_api,
    install_template_runtime,
)

ROOT = Path(__file__).resolve().parents[1]


def test_profiles_and_preconfigured_logging(settings, monkeypatch):
    values = settings.model_dump(exclude={"logging_mode"})
    for profile in ("local", "dev"):
        configured = settings_from_template({**values, "environment": profile})
        assert configured.environment == profile
        assert configured.logging_mode == "preconfigured"

    def forbidden_logging(**kwargs):
        raise AssertionError("logging reset")

    monkeypatch.setattr(logging, "basicConfig", forbidden_logging)
    initialize_logging(settings_from_template(values))
    for profile in ("stg", "prd"):
        with pytest.raises(RuntimeError, match="Invalid AGENT_SERVICE"):
            settings_from_template({**values, "environment": profile})
        production = settings_from_template(
            {
                **values,
                "environment": profile,
                "auth_mode": "trusted_header",
                "trusted_proxy_cidrs": ["10.0.0.0/24"],
            }
        )
        assert production.environment == profile
    with pytest.raises(RuntimeError, match="Invalid AGENT_SERVICE"):
        settings_from_template({**values, "environment": "unknown"})
    with pytest.raises(RuntimeError, match="must be a mapping"):
        settings_from_template(None)  # ty: ignore
    with pytest.raises(RuntimeError, match="ENVIRONMENT is required"):
        settings_from_template(
            {
                key: value
                for key, value in values.items()
                if key != "environment"
            }
        )
    with pytest.raises(RuntimeError) as caught:
        settings_from_template({**values, "unexpected": "secret-password"})
    assert "secret-password" not in str(caught.value)


def test_complete_template_yaml_loads_uppercase_settings():
    with (ROOT / "examples/gaia_template/config.dev.yml").open() as source:
        template = yaml.safe_load(source)
    values = template["AGENT_SERVICE"]
    assert all(key.isupper() for key in values)
    configured = settings_from_template(values)
    assert configured.environment == "dev"
    assert configured.database_bootstrap == "initialize_if_empty"
    assert configured.database_url.get_secret_value().endswith("/chatapp")
    assert configured.model_name == "qwen38-27b-nvfp4"
    assert configured.logging_mode == "preconfigured"
    assert configured.model_extra_body == {
        "chat_template_kwargs": {"enable_thinking": False}
    }
    assert template["PORT"] == 5000
    assert template["SERVICE_ID"] == "S030042"
    assert template["GAIA_API_SESSION_NAME"] == ""
    assert template["S3_FULE_URL_ENABLED"] is True


@pytest.mark.parametrize("profile", ["stg", "prd"])
def test_uppercase_settings_preserve_deployment_checks(settings, profile):
    values = {
        key.upper(): value for key, value in settings.model_dump().items()
    }
    values.update(ENVIRONMENT=profile, LOGGING_MODE="preconfigured")
    with pytest.raises(RuntimeError, match="Invalid AGENT_SERVICE"):
        settings_from_template(values)
    values.update(
        AUTH_MODE="trusted_header", TRUSTED_PROXY_CIDRS=["10.0.0.0/24"]
    )
    assert settings_from_template(values).environment == profile


def test_duplicate_setting_casing_is_not_silently_overwritten(settings):
    with pytest.raises(RuntimeError, match="duplicate AGENT_SERVICE"):
        settings_from_template({**settings.model_dump(), "ENVIRONMENT": "prd"})


@pytest.mark.parametrize(
    ("overrides", "expected"),
    [
        ({"environment": "secret-invalid-value"}, "ENVIRONMENT: 허용값"),
        ({"pool_timeout": "secret-invalid-value"}, "POOL_TIMEOUT:"),
        ({"database_url": "secret-invalid-value"}, "DATABASE_URL:"),
        ({"cursor_secret": "secret-invalid-value"}, "CURSOR_SECRET:"),
        ({"auth_mode": "external"}, "IDENTITY_PROVIDER_FACTORY:"),
        ({"environment": "stg"}, "AUTH_MODE:"),
        (
            {
                "auth_mode": "trusted_header",
                "trusted_proxy_cidrs": ["secret-invalid-value"],
            },
            "TRUSTED_PROXY_CIDRS:",
        ),
        ({"secret-unknown-key": "secret-invalid-value"}, "<UNKNOWN_FIELD>"),
    ],
)
def test_template_diagnostics_name_issue_without_exposing_input(
    settings, overrides, expected
):
    values = settings.model_dump(exclude={"logging_mode"})
    values.update(overrides, model_api_key="secret-model-api-key")
    with pytest.raises(RuntimeError) as caught:
        settings_from_template(values)
    output = "".join(traceback.format_exception(caught.value))
    assert expected in str(caught.value)
    assert "secret-invalid-value" not in output
    assert "secret-model-api-key" not in output
    assert "secret-unknown-key" not in output
    assert "input_value" not in output


def test_template_diagnostics_report_multiple_field_errors(settings):
    values = settings.model_dump(exclude={"logging_mode", "database_url"})
    values.update(environment="bad-profile", pool_timeout="bad-timeout")
    with pytest.raises(RuntimeError) as caught:
        settings_from_template(values)
    message = str(caught.value)
    assert "ENVIRONMENT: 허용값: local, dev, stg, prd" in message
    assert "DATABASE_URL: 필수 항목이 없습니다" in message
    assert "POOL_TIMEOUT:" in message


def test_router_collision_and_missing_registration(settings):
    app = FastAPI()
    with pytest.raises(RuntimeError, match="Register all"):
        install_management_api(app, settings, include_routers=False)
    for router in get_management_routers():
        app.include_router(router)
    with pytest.raises(RuntimeError, match="collision"):
        install_management_api(app, settings)


@pytest.mark.parametrize("fail_start", [False, True])
async def test_template_lifespan_composes_and_releases(
    settings, monkeypatch, fail_start
):
    order = []

    @asynccontextmanager
    async def original(app):
        order.append("host-start")
        try:
            yield {"host": True}
        finally:
            order.append("host-stop")

    async def start():
        order.append("agent-start")
        if fail_start:
            raise RuntimeError("startup failed")

    async def close():
        order.append("agent-stop")

    async def resolve(payload, config):
        raise AssertionError("Not invoked")

    resources = SimpleNamespace(start=start, close=close, runs=object())
    monkeypatch.setattr(
        "d_test.api_service.bootstrap.application.ManagementRuntime.build",
        lambda _: resources,
    )
    app = FastAPI(lifespan=original)
    for router in get_management_routers():
        app.include_router(router)
    manager = WorkflowManager()
    install_template_runtime(
        app,
        settings.model_copy(update={"logging_mode": "preconfigured"}),
        manager=manager,
        resolve=resolve,
    )
    assert not any(
        getattr(r, "path", "").startswith("/dev") for r in app.routes
    )

    async def run():
        async with app.router.lifespan_context(app) as state:
            assert state == {"host": True}
            assert manager.require_binding().runs is resources.runs
            with pytest.raises(RuntimeError, match="already bound"):
                async with manager.bind(resources.runs, resolve):
                    pass

    if fail_start:
        with pytest.raises(RuntimeError, match="startup failed"):
            await run()
    else:
        await run()
    assert order == ["host-start", "agent-start", "agent-stop", "host-stop"]
    assert not hasattr(app.state, "management_runtime")
    with pytest.raises(RuntimeError, match="outside app lifespan"):
        manager.require_binding()


async def test_discovery_no_database_or_implicit_resume():
    path = ROOT / "examples/gaia_template/src/workflows/analysis.py"
    spec = importlib.util.spec_from_file_location("workflow_example", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    manager = module.manager
    assert callable(manager.ainvoke) and callable(manager.astream)
    with pytest.raises(RuntimeError, match="outside app lifespan"):
        await manager.ainvoke({})
    with pytest.raises(ValueError, match="stream mode"):
        async for _ in manager.astream({}, stream_mode=["messages"]):
            pass
    # A2A text-only resumption is NOT guessed from task/context IDs.
    with pytest.raises(ValidationError):
        decode_v1(
            {"message": {"taskId": "x", "parts": [{"text": "승인"}]}},
            owner=uuid4(),
        )


def test_provisional_envelope_validates_response_and_owner_separately():
    user, owner, session = uuid4(), uuid4(), uuid4()
    data = {
        "agent_request": {
            "protocol_version": 1,
            "request_id": "stable-retry-key",
            "request": {
                "user_id": str(user),
                "session_id": str(session),
                "main_model_name": "selected",
                "input": {
                    "type": "message",
                    "content": [{"type": "text", "text": "안녕"}],
                },
            },
        },
        "trace_id": "trace-is-not-idempotency",
    }
    request = decode_v1(data, owner=owner)
    assert request.owner == owner
    assert request.request.user_id == user  # RunService verifies equality.
    assert request.request.main_model_name == "selected"
    data["agent_request"]["protocol_version"] = 2
    with pytest.raises(ValidationError):
        decode_v1(data, owner=owner)


def test_example_entrypoint_uses_host_app_not_main(settings, monkeypatch):
    calls = []
    app = FastAPI()
    for router in get_management_routers():
        app.include_router(router)

    class GaiaService:
        def __init__(self):
            self.app = app
            calls.append("host-init-and-logging")

        def custom_openapi(self):
            return {"host": "openapi"}

        def main(self):
            raise AssertionError("Host main overwrites lifespan")

    class Instrumentator:
        def instrument(self, target):
            assert target is app
            calls.append("instrument")
            return self

        def expose(self, target):
            assert target is app
            calls.append("expose")

    async def resolver(payload, config):
        raise AssertionError("No requests in entrypoint test")

    modules = {
        "common.config": SimpleNamespace(
            config=SimpleNamespace(
                PORT=8020,
                AGENT_SERVICE={
                    key.upper(): value
                    for key, value in settings.model_dump(
                        exclude={"logging_mode"}
                    ).items()
                },
            )
        ),
        "gaia.core": SimpleNamespace(GaiaService=GaiaService),
        "prometheus_fastapi_instrumentator": SimpleNamespace(
            Instrumentator=Instrumentator
        ),
        "template_bindings": SimpleNamespace(resolve_request=resolver),
        "workflows.analysis": SimpleNamespace(manager=WorkflowManager()),
    }
    for name, module in modules.items():
        monkeypatch.setitem(sys.modules, name, module)
    monkeypatch.setattr(sys, "path", list(sys.path))

    def run(target, **kwargs):
        assert target is app
        assert kwargs["log_config"] is None
        assert kwargs["proxy_headers"] is False
        calls.append("uvicorn")

    monkeypatch.setattr("d_test.api_service.run_server", run)
    spec = importlib.util.spec_from_file_location(
        "template_entrypoint", ROOT / "examples/gaia_template/app.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    module.main()
    assert calls == [
        "host-init-and-logging",
        "instrument",
        "expose",
        "uvicorn",
    ]
    assert app.openapi() == {"host": "openapi"}
