"""The distribution exposes one template-safe application namespace."""

import importlib
import importlib.util
import pkgutil
from importlib.resources import files

import d_test


def test_only_d_test_namespace_is_exposed():
    assert importlib.util.find_spec("api_service") is None
    assert importlib.util.find_spec("agent_service") is None
    modules = {
        item.name
        for item in pkgutil.walk_packages(d_test.__path__, prefix="d_test.")
    }
    assert {
        "d_test.api_service.bootstrap.application",
        "d_test.api_service.routers.projects",
        "d_test.api_service.schemas.projects",
        "d_test.api_service.dependencies.services",
        "d_test.api_service.auth.providers",
        "d_test.api_service.middleware.request_context",
        "d_test.api_service.handlers.exceptions",
        "d_test.api_service.streaming.runs",
        "d_test.api_service.integrations.gaia",
        "d_test.agent_service.worker_main",
        "d_test.agent_service.migrate",
        "d_test.agent_service.checkpoint_main",
    } <= modules
    for name in sorted(modules):
        importlib.import_module(name)


def test_assets_ship_under_new_package():
    catalog = files("d_test.agent_service.catalog").joinpath("assets")
    assert catalog.joinpath("sample-data.md").is_file()
    assert catalog.joinpath("fetch_sample_data.py").is_file()
    ui = files("d_test.api_service").joinpath("static", "dev")
    for name in ("index.html", "app.js", "chat.js", "styles.css"):
        assert ui.joinpath(name).is_file()


def test_explicit_logger_names_remain_compatible():
    from d_test.agent_service.runtime import demo_driver, graph_driver
    from d_test.api_service.bootstrap import application

    assert application.logger.name == "api_service"
    assert graph_driver.logger.name == "agent_service.graph"
    assert demo_driver.logger.name == "agent_service.demo"


def test_public_installation_entrypoints():
    from d_test import api_service
    from d_test.api_service.bootstrap import application, server
    from d_test.api_service.integrations import gaia
    from d_test.api_service.routers import get_management_routers

    assert api_service.create_app is application.create_app
    assert (
        api_service.install_management_api
        is application.install_management_api
    )
    assert api_service.run_server is server.run_server
    assert (
        api_service.install_template_runtime is gaia.install_template_runtime
    )
    assert api_service.get_management_routers is get_management_routers


def test_schema_facades_reuse_domain_contracts():
    from d_test.agent_service.domain.management import Project
    from d_test.agent_service.domain.runs import RunRequest
    from d_test.api_service.schemas.projects import Project as ProjectResponse
    from d_test.api_service.schemas.runs import RunRequest as ApiRunRequest

    assert ProjectResponse is Project
    assert ApiRunRequest is RunRequest


def test_router_registry_has_no_duplicate_endpoints():
    from fastapi.routing import APIRoute

    from d_test.api_service import get_management_routers

    endpoints = [
        (route.path, method)
        for router in get_management_routers()
        for route in router.routes
        if isinstance(route, APIRoute)
        for method in (route.methods or ())
    ]
    assert len(endpoints) == len(set(endpoints))
    assert ("/api/v1/me", "POST") in endpoints
    assert ("/api/v1/agent/runs", "POST") in endpoints
    assert not any(path.startswith("/health/") for path, _ in endpoints)
