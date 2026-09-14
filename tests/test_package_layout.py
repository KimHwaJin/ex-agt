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
        "d_test.api_service.factory",
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
    from d_test.api_service import factory

    assert factory.logger.name == "api_service"
    assert graph_driver.logger.name == "agent_service.graph"
    assert demo_driver.logger.name == "agent_service.demo"
